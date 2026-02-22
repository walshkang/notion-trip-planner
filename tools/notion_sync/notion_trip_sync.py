#!/usr/bin/env python3
"""
notion_trip_sync.py

Sync TripPlan Compiler canonical JSON into a single Notion database (data source) via upsert.

Key behaviors:
- Sync modes:
  - patch (default): missing keys => do not touch those Notion properties
  - canonical: missing keys may clear properties (authoritative payload)
- Explicit relation clearing rules (no accidental wipes)
- Checkbox handling:
  - patch: missing key => no touch; null => warn+skip (or fail in strict)
  - canonical: missing/null => set false
- Duplicate Row IDs: warn, or fail with --strict
- Filter fallback: try property ID, then property name if Notion rejects ID
- Retries now include network exceptions + jitter
- Schema gate:
  - hard-required: title/type/row_id
  - relation-required (trip/place): strict|canonical fail, patch non-strict warn+skip
  - Category select: strict|canonical fail, patch non-strict warn+skip when Category is touched

Upsert key: Row ID (rich text)

Usage:
  export NOTION_TOKEN="secret_..."
  python notion_trip_sync.py init --database-id <DATABASE_ID> --out config.json
  python notion_trip_sync.py apply --config config.json --payload payload.json
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'requests'. Install it with:\n"
        "  pip install -r tools/notion_sync/requirements.txt"
    ) from exc

BASE_URL = "https://api.notion.com/v1"
DEFAULT_NOTION_VERSION = "2025-09-03"
USER_AGENT = "tripplan-compiler-sync/0.2"


class NotionAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: Optional[int] = None, error_json: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.error_json = error_json or {}


@dataclass
class DataSourceSchema:
    data_source_id: str
    properties_by_name: Dict[str, Dict[str, Any]]
    properties_by_id: Dict[str, Dict[str, Any]]
    id_by_name: Dict[str, str]
    name_by_id: Dict[str, str]
    type_by_id: Dict[str, str]


class NotionClient:
    def __init__(self, token: str, notion_version: str = DEFAULT_NOTION_VERSION, timeout_s: int = 30):
        self.token = token
        self.notion_version = notion_version
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Notion-Version": self.notion_version,
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            }
        )

    def request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, body: Any = None) -> Any:
        url = f"{BASE_URL}{path}"
        last_exc: Optional[Exception] = None

        for attempt in range(1, 8):
            try:
                resp = self.session.request(method, url, params=params, json=body, timeout=self.timeout_s)
            except requests.exceptions.RequestException as e:
                last_exc = e
                time.sleep(min(2 ** attempt, 30) + random.random() * 0.25)
                continue

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                base_sleep = float(retry_after) if retry_after else min(2 ** attempt, 30)
                time.sleep(base_sleep + random.random() * 0.25)
                continue

            if resp.status_code in (500, 502, 503, 504):
                time.sleep(min(2 ** attempt, 30) + random.random() * 0.25)
                continue

            if resp.status_code >= 400:
                try:
                    err = resp.json()
                except Exception:
                    err = {"text": resp.text}
                raise NotionAPIError(
                    f"{method} {path} failed ({resp.status_code}): {json.dumps(err)[:2000]}",
                    status_code=resp.status_code,
                    error_json=err if isinstance(err, dict) else {"text": str(err)},
                )

            if resp.content:
                return resp.json()
            return None

        if last_exc:
            raise NotionAPIError(f"{method} {path} failed after retries: {last_exc}")
        raise NotionAPIError(f"{method} {path} failed after retries")

    def query_data_source(self, data_source_id: str, filter_obj: Optional[Dict[str, Any]] = None, page_size: int = 10) -> List[Dict[str, Any]]:
        body: Dict[str, Any] = {"page_size": page_size}
        if filter_obj is not None:
            body["filter"] = filter_obj
        out: List[Dict[str, Any]] = []
        cursor = None
        while True:
            if cursor:
                body["start_cursor"] = cursor
            res = self.request("POST", f"/data_sources/{data_source_id}/query", body=body)
            out.extend(res.get("results", []))
            cursor = res.get("next_cursor")
            if not cursor:
                break
        return out

    def retrieve_data_source(self, data_source_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/data_sources/{data_source_id}")

    def create_page_in_data_source(self, data_source_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        body = {"parent": {"type": "data_source_id", "data_source_id": data_source_id}, "properties": properties}
        return self.request("POST", "/pages", body=body)

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        body = {"properties": properties}
        return self.request("PATCH", f"/pages/{page_id}", body=body)

    def retrieve_database(self, database_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/databases/{database_id}")


def _extract_json_from_text(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("{"):
        return json.loads(text)

    m = re.search(r"```json\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if m:
        return json.loads(m.group(1).strip())

    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in payload text.")
    s = text[start:]
    depth = 0
    for i, ch in enumerate(s):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(s[: i + 1])
    raise ValueError("Found '{' but could not find matching '}' for a complete JSON object.")


def load_payload(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return _extract_json_from_text(f.read())


def count_sync_targets(payload: Dict[str, Any]) -> int:
    total = 0
    trip = payload.get("trip") or {}
    if isinstance(trip, dict) and trip.get("row_id"):
        total += 1

    for key in ("places", "items"):
        for obj in payload.get(key) or []:
            if isinstance(obj, dict) and obj.get("row_id"):
                total += 1
    return total


class ProgressRenderer:
    FRAMES = ("|", "/", "-", "\\")

    def __init__(self, total: int, *, enabled: bool = True, stream: Any = None):
        self.total = total
        self.enabled = enabled
        self.stream = stream if stream is not None else sys.stderr
        self._is_tty = bool(enabled and hasattr(self.stream, "isatty") and self.stream.isatty())
        self._count = 0
        self._start_ts = time.time()

    def begin(self, *, mode: str, dry_run: bool) -> None:
        if not self.enabled:
            return
        run_kind = "DRY RUN" if dry_run else "APPLY"
        print(f"[sync] {run_kind} mode={mode} targets={self.total}", file=self.stream)

    def tick(self, *, stage: str, row_id: str, action: str) -> None:
        if not self.enabled:
            return
        self._count += 1
        frame = self.FRAMES[(self._count - 1) % len(self.FRAMES)]
        total = self.total if self.total > 0 else 1
        pct = int((self._count / total) * 100)
        bar_width = 24
        fill = int((self._count / total) * bar_width)
        bar = ("#" * fill) + ("." * (bar_width - fill))
        short_row_id = row_id if len(row_id) <= 40 else (row_id[:37] + "...")
        line = f"{frame} [{bar}] {self._count}/{total} {pct:3d}% {action:<6} {stage:<8} {short_row_id}"
        if self._is_tty:
            self.stream.write("\r" + line)
            self.stream.flush()
        else:
            print(f"[sync] {line}", file=self.stream)

    def finish(self, *, success: bool) -> None:
        if not self.enabled:
            return
        if self._is_tty:
            self.stream.write("\n")
            self.stream.flush()
        elapsed = time.time() - self._start_ts
        status = "done" if success else "aborted"
        print(f"[sync] {status} in {elapsed:.1f}s", file=self.stream)


EXPECTED_PROPERTY_NAMES = {
    "Name": "title",
    "Type": "type",
    "Start": "start",
    "End": "end",
    "Location": "location",
    "Area": "area",
    "Map": "map",
    "Status": "status",
    "Priority": "priority",
    "Mode": "mode",
    "Drive hrs": "drive_hrs",
    "Book?": "book",
    "Link": "link",
    "Conf #": "conf",
    "Cost (total)": "cost_total",
    "Trip (link)": "trip_rel",
    "Trip": "trip_rel",
    "Category": "category",
    "Place (link)": "place_rel",
    "Place": "place_rel",
    "Emoji": "emoji",
    "Travelers": "travelers",
    "Default buffer mins": "default_buffer_mins",
    "Owner": "owner",
    "Notes": "notes",
    "Row ID": "row_id",
    "Import Batch": "import_batch",
    "Scaffold": "scaffold",
    "Scaffold?": "scaffold",
    "Lat": "lat",
    "Lng": "lng",
    "Google Place ID": "google_place_id",
}

HARD_REQUIRED_PROPERTY_TYPES = {
    "title": "title",
    "type": "select",
    "row_id": "rich_text",
}

RELATION_PROPERTY_TYPES = {
    "trip_rel": "relation",
    "place_rel": "relation",
}

CATEGORY_ALLOWLIST = ("Food", "Coffee", "Drinks", "Activity", "Logistics")
CATEGORY_ALLOWLIST_SET = set(CATEGORY_ALLOWLIST)
LEGACY_CATEGORY_ALIASES = {
    "meal": "Food",
    "cafe": "Coffee",
    "drinks": "Drinks",
    "sights": "Activity",
    "shopping": "Activity",
}


def normalize_category_label(raw_value: Any) -> Tuple[Optional[str], bool, Optional[str]]:
    if raw_value is None:
        return None, True, None
    if not isinstance(raw_value, str):
        return None, False, f"Category must be a string or null; got {type(raw_value).__name__}."
    value = raw_value.strip()
    if not value:
        return None, True, None
    if value in CATEGORY_ALLOWLIST_SET:
        return value, False, None
    legacy = LEGACY_CATEGORY_ALIASES.get(value.lower())
    if legacy:
        return legacy, False, None
    allowed = ", ".join(CATEGORY_ALLOWLIST)
    return None, False, f"Unknown category '{value}'. Allowed values: {allowed}."


def choose_data_source_id(notion: NotionClient, database_id: str, preferred_data_source_name: Optional[str] = None) -> Tuple[str, List[Dict[str, Any]]]:
    db = notion.retrieve_database(database_id)
    data_sources = db.get("data_sources") or []
    if not data_sources:
        raise NotionAPIError("No data_sources returned. Ensure DB is shared with the integration and Notion-Version is 2025-09-03.")
    if preferred_data_source_name:
        for ds in data_sources:
            if ds.get("name") == preferred_data_source_name:
                return ds["id"], data_sources
        raise NotionAPIError(f"Could not find data source named '{preferred_data_source_name}'. Available: {[d.get('name') for d in data_sources]}")
    return data_sources[0]["id"], data_sources


def build_schema(ds_obj: Dict[str, Any]) -> DataSourceSchema:
    ds_id = ds_obj["id"]
    props_by_name: Dict[str, Dict[str, Any]] = ds_obj.get("properties") or {}
    props_by_id: Dict[str, Dict[str, Any]] = {}
    id_by_name: Dict[str, str] = {}
    name_by_id: Dict[str, str] = {}
    type_by_id: Dict[str, str] = {}

    for name, p in props_by_name.items():
        pid = p.get("id")
        ptype = p.get("type")
        if not pid or not ptype:
            continue
        props_by_id[pid] = p
        id_by_name[name] = pid
        name_by_id[pid] = name
        type_by_id[pid] = ptype

    return DataSourceSchema(ds_id, props_by_name, props_by_id, id_by_name, name_by_id, type_by_id)


def auto_config_from_schema(schema: DataSourceSchema) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    for notion_name, key in EXPECTED_PROPERTY_NAMES.items():
        found = schema.id_by_name.get(notion_name)
        if found:
            props[key] = found
        elif key not in props:
            props[key] = None
    props["title"] = schema.id_by_name.get("Name") or "title"
    return {
        "notion_version": DEFAULT_NOTION_VERSION,
        "database_id": None,
        "data_source_id": schema.data_source_id,
        "properties": props,
    }


def rich_text_value(text: Optional[str]) -> Dict[str, Any]:
    if not text:
        return {"rich_text": []}
    return {"rich_text": [{"type": "text", "text": {"content": str(text)}}]}

def title_value(text: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": str(text)}}]}

def url_value(url: Optional[str]) -> Dict[str, Any]:
    u = (url or "").strip()
    return {"url": u or None}

def number_value(n: Any) -> Dict[str, Any]:
    return {"number": n if n is not None else None}

def checkbox_value_bool(b: bool) -> Dict[str, Any]:
    return {"checkbox": bool(b)}

def select_value(name: Optional[str]) -> Dict[str, Any]:
    return {"select": {"name": name}} if name else {"select": None}

def date_value_start(iso_start: Optional[str]) -> Dict[str, Any]:
    if not iso_start:
        return {"date": None}
    return {"date": {"start": iso_start, "end": None}}

def relation_value(page_ids: List[str]) -> Dict[str, Any]:
    return {"relation": [{"id": pid} for pid in page_ids]}

def place_value(name: Optional[str], address: Optional[str], lat: Optional[float], lon: Optional[float], google_place_id: Optional[str]) -> Dict[str, Any]:
    return {"place": {"name": name, "address": address, "lat": lat, "lon": lon, "google_place_id": google_place_id}}

def set_prop(out: Dict[str, Any], prop_id: Optional[str], prop_type: Optional[str], value_obj: Dict[str, Any]) -> None:
    if not prop_id or not prop_type:
        return
    out[prop_id] = value_obj

def resolve_prop(schema: DataSourceSchema, prop_id_or_name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not prop_id_or_name:
        return None, None
    if prop_id_or_name in schema.properties_by_id:
        return prop_id_or_name, schema.type_by_id.get(prop_id_or_name)
    if prop_id_or_name in schema.properties_by_name:
        pid = schema.id_by_name[prop_id_or_name]
        return pid, schema.type_by_id.get(pid)
    return None, None

def _schema_error_message(key: str, configured: Any, expected: str, actual: Optional[str]) -> str:
    if configured is None:
        return f"Config is missing properties.{key} (expected type '{expected}')."
    if actual is None:
        return f"Config properties.{key}='{configured}' was not found in the data source schema."
    return f"Config properties.{key} resolved to type '{actual}', expected '{expected}'."

def validate_schema_requirements(
    schema: DataSourceSchema,
    cfg_props: Dict[str, Any],
    *,
    mode: str,
    strict: bool,
    warnings: List[str],
) -> Dict[str, bool]:
    hard_errors: List[str] = []
    for key, expected_type in HARD_REQUIRED_PROPERTY_TYPES.items():
        configured = cfg_props.get(key)
        pid, actual_type = resolve_prop(schema, configured)
        if not pid or actual_type != expected_type:
            hard_errors.append(_schema_error_message(key, configured, expected_type, actual_type))
    if hard_errors:
        raise RuntimeError("Schema validation failed:\n- " + "\n- ".join(hard_errors))

    relation_enabled: Dict[str, bool] = {}
    for key, expected_type in RELATION_PROPERTY_TYPES.items():
        configured = cfg_props.get(key)
        pid, actual_type = resolve_prop(schema, configured)
        if pid and actual_type == expected_type:
            relation_enabled[key] = True
            continue

        msg = _schema_error_message(key, configured, expected_type, actual_type)
        if strict or mode == "canonical":
            raise RuntimeError(f"Schema validation failed:\n- {msg}")
        warnings.append(f"{msg} Relation writes for '{key}' will be skipped in patch/non-strict mode.")
        relation_enabled[key] = False
    return relation_enabled

def should_touch(mode: str, obj: Dict[str, Any], key: str) -> bool:
    return True if mode == "canonical" else (key in obj)

def normalize_bool(
    mode: str,
    obj: Dict[str, Any],
    key: str,
    *,
    strict: bool,
    warnings: List[str],
    row_label: str,
) -> Optional[bool]:
    if mode == "patch":
        if key not in obj:
            return None
        v = obj.get(key)
        if isinstance(v, bool):
            return v
        if v is None:
            if strict:
                raise ValueError(f"{row_label}: checkbox field '{key}' is null; supply true/false.")
            warnings.append(f"{row_label}: checkbox field '{key}' is null in patch mode; skipping checkbox update.")
            return None
        if strict:
            raise ValueError(f"{row_label}: checkbox field '{key}' must be boolean; got {type(v).__name__}.")
        coerced = bool(v)
        warnings.append(f"{row_label}: checkbox field '{key}' is non-boolean ({type(v).__name__}); coercing to {coerced}.")
        return coerced

    # canonical mode: missing/null means false (authoritative payload)
    v = obj.get(key)
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if strict:
        raise ValueError(f"{row_label}: checkbox field '{key}' must be boolean; got {type(v).__name__}.")
    coerced = bool(v)
    warnings.append(f"{row_label}: checkbox field '{key}' is non-boolean ({type(v).__name__}); coercing to {coerced}.")
    return coerced

def _query_row_id(notion: NotionClient, schema: DataSourceSchema, prop: str, row_id_value_str: str) -> List[Dict[str, Any]]:
    return notion.query_data_source(schema.data_source_id, filter_obj={"property": prop, "rich_text": {"equals": row_id_value_str}}, page_size=20)

def find_existing_by_row_id(notion: NotionClient, schema: DataSourceSchema, row_id_prop_id: str, row_id_value_str: str, *, strict: bool) -> Tuple[Optional[str], int]:
    try:
        results = _query_row_id(notion, schema, row_id_prop_id, row_id_value_str)
    except NotionAPIError:
        prop_name = schema.name_by_id.get(row_id_prop_id)
        if not prop_name:
            raise
        results = _query_row_id(notion, schema, prop_name, row_id_value_str)

    if not results:
        return None, 0
    if len(results) > 1 and strict:
        raise NotionAPIError(f"Duplicate Row ID '{row_id_value_str}' found in Notion: {[r.get('id') for r in results]}")
    results.sort(key=lambda r: r.get("last_edited_time") or "", reverse=True)
    return results[0]["id"], len(results)

def upsert_page(notion: NotionClient, schema: DataSourceSchema, row_id_prop_id: str, row_id_value_str: str, properties: Dict[str, Any], *, dry_run: bool, strict: bool, warnings: List[str]) -> Tuple[str, str]:
    existing_id, dup_count = find_existing_by_row_id(notion, schema, row_id_prop_id, row_id_value_str, strict=strict)
    if dup_count > 1:
        warnings.append(f"Duplicate Row ID '{row_id_value_str}' exists {dup_count} times; updating most recently edited.")
    if existing_id:
        if not dry_run:
            notion.update_page(existing_id, properties)
        return existing_id, "UPDATE"
    if dry_run:
        return f"DRYRUN-{row_id_value_str}", "CREATE"
    created = notion.create_page_in_data_source(schema.data_source_id, properties)
    return created["id"], "CREATE"

def build_base_properties(schema: DataSourceSchema, cfg_props: Dict[str, Any], *, name: str, type_value_str: str, row_id: str, import_batch: str, notes: str, scaffold: bool) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    pid, ptype = resolve_prop(schema, cfg_props.get("title"))
    set_prop(out, pid, ptype, title_value(name))
    pid, ptype = resolve_prop(schema, cfg_props.get("type"))
    set_prop(out, pid, ptype, select_value(type_value_str))
    pid, ptype = resolve_prop(schema, cfg_props.get("row_id"))
    set_prop(out, pid, ptype, rich_text_value(row_id))
    pid, ptype = resolve_prop(schema, cfg_props.get("import_batch"))
    set_prop(out, pid, ptype, rich_text_value(import_batch))
    pid, ptype = resolve_prop(schema, cfg_props.get("notes"))
    set_prop(out, pid, ptype, rich_text_value(notes))
    pid, ptype = resolve_prop(schema, cfg_props.get("scaffold"))
    set_prop(out, pid, ptype, checkbox_value_bool(scaffold))
    return out

def sync_payload(
    notion: NotionClient,
    schema: DataSourceSchema,
    config: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    dry_run: bool,
    enable_place: bool,
    mode: str,
    strict: bool,
    progress: Optional[ProgressRenderer] = None,
) -> Dict[str, Any]:
    cfg_props: Dict[str, Any] = config.get("properties") or {}
    warnings: List[str] = []
    relation_enabled = validate_schema_requirements(schema, cfg_props, mode=mode, strict=strict, warnings=warnings)

    place_objs = [p for p in (payload.get("places") or []) if isinstance(p, dict)]
    item_objs = [i for i in (payload.get("items") or []) if isinstance(i, dict)]
    wants_category = (
        bool(place_objs or item_objs)
        if mode == "canonical"
        else any(("category" in p or "category_row_id" in p) for p in place_objs)
        or any(("category" in i or "category_row_id" in i or "place_row_id" in i) for i in item_objs)
    )
    category_enabled = True
    category_prop_id: Optional[str] = None
    category_prop_type: Optional[str] = None
    if wants_category:
        configured = cfg_props.get("category")
        category_prop_id, category_prop_type = resolve_prop(schema, configured)
        if not category_prop_id or category_prop_type != "select":
            msg = _schema_error_message("category", configured, "select", category_prop_type)
            if strict or mode == "canonical":
                raise RuntimeError("Schema validation failed:\n- " + msg)
            warnings.append(msg + " Category writes will be skipped in patch/non-strict mode.")
            category_enabled = False

    row_id_prop_id, row_id_prop_type = resolve_prop(schema, cfg_props.get("row_id"))
    if not row_id_prop_id:
        raise RuntimeError("Config is missing properties.row_id (Row ID property id/name). Run init or fill config.json.")
    if row_id_prop_type != "rich_text":
        raise RuntimeError(f"Row ID property must be rich_text. Got type '{row_id_prop_type}'.")

    import_batch = payload.get("import_batch", "")
    categories_by_row_id: Dict[str, Dict[str, Any]] = {
        c["row_id"]: c
        for c in (payload.get("categories") or [])
        if isinstance(c, dict) and c.get("row_id")
    }
    places_by_row_id: Dict[str, Dict[str, Any]] = {p["row_id"]: p for p in (payload.get("places") or []) if isinstance(p, dict) and p.get("row_id")}

    page_ids: Dict[str, str] = {}
    actions: List[str] = []

    category_skip = object()

    def fail_or_warn_category(msg: str) -> None:
        if strict or mode == "canonical":
            raise ValueError(msg)
        warnings.append(msg + " Skipping category update in patch/non-strict mode.")

    def normalize_explicit_category(raw_value: Any, *, row_label: str) -> Any:
        normalized, explicit_clear, err = normalize_category_label(raw_value)
        if err:
            fail_or_warn_category(f"{row_label}: {err}")
            return category_skip
        if explicit_clear:
            return None
        return normalized

    def normalize_legacy_category_name(raw_value: Any, *, row_label: str, source_label: str) -> Any:
        normalized, explicit_clear, err = normalize_category_label(raw_value)
        if err:
            fail_or_warn_category(f"{row_label}: {source_label}: {err}")
            return category_skip
        if explicit_clear:
            fail_or_warn_category(f"{row_label}: {source_label} resolved to an empty category.")
            return category_skip
        return normalized

    def resolve_legacy_category_row(category_row_id: Any, *, row_label: str, source_label: str) -> Any:
        if not isinstance(category_row_id, str) or not category_row_id.strip():
            fail_or_warn_category(f"{row_label}: {source_label} must be a non-empty string.")
            return category_skip
        clean_row_id = category_row_id.strip()
        category_obj = categories_by_row_id.get(clean_row_id)
        if not category_obj:
            fail_or_warn_category(f"{row_label}: {source_label} '{clean_row_id}' was not found in payload.categories.")
            return category_skip
        return normalize_legacy_category_name(category_obj.get("name"), row_label=row_label, source_label=f"{source_label} name")

    # Trip
    trip = payload.get("trip") or {}
    trip_row_id = trip.get("row_id")
    if not trip_row_id:
        raise ValueError("payload.trip.row_id is required")

    trip_notes = (trip.get("notes") or "").strip()
    tz = trip.get("timezone")
    if tz and f"tz={tz}" not in trip_notes:
        trip_notes = (f"tz={tz}\n" + trip_notes).strip()

    trip_props = build_base_properties(
        schema, cfg_props,
        name=trip.get("name") or "Untitled Trip",
        type_value_str=trip.get("type") or "Trip",
        row_id=trip_row_id,
        import_batch=import_batch,
        notes=trip_notes,
        scaffold=bool(trip.get("scaffold", False)),
    )

    if should_touch(mode, trip, "start"):
        pid, ptype = resolve_prop(schema, cfg_props.get("start"))
        set_prop(trip_props, pid, ptype, date_value_start(trip.get("start")))
    if should_touch(mode, trip, "end"):
        pid, ptype = resolve_prop(schema, cfg_props.get("end"))
        set_prop(trip_props, pid, ptype, date_value_start(trip.get("end")))
    if should_touch(mode, trip, "status"):
        pid, ptype = resolve_prop(schema, cfg_props.get("status"))
        set_prop(trip_props, pid, ptype, select_value(trip.get("status")))
    if should_touch(mode, trip, "travelers"):
        pid, ptype = resolve_prop(schema, cfg_props.get("travelers"))
        set_prop(trip_props, pid, ptype, number_value(trip.get("travelers")))
    if should_touch(mode, trip, "default_buffer_mins"):
        pid, ptype = resolve_prop(schema, cfg_props.get("default_buffer_mins"))
        set_prop(trip_props, pid, ptype, number_value(trip.get("default_buffer_mins")))

    trip_page_id, act = upsert_page(notion, schema, row_id_prop_id, trip_row_id, trip_props, dry_run=dry_run, strict=strict, warnings=warnings)
    actions.append(act)
    page_ids[trip_row_id] = trip_page_id
    if progress:
        progress.tick(stage="trip", row_id=trip_row_id, action=act)

    # Places
    for pl in payload.get("places") or []:
        if not isinstance(pl, dict) or not pl.get("row_id"):
            continue
        rid = pl["row_id"]
        props = build_base_properties(
            schema, cfg_props,
            name=pl.get("name") or "Place",
            type_value_str=pl.get("type") or "Place",
            row_id=rid,
            import_batch=import_batch,
            notes=(pl.get("notes") or "").strip(),
            scaffold=bool(pl.get("scaffold", False)),
        )
        if should_touch(mode, pl, "area"):
            pid, ptype = resolve_prop(schema, cfg_props.get("area"))
            set_prop(props, pid, ptype, select_value(pl.get("area")))
        if should_touch(mode, pl, "map"):
            pid, ptype = resolve_prop(schema, cfg_props.get("map"))
            set_prop(props, pid, ptype, url_value(pl.get("map")))
        if should_touch(mode, pl, "lat"):
            pid, ptype = resolve_prop(schema, cfg_props.get("lat"))
            set_prop(props, pid, ptype, number_value(pl.get("lat")))
        if should_touch(mode, pl, "lng"):
            pid, ptype = resolve_prop(schema, cfg_props.get("lng"))
            set_prop(props, pid, ptype, number_value(pl.get("lng")))
        if should_touch(mode, pl, "google_place_id"):
            pid, ptype = resolve_prop(schema, cfg_props.get("google_place_id"))
            set_prop(props, pid, ptype, rich_text_value(pl.get("google_place_id")))

        pid, ptype = resolve_prop(schema, cfg_props.get("trip_rel"))
        if relation_enabled.get("trip_rel", True) and pid and ptype == "relation":
            set_prop(props, pid, ptype, relation_value([trip_page_id]))

        should_touch_category = should_touch(mode, pl, "category") or should_touch(mode, pl, "category_row_id")
        if category_enabled and should_touch_category:
            category_value: Any = category_skip
            if "category" in pl:
                category_value = normalize_explicit_category(pl.get("category"), row_label=f"Place '{rid}'")
            elif "category_row_id" in pl:
                category_value = resolve_legacy_category_row(
                    pl.get("category_row_id"),
                    row_label=f"Place '{rid}'",
                    source_label="category_row_id",
                )
            elif mode == "canonical":
                fail_or_warn_category(
                    f"Place '{rid}': canonical mode requires 'category' or a resolvable 'category_row_id'."
                )

            if category_value is not category_skip:
                set_prop(props, category_prop_id, category_prop_type, select_value(category_value))

        if enable_place:
            pid, ptype = resolve_prop(schema, cfg_props.get("location"))
            if pid and ptype == "place":
                set_prop(props, pid, ptype, place_value(pl.get("name"), None, pl.get("lat"), pl.get("lng"), pl.get("google_place_id")))

        pl_page_id, act = upsert_page(notion, schema, row_id_prop_id, rid, props, dry_run=dry_run, strict=strict, warnings=warnings)
        actions.append(act)
        page_ids[rid] = pl_page_id
        if progress:
            progress.tick(stage="place", row_id=rid, action=act)

    # Items
    for it in payload.get("items") or []:
        if not isinstance(it, dict) or not it.get("row_id"):
            continue
        rid = it["row_id"]
        it_type = it.get("type") or "Event"
        props = build_base_properties(
            schema, cfg_props,
            name=it.get("name") or "Item",
            type_value_str=it_type,
            row_id=rid,
            import_batch=import_batch,
            notes=(it.get("notes") or "").strip(),
            scaffold=bool(it.get("scaffold", False)),
        )
        pid, ptype = resolve_prop(schema, cfg_props.get("trip_rel"))
        if relation_enabled.get("trip_rel", True) and pid and ptype == "relation":
            set_prop(props, pid, ptype, relation_value([trip_page_id]))

        if it_type == "Event":
            if should_touch(mode, it, "start"):
                pid, ptype = resolve_prop(schema, cfg_props.get("start"))
                set_prop(props, pid, ptype, date_value_start(it.get("start")))
            if should_touch(mode, it, "end"):
                pid, ptype = resolve_prop(schema, cfg_props.get("end"))
                set_prop(props, pid, ptype, date_value_start(it.get("end")))
        elif mode == "canonical":
            pid, ptype = resolve_prop(schema, cfg_props.get("start"))
            set_prop(props, pid, ptype, {"date": None})
            pid, ptype = resolve_prop(schema, cfg_props.get("end"))
            set_prop(props, pid, ptype, {"date": None})

        if should_touch(mode, it, "area"):
            pid, ptype = resolve_prop(schema, cfg_props.get("area"))
            set_prop(props, pid, ptype, select_value(it.get("area")))
        if should_touch(mode, it, "mode"):
            pid, ptype = resolve_prop(schema, cfg_props.get("mode"))
            set_prop(props, pid, ptype, select_value(it.get("mode")))
        if should_touch(mode, it, "drive_hrs"):
            pid, ptype = resolve_prop(schema, cfg_props.get("drive_hrs"))
            set_prop(props, pid, ptype, number_value(it.get("drive_hrs")))
        if should_touch(mode, it, "status"):
            pid, ptype = resolve_prop(schema, cfg_props.get("status"))
            set_prop(props, pid, ptype, select_value(it.get("status")))
        if should_touch(mode, it, "priority"):
            pid, ptype = resolve_prop(schema, cfg_props.get("priority"))
            set_prop(props, pid, ptype, select_value(it.get("priority")))

        b = normalize_bool(mode, it, "book", strict=strict, warnings=warnings, row_label=f"Item '{rid}'")
        if b is not None:
            pid, ptype = resolve_prop(schema, cfg_props.get("book"))
            set_prop(props, pid, ptype, checkbox_value_bool(b))

        if should_touch(mode, it, "link"):
            pid, ptype = resolve_prop(schema, cfg_props.get("link"))
            set_prop(props, pid, ptype, url_value(it.get("link")))
        if should_touch(mode, it, "conf"):
            pid, ptype = resolve_prop(schema, cfg_props.get("conf"))
            set_prop(props, pid, ptype, rich_text_value(it.get("conf")))
        if should_touch(mode, it, "cost_total"):
            pid, ptype = resolve_prop(schema, cfg_props.get("cost_total"))
            set_prop(props, pid, ptype, number_value(it.get("cost_total")))

        pid, ptype = resolve_prop(schema, cfg_props.get("place_rel"))
        should_touch_place_rel = should_touch(mode, it, "place_row_id")
        if relation_enabled.get("place_rel", True) and pid and ptype == "relation" and should_touch_place_rel:
            place_rid = it.get("place_row_id")
            if not place_rid:
                set_prop(props, pid, ptype, relation_value([]))
            else:
                place_page_id = page_ids.get(place_rid)
                if place_page_id:
                    set_prop(props, pid, ptype, relation_value([place_page_id]))
                else:
                    msg = f"Item '{rid}' references unknown place_row_id '{place_rid}'."
                    if strict:
                        raise NotionAPIError(msg)
                    warnings.append(msg)

        should_touch_category = (
            should_touch(mode, it, "category")
            or should_touch(mode, it, "category_row_id")
            or should_touch(mode, it, "place_row_id")
        )
        if category_enabled and should_touch_category:
            category_value: Any = category_skip
            if "category" in it:
                category_value = normalize_explicit_category(it.get("category"), row_label=f"Item '{rid}'")
            elif "category_row_id" in it:
                category_value = resolve_legacy_category_row(
                    it.get("category_row_id"),
                    row_label=f"Item '{rid}'",
                    source_label="category_row_id",
                )
            else:
                place_rid = it.get("place_row_id")
                if place_rid:
                    place_obj = places_by_row_id.get(place_rid)
                    if not place_obj:
                        fail_or_warn_category(f"Item '{rid}': place_row_id '{place_rid}' was not found in payload.places.")
                    elif "category" in place_obj:
                        category_value = normalize_legacy_category_name(
                            place_obj.get("category"),
                            row_label=f"Item '{rid}'",
                            source_label=f"place '{place_rid}' category",
                        )
                    elif "category_row_id" in place_obj:
                        category_value = resolve_legacy_category_row(
                            place_obj.get("category_row_id"),
                            row_label=f"Item '{rid}'",
                            source_label=f"place '{place_rid}' category_row_id",
                        )
                    else:
                        fail_or_warn_category(
                            f"Item '{rid}': place '{place_rid}' has no category/category_row_id to infer Category."
                        )
                elif mode == "canonical":
                    fail_or_warn_category(
                        f"Item '{rid}': canonical mode requires 'category', 'category_row_id', or inferable 'place_row_id'."
                    )

            if category_value is not category_skip:
                set_prop(props, category_prop_id, category_prop_type, select_value(category_value))

        _, act = upsert_page(notion, schema, row_id_prop_id, rid, props, dry_run=dry_run, strict=strict, warnings=warnings)
        actions.append(act)
        page_ids[rid] = _
        if progress:
            progress.tick(stage="item", row_id=rid, action=act)

    stats = {"create": actions.count("CREATE"), "update": actions.count("UPDATE"), "total": len(actions)}
    return {"page_ids": page_ids, "stats": stats, "warnings": warnings}

def cmd_init(args: argparse.Namespace) -> None:
    token = os.environ.get("NOTION_TOKEN") or args.token
    if not token:
        raise SystemExit("Missing NOTION_TOKEN env var (recommended) or --token.")
    notion = NotionClient(token, notion_version=DEFAULT_NOTION_VERSION)
    ds_id, ds_list = choose_data_source_id(notion, args.database_id, args.data_source_name)
    ds_obj = notion.retrieve_data_source(ds_id)
    schema = build_schema(ds_obj)

    print("\nData sources under this database:")
    for ds in ds_list:
        print(f"- {ds.get('name')}  id={ds.get('id')}")
    print(f"\nUsing data_source_id: {ds_id}\n")

    print("Property schema (name -> id [type]):")
    for name, p in schema.properties_by_name.items():
        print(f"- {name}: {p.get('id')} [{p.get('type')}]")

    cfg = auto_config_from_schema(schema)
    cfg["database_id"] = args.database_id
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    print(f"\nWrote config template to: {args.out}")

def cmd_apply(args: argparse.Namespace) -> None:
    token = os.environ.get("NOTION_TOKEN") or args.token
    if not token:
        raise SystemExit("Missing NOTION_TOKEN env var (recommended) or --token.")
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    database_id = config.get("database_id") or args.database_id
    if not config.get("data_source_id"):
        if not database_id:
            raise SystemExit("Config missing data_source_id and no --database-id provided. Run init first.")
        notion_tmp = NotionClient(token, notion_version=config.get("notion_version") or DEFAULT_NOTION_VERSION)
        data_source_id, _ = choose_data_source_id(notion_tmp, database_id, config.get("data_source_name"))
        config["data_source_id"] = data_source_id

    notion = NotionClient(token, notion_version=config.get("notion_version") or DEFAULT_NOTION_VERSION)
    ds_obj = notion.retrieve_data_source(config["data_source_id"])
    schema = build_schema(ds_obj)

    payload = load_payload(args.payload)
    progress = ProgressRenderer(count_sync_targets(payload), enabled=not bool(args.no_progress))
    progress.begin(mode=args.mode, dry_run=bool(args.dry_run))
    success = False
    try:
        res = sync_payload(
            notion, schema, config, payload,
            dry_run=bool(args.dry_run),
            enable_place=bool(args.enable_place),
            mode=args.mode,
            strict=bool(args.strict),
            progress=progress,
        )
        success = True
    finally:
        progress.finish(success=success)

    stats = res["stats"]
    print(f"\n{'DRY RUN' if args.dry_run else 'APPLY'} complete. Mode={args.mode}. Created={stats['create']} Updated={stats['update']} Total={stats['total']}")
    if res["warnings"]:
        print("\nWarnings:")
        for w in res["warnings"]:
            print(f"- {w}")
    if args.print_ids:
        print("\nRow IDs → Notion Page IDs:")
        for rid, pid in res["page_ids"].items():
            print(f"- {rid} -> {pid}")

def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_init = sub.add_parser("init", help="Discover data_source_id + schema; write a config template.")
    ap_init.add_argument("--database-id", required=True)
    ap_init.add_argument("--data-source-name", default=None)
    ap_init.add_argument("--out", default="config.json")
    ap_init.add_argument("--token", default=None)
    ap_init.set_defaults(func=cmd_init)

    ap_apply = sub.add_parser("apply", help="Upsert a TripPlan payload into Notion.")
    ap_apply.add_argument("--config", required=True)
    ap_apply.add_argument("--payload", required=True)
    ap_apply.add_argument("--mode", choices=["patch", "canonical"], default="patch")
    ap_apply.add_argument("--strict", action="store_true")
    ap_apply.add_argument("--dry-run", action="store_true")
    ap_apply.add_argument("--enable-place", action="store_true")
    ap_apply.add_argument("--print-ids", action="store_true")
    ap_apply.add_argument("--no-progress", action="store_true", help="Disable progress rendering during apply.")
    ap_apply.add_argument("--database-id", default=None)
    ap_apply.add_argument("--token", default=None)
    ap_apply.set_defaults(func=cmd_apply)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
