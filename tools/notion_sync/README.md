# TripPlan -> Notion Sync (Repo Managed)

Sync TripPlan Compiler payload JSON into one Notion database/data source with deterministic upsert behavior.

## Runtime Requirements
- Python 3.10+
- A Notion internal integration token
- Your Notion database shared with that integration

## Install (reproducible)
```bash
cd /Users/walsh.kang/Documents/GitHub/notion-trip-planner/tools/notion_sync
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configure environment
```bash
cp .env.example .env
# edit .env and set NOTION_TOKEN and NOTION_DATABASE_ID
set -a; source .env; set +a
```

## Bootstrap schema config
```bash
python notion_trip_sync.py init --database-id "$NOTION_DATABASE_ID" --out config.json
```

If your database has multiple data sources, select by name:
```bash
python notion_trip_sync.py init \
  --database-id "$NOTION_DATABASE_ID" \
  --data-source-name "$NOTION_DATA_SOURCE_NAME" \
  --out config.json
```

## Apply payload
```bash
python notion_trip_sync.py apply --config config.json --payload payload.json
```

Payload may be either:
- raw JSON
- a transcript containing a fenced JSON block (```json ... ```)

## First-live-write safety policy
Always do first run with strict dry-run patch mode:
```bash
python notion_trip_sync.py apply \
  --config config.json \
  --payload payload.json \
  --mode patch \
  --strict \
  --dry-run \
  --print-ids
```

Then run the same command without `--dry-run`.

## Sync modes
- `--mode patch` (default): missing keys do not touch existing Notion values.
- `--mode canonical`: payload is authoritative; missing fields may be cleared.

## Schema gate behavior
The script validates schema mappings before writing:

Hard-required (always fail on missing/wrong type):
- `properties.title` -> `title`
- `properties.type` -> `select`
- `properties.row_id` -> `rich_text`

Relation-required (`trip_rel`, `category_rel`, `place_rel`):
- strict or canonical: fail on missing/wrong type
- non-strict patch: warn and skip relation writes

## Checkbox (`book`) behavior
- Patch mode:
  - key missing: no update
  - explicit `null`: warn and skip (strict mode fails)
- Canonical mode:
  - missing or `null`: set false
- Non-boolean values:
  - strict mode: fail
  - non-strict mode: coerce with warning

## Place column
Notion Place fields can be inconsistent via API. Place writes are disabled by default.

To attempt place writes:
```bash
python notion_trip_sync.py apply --config config.json --payload payload.json --enable-place
```

## First live write checklist
1. Database is shared with the integration.
2. `init` output confirms `Row ID` is `rich_text`.
3. Dry run (`patch + strict + dry-run`) has zero unreviewed warnings.
4. First live apply uses the same flags minus `--dry-run`.
5. Verify in Notion: Row IDs populated, relations present, no unexpected clears.
