import unittest
from unittest.mock import patch

from helpers import load_sync_module

MODULE = load_sync_module()


def make_schema(type_overrides=None):
    type_overrides = type_overrides or {}
    base = {
        "Name": "title",
        "Type": "select",
        "Row ID": "rich_text",
        "Import Batch": "rich_text",
        "Notes": "rich_text",
        "Scaffold": "checkbox",
        "Trip (link)": "relation",
        "Category": "select",
        "Place (link)": "relation",
    }
    base.update(type_overrides)

    props_by_name = {}
    props_by_id = {}
    id_by_name = {}
    name_by_id = {}
    type_by_id = {}

    for idx, (name, ptype) in enumerate(base.items()):
        pid = f"p{idx}"
        prop = {"id": pid, "type": ptype}
        props_by_name[name] = prop
        props_by_id[pid] = prop
        id_by_name[name] = pid
        name_by_id[pid] = name
        type_by_id[pid] = ptype

    return MODULE.DataSourceSchema(
        data_source_id="ds_1",
        properties_by_name=props_by_name,
        properties_by_id=props_by_id,
        id_by_name=id_by_name,
        name_by_id=name_by_id,
        type_by_id=type_by_id,
    )


def make_config():
    return {
        "properties": {
            "title": "Name",
            "type": "Type",
            "row_id": "Row ID",
            "import_batch": "Import Batch",
            "notes": "Notes",
            "scaffold": "Scaffold",
            "trip_rel": "Trip (link)",
            "category": "Category",
            "place_rel": "Place (link)",
        }
    }


def run_sync(payload, *, mode="patch", strict=False, schema=None, config=None):
    schema = schema or make_schema()
    config = config or make_config()
    captured_props = {}

    def fake_upsert(_notion, _schema, _row_id_prop_id, row_id_value_str, properties, *, dry_run, strict, warnings):
        captured_props[row_id_value_str] = dict(properties)
        return f"PAGE-{row_id_value_str}", "CREATE"

    with patch.object(MODULE, "upsert_page", side_effect=fake_upsert):
        res = MODULE.sync_payload(
            notion=None,
            schema=schema,
            config=config,
            payload=payload,
            dry_run=True,
            enable_place=False,
            mode=mode,
            strict=strict,
        )
    return res, captured_props, schema


class TestCategorySelectMigration(unittest.TestCase):
    def test_place_legacy_category_row_id_maps_to_allowed_select(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "categories": [{"row_id": "cat_meal", "name": "Meal"}],
            "places": [{"row_id": "place_1", "name": "Dinner Spot", "type": "Place", "category_row_id": "cat_meal"}],
            "items": [],
        }

        _, captured, schema = run_sync(payload, mode="patch", strict=True)
        category_pid = schema.id_by_name["Category"]
        self.assertEqual(captured["place_1"][category_pid]["select"]["name"], "Food")

    def test_patch_non_strict_unknown_category_warns_and_skips(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "places": [{"row_id": "place_1", "name": "Night Spot", "type": "Place", "category": "Nightlife"}],
            "items": [],
        }

        res, captured, schema = run_sync(payload, mode="patch", strict=False)
        category_pid = schema.id_by_name["Category"]
        self.assertTrue(any("Unknown category" in w for w in res["warnings"]))
        self.assertNotIn(category_pid, captured["place_1"])

    def test_canonical_unknown_category_fails(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "places": [{"row_id": "place_1", "name": "Night Spot", "type": "Place", "category": "Nightlife"}],
            "items": [],
        }

        with self.assertRaises(ValueError) as ctx:
            run_sync(payload, mode="canonical", strict=False)
        self.assertIn("Unknown category", str(ctx.exception))

    def test_patch_non_strict_unresolved_legacy_category_row_id_warns_and_skips(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "categories": [{"row_id": "cat_known", "name": "Coffee"}],
            "places": [{"row_id": "place_1", "name": "Cafe", "type": "Place", "category_row_id": "cat_missing"}],
            "items": [],
        }

        res, captured, schema = run_sync(payload, mode="patch", strict=False)
        category_pid = schema.id_by_name["Category"]
        self.assertTrue(any("category_row_id 'cat_missing' was not found" in w for w in res["warnings"]))
        self.assertNotIn(category_pid, captured["place_1"])

    def test_canonical_unresolved_legacy_category_row_id_fails(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "categories": [{"row_id": "cat_known", "name": "Coffee"}],
            "places": [{"row_id": "place_1", "name": "Cafe", "type": "Place", "category_row_id": "cat_missing"}],
            "items": [],
        }

        with self.assertRaises(ValueError) as ctx:
            run_sync(payload, mode="canonical", strict=False)
        self.assertIn("category_row_id 'cat_missing' was not found", str(ctx.exception))

    def test_item_infers_category_from_place_legacy_mapping(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "categories": [{"row_id": "cat_meal", "name": "Meal"}],
            "places": [{"row_id": "place_1", "name": "Dinner Spot", "type": "Place", "category_row_id": "cat_meal"}],
            "items": [{"row_id": "item_1", "name": "Dinner", "type": "Event", "place_row_id": "place_1"}],
        }

        _, captured, schema = run_sync(payload, mode="patch", strict=True)
        category_pid = schema.id_by_name["Category"]
        self.assertEqual(captured["item_1"][category_pid]["select"]["name"], "Food")

    def test_explicit_category_null_clears_select(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "places": [{"row_id": "place_1", "name": "Flexible", "type": "Place", "category": None}],
            "items": [],
        }

        _, captured, schema = run_sync(payload, mode="patch", strict=True)
        category_pid = schema.id_by_name["Category"]
        self.assertIsNone(captured["place_1"][category_pid]["select"])

    def test_canonical_requires_resolvable_category_on_place(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "places": [{"row_id": "place_1", "name": "No Category", "type": "Place"}],
            "items": [],
        }

        with self.assertRaises(ValueError) as ctx:
            run_sync(payload, mode="canonical", strict=False)
        self.assertIn("canonical mode requires 'category' or a resolvable 'category_row_id'", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
