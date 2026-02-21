import unittest
from unittest.mock import patch

from helpers import load_sync_module

MODULE = load_sync_module()


def make_schema():
    base = {
        "Name": "title",
        "Type": "select",
        "Row ID": "rich_text",
        "Import Batch": "rich_text",
        "Notes": "rich_text",
        "Scaffold": "checkbox",
        "Trip (link)": "relation",
        "Category (link)": "relation",
        "Place (link)": "relation",
        "Start": "date",
        "End": "date",
        "Book?": "checkbox",
    }

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
            "category_rel": "Category (link)",
            "place_rel": "Place (link)",
            "start": "Start",
            "end": "End",
            "book": "Book?",
        }
    }


class TestRelationClearingGuard(unittest.TestCase):
    def test_patch_mode_missing_place_row_id_does_not_clear_place_relation(self) -> None:
        schema = make_schema()
        config = make_config()
        payload = {
            "import_batch": "batch_1",
            "trip": {"row_id": "trip_1", "name": "Trip"},
            "places": [{"row_id": "place_1", "name": "Hotel", "type": "Place"}],
            "items": [
                {
                    "row_id": "item_1",
                    "name": "Dinner",
                    "type": "Event",
                    "start": "2026-02-21",
                }
            ],
        }

        captured_props = {}

        def fake_upsert(_notion, _schema, _row_id_prop_id, row_id_value_str, properties, *, dry_run, strict, warnings):
            captured_props[row_id_value_str] = dict(properties)
            return f"PAGE-{row_id_value_str}", "CREATE"

        with patch.object(MODULE, "upsert_page", side_effect=fake_upsert):
            MODULE.sync_payload(
                notion=None,
                schema=schema,
                config=config,
                payload=payload,
                dry_run=True,
                enable_place=False,
                mode="patch",
                strict=True,
            )

        place_rel_id = schema.id_by_name["Place (link)"]
        item_props = captured_props["item_1"]
        self.assertNotIn(place_rel_id, item_props)


if __name__ == "__main__":
    unittest.main()
