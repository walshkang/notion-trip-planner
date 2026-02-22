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
        "Trip (link)": "relation",
        "Place (link)": "relation",
        "Category": "select",
        "Import Batch": "rich_text",
        "Notes": "rich_text",
        "Scaffold": "checkbox",
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


def make_cfg(include_relations=True):
    cfg = {
        "title": "Name",
        "type": "Type",
        "row_id": "Row ID",
        "import_batch": "Import Batch",
        "notes": "Notes",
        "scaffold": "Scaffold",
        "category": "Category",
    }
    if include_relations:
        cfg.update(
            {
                "trip_rel": "Trip (link)",
                "place_rel": "Place (link)",
            }
        )
    return cfg


def make_config():
    return {"properties": make_cfg()}


def make_payload_with_category():
    return {
        "import_batch": "batch_1",
        "trip": {"row_id": "trip_1", "name": "Trip"},
        "places": [{"row_id": "place_1", "name": "Cafe", "type": "Place", "category": "Coffee"}],
        "items": [],
    }


class TestSchemaGate(unittest.TestCase):
    def test_hard_required_missing_fails(self) -> None:
        schema = make_schema()
        cfg = make_cfg()
        del cfg["row_id"]
        with self.assertRaises(RuntimeError) as ctx:
            MODULE.validate_schema_requirements(schema, cfg, mode="patch", strict=False, warnings=[])
        self.assertIn("properties.row_id", str(ctx.exception))

    def test_relation_missing_patch_non_strict_warns_and_skips(self) -> None:
        schema = make_schema()
        cfg = make_cfg()
        del cfg["place_rel"]
        warnings = []
        relation_enabled = MODULE.validate_schema_requirements(schema, cfg, mode="patch", strict=False, warnings=warnings)
        self.assertFalse(relation_enabled["place_rel"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("place_rel", warnings[0])

    def test_relation_missing_patch_strict_fails(self) -> None:
        schema = make_schema()
        cfg = make_cfg()
        del cfg["trip_rel"]
        with self.assertRaises(RuntimeError):
            MODULE.validate_schema_requirements(schema, cfg, mode="patch", strict=True, warnings=[])

    def test_relation_type_mismatch_canonical_fails(self) -> None:
        schema = make_schema({"Place (link)": "select"})
        cfg = make_cfg()
        with self.assertRaises(RuntimeError) as ctx:
            MODULE.validate_schema_requirements(schema, cfg, mode="canonical", strict=False, warnings=[])
        self.assertIn("expected 'relation'", str(ctx.exception))

    def test_category_missing_patch_non_strict_warns_and_skips(self) -> None:
        schema = make_schema()
        config = make_config()
        del config["properties"]["category"]
        payload = make_payload_with_category()
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
                mode="patch",
                strict=False,
            )

        self.assertTrue(any("properties.category" in w for w in res["warnings"]))
        category_prop_id = schema.id_by_name["Category"]
        self.assertNotIn(category_prop_id, captured_props["place_1"])

    def test_category_type_mismatch_patch_non_strict_warns_and_skips(self) -> None:
        schema = make_schema({"Category": "rich_text"})
        config = make_config()
        payload = make_payload_with_category()
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
                mode="patch",
                strict=False,
            )

        self.assertTrue(any("expected 'select'" in w for w in res["warnings"]))
        category_prop_id = schema.id_by_name["Category"]
        self.assertNotIn(category_prop_id, captured_props["place_1"])

    def test_category_missing_patch_strict_fails(self) -> None:
        schema = make_schema()
        config = make_config()
        del config["properties"]["category"]
        payload = make_payload_with_category()

        with self.assertRaises(RuntimeError) as ctx:
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
        self.assertIn("properties.category", str(ctx.exception))

    def test_category_missing_canonical_fails_even_non_strict(self) -> None:
        schema = make_schema()
        config = make_config()
        del config["properties"]["category"]
        payload = make_payload_with_category()

        with self.assertRaises(RuntimeError) as ctx:
            MODULE.sync_payload(
                notion=None,
                schema=schema,
                config=config,
                payload=payload,
                dry_run=True,
                enable_place=False,
                mode="canonical",
                strict=False,
            )
        self.assertIn("properties.category", str(ctx.exception))

    def test_category_type_mismatch_canonical_fails_even_non_strict(self) -> None:
        schema = make_schema({"Category": "rich_text"})
        config = make_config()
        payload = make_payload_with_category()

        with self.assertRaises(RuntimeError) as ctx:
            MODULE.sync_payload(
                notion=None,
                schema=schema,
                config=config,
                payload=payload,
                dry_run=True,
                enable_place=False,
                mode="canonical",
                strict=False,
            )
        self.assertIn("expected 'select'", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
