import unittest

from helpers import load_sync_module

MODULE = load_sync_module()


def make_schema(type_overrides=None):
    type_overrides = type_overrides or {}
    base = {
        "Name": "title",
        "Type": "select",
        "Row ID": "rich_text",
        "Trip (link)": "relation",
        "Category (link)": "relation",
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


def make_cfg(include_relations=True):
    cfg = {
        "title": "Name",
        "type": "Type",
        "row_id": "Row ID",
    }
    if include_relations:
        cfg.update(
            {
                "trip_rel": "Trip (link)",
                "category_rel": "Category (link)",
                "place_rel": "Place (link)",
            }
        )
    return cfg


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


if __name__ == "__main__":
    unittest.main()
