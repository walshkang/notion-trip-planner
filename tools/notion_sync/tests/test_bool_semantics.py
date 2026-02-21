import unittest

from helpers import load_sync_module

MODULE = load_sync_module()


class TestBoolSemantics(unittest.TestCase):
    def test_patch_missing_key_does_not_touch(self) -> None:
        warnings = []
        out = MODULE.normalize_bool("patch", {}, "book", strict=False, warnings=warnings, row_label="Item 'i1'")
        self.assertIsNone(out)
        self.assertEqual(warnings, [])

    def test_patch_null_non_strict_warns_and_skips(self) -> None:
        warnings = []
        out = MODULE.normalize_bool("patch", {"book": None}, "book", strict=False, warnings=warnings, row_label="Item 'i1'")
        self.assertIsNone(out)
        self.assertEqual(len(warnings), 1)
        self.assertIn("null", warnings[0])

    def test_patch_null_strict_fails(self) -> None:
        with self.assertRaises(ValueError):
            MODULE.normalize_bool("patch", {"book": None}, "book", strict=True, warnings=[], row_label="Item 'i1'")

    def test_canonical_missing_sets_false(self) -> None:
        warnings = []
        out = MODULE.normalize_bool("canonical", {}, "book", strict=False, warnings=warnings, row_label="Item 'i1'")
        self.assertFalse(out)
        self.assertEqual(warnings, [])

    def test_canonical_non_boolean_non_strict_coerces_with_warning(self) -> None:
        warnings = []
        out = MODULE.normalize_bool("canonical", {"book": "yes"}, "book", strict=False, warnings=warnings, row_label="Item 'i1'")
        self.assertTrue(out)
        self.assertEqual(len(warnings), 1)
        self.assertIn("coercing", warnings[0])


if __name__ == "__main__":
    unittest.main()
