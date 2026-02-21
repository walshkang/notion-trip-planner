import unittest

from helpers import load_sync_module

MODULE = load_sync_module()


class TestPayloadExtract(unittest.TestCase):
    def test_extract_raw_json(self) -> None:
        text = '{"trip": {"row_id": "trip_1"}}'
        out = MODULE._extract_json_from_text(text)
        self.assertEqual(out["trip"]["row_id"], "trip_1")

    def test_extract_fenced_json_block(self) -> None:
        text = """Some intro text

```json
{
  \"trip\": {\"row_id\": \"trip_2\"}
}
```

Some trailing text
"""
        out = MODULE._extract_json_from_text(text)
        self.assertEqual(out["trip"]["row_id"], "trip_2")

    def test_extract_raises_on_missing_json(self) -> None:
        with self.assertRaises(ValueError):
            MODULE._extract_json_from_text("hello world")


if __name__ == "__main__":
    unittest.main()
