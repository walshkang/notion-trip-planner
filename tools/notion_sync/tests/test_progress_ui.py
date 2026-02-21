import io
import unittest

from helpers import load_sync_module

MODULE = load_sync_module()


class TestProgressUi(unittest.TestCase):
    def test_count_sync_targets_counts_only_objects_with_row_id(self) -> None:
        payload = {
            "trip": {"row_id": "trip_1"},
            "categories": [{"row_id": "cat_1"}, {"name": "missing"}],
            "places": [{"row_id": "place_1"}, "bad"],
            "items": [{"row_id": "item_1"}, {"row_id": ""}],
        }
        self.assertEqual(MODULE.count_sync_targets(payload), 4)

    def test_progress_renderer_non_tty_writes_status_lines(self) -> None:
        buf = io.StringIO()
        progress = MODULE.ProgressRenderer(total=2, enabled=True, stream=buf)
        progress.begin(mode="patch", dry_run=True)
        progress.tick(stage="trip", row_id="trip_1", action="CREATE")
        progress.tick(stage="item", row_id="item_1", action="UPDATE")
        progress.finish(success=True)

        out = buf.getvalue()
        self.assertIn("[sync] DRY RUN mode=patch targets=2", out)
        self.assertIn("1/2", out)
        self.assertIn("2/2", out)
        self.assertIn("[sync] done in", out)


if __name__ == "__main__":
    unittest.main()
