from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from streaming_analytics.generator import generate_events, write_events
from streaming_analytics.local_pipeline import run_pipeline


class LocalPipelineTests(unittest.TestCase):
    def test_pipeline_deduplicates_and_quarantines(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "events.ndjson"
            output_path = root / "output"
            write_events(input_path, list(generate_events(20, seed=5)), include_edge_cases=True)

            summary = run_pipeline(input_path, output_path, run_id="test-run")

            self.assertEqual(summary["input_records"], 22)
            self.assertEqual(summary["valid_records"], 20)
            self.assertEqual(summary["duplicate_records"], 1)
            self.assertEqual(summary["quarantined_records"], 1)
            self.assertTrue((output_path / "gold" / "content_daily.csv").exists())
            persisted = json.loads((output_path / "run_summary.json").read_text())
            self.assertEqual(persisted["run_id"], "test-run")


if __name__ == "__main__":
    unittest.main()

