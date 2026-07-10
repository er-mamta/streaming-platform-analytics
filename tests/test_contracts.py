from __future__ import annotations

import unittest

from streaming_analytics.contracts import parse_utc_timestamp, validate_event
from streaming_analytics.generator import generate_events


class ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = next(generate_events(1, seed=7))

    def test_generated_event_satisfies_contract(self) -> None:
        result = validate_event(self.event)
        self.assertTrue(result.is_valid, result.errors)

    def test_negative_watch_time_and_new_schema_are_quarantined(self) -> None:
        invalid = {**self.event, "watch_seconds": -1, "schema_version": 2}
        result = validate_event(invalid)
        self.assertFalse(result.is_valid)
        self.assertIn("invalid_non_negative_number:watch_seconds", result.errors)
        self.assertIn("unsupported:schema_version", result.errors)

    def test_timestamp_requires_timezone(self) -> None:
        self.assertIsNone(parse_utc_timestamp("2026-07-01T10:30:00"))
        self.assertIsNotNone(parse_utc_timestamp("2026-07-01T10:30:00Z"))


if __name__ == "__main__":
    unittest.main()

