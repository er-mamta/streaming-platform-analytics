from __future__ import annotations

import unittest

from streaming_analytics.generator import generate_events
from streaming_analytics.metrics import (
    aggregate_content_daily,
    aggregate_platform_hourly,
    calculate_qoe_score,
)


class MetricTests(unittest.TestCase):
    def test_qoe_score_is_bounded_and_penalizes_bad_playback(self) -> None:
        healthy = calculate_qoe_score(500, 0.001, 0.0)
        degraded = calculate_qoe_score(4_000, 0.20, 0.50)
        self.assertGreater(healthy, degraded)
        self.assertGreaterEqual(degraded, 0)
        self.assertLessEqual(healthy, 100)

    def test_daily_and_hourly_rollups_preserve_event_counts(self) -> None:
        events = list(generate_events(50, seed=11))
        daily = aggregate_content_daily(events)
        hourly = aggregate_platform_hourly(events)
        self.assertEqual(sum(row["event_count"] for row in daily), len(events))
        self.assertEqual(sum(row["event_count"] for row in hourly), len(events))


if __name__ == "__main__":
    unittest.main()

