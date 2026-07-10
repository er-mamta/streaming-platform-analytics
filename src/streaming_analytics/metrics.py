"""Business-facing Gold-layer aggregations."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable

from streaming_analytics.contracts import parse_utc_timestamp


def calculate_qoe_score(avg_startup_ms: float, rebuffer_ratio: float, error_rate: float) -> float:
    """Return a transparent 0-100 playback quality score.

    This is an illustrative, explainable score rather than a proprietary formula.
    Penalties are capped so one dimension cannot dominate the entire score.
    """

    startup_penalty = min(avg_startup_ms / 100.0, 30.0)
    rebuffer_penalty = min(rebuffer_ratio * 500.0, 40.0)
    error_penalty = min(error_rate * 100.0, 30.0)
    return round(max(0.0, 100.0 - startup_penalty - rebuffer_penalty - error_penalty), 2)


def _new_accumulator() -> dict[str, Any]:
    return {
        "event_count": 0,
        "plays": 0,
        "completes": 0,
        "errors": 0,
        "watch_seconds": 0.0,
        "rebuffer_seconds": 0.0,
        "startup_total_ms": 0.0,
        "startup_samples": 0,
        "viewers": set(),
    }


def _update(accumulator: dict[str, Any], event: dict[str, Any]) -> None:
    event_type = event["event_type"]
    accumulator["event_count"] += 1
    accumulator["plays"] += int(event_type == "playback_started")
    accumulator["completes"] += int(event_type == "playback_completed")
    accumulator["errors"] += int(event_type == "playback_error")
    accumulator["watch_seconds"] += float(event.get("watch_seconds") or 0.0)
    accumulator["rebuffer_seconds"] += float(event.get("rebuffer_seconds") or 0.0)
    accumulator["viewers"].add(event["user_id"])
    if event.get("startup_ms") is not None:
        accumulator["startup_total_ms"] += float(event["startup_ms"])
        accumulator["startup_samples"] += 1


def _finalize(dimensions: dict[str, Any], accumulator: dict[str, Any]) -> dict[str, Any]:
    plays = accumulator["plays"]
    watch_seconds = accumulator["watch_seconds"]
    startup_samples = accumulator["startup_samples"]
    completion_rate = accumulator["completes"] / plays if plays else 0.0
    error_rate = accumulator["errors"] / plays if plays else 0.0
    rebuffer_ratio = accumulator["rebuffer_seconds"] / watch_seconds if watch_seconds else 0.0
    avg_startup_ms = accumulator["startup_total_ms"] / startup_samples if startup_samples else 0.0
    return {
        **dimensions,
        "event_count": accumulator["event_count"],
        "plays": plays,
        "unique_viewers": len(accumulator["viewers"]),
        "completes": accumulator["completes"],
        "playback_errors": accumulator["errors"],
        "watch_hours": round(watch_seconds / 3_600.0, 4),
        "completion_rate": round(completion_rate, 4),
        "error_rate": round(error_rate, 4),
        "rebuffer_ratio": round(rebuffer_ratio, 4),
        "avg_startup_ms": round(avg_startup_ms, 2),
        "qoe_score": calculate_qoe_score(avg_startup_ms, rebuffer_ratio, error_rate),
    }


def aggregate_content_daily(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate engagement and quality metrics by content, market, tier, and day."""

    groups: dict[tuple[str, str, str, str, str], dict[str, Any]] = defaultdict(_new_accumulator)
    titles: dict[str, str] = {}
    for event in events:
        timestamp = parse_utc_timestamp(event["event_ts"])
        if timestamp is None:
            continue
        key = (
            timestamp.date().isoformat(),
            event["content_id"],
            event["country"],
            event["subscription_tier"],
            event["device_type"],
        )
        titles[event["content_id"]] = event.get("content_title", "Unknown")
        _update(groups[key], event)

    rows = []
    for key in sorted(groups):
        event_date, content_id, country, tier, device_type = key
        rows.append(
            _finalize(
                {
                    "event_date": event_date,
                    "content_id": content_id,
                    "content_title": titles[content_id],
                    "country": country,
                    "subscription_tier": tier,
                    "device_type": device_type,
                },
                groups[key],
            )
        )
    return rows


def aggregate_platform_hourly(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate platform health by event hour, market, and device."""

    groups: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(_new_accumulator)
    for event in events:
        timestamp = parse_utc_timestamp(event["event_ts"])
        if timestamp is None:
            continue
        hour = datetime(
            timestamp.year,
            timestamp.month,
            timestamp.day,
            timestamp.hour,
            tzinfo=timestamp.tzinfo,
        ).isoformat()
        key = (hour, event["country"], event["device_type"])
        _update(groups[key], event)

    return [
        _finalize(
            {"window_start": hour, "country": country, "device_type": device_type},
            groups[(hour, country, device_type)],
        )
        for hour, country, device_type in sorted(groups)
    ]

