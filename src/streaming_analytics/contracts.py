"""Playback-event data contract and dependency-free validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

EVENT_TYPES = {
    "playback_started",
    "playback_progress",
    "playback_completed",
    "playback_error",
}
DEVICE_TYPES = {"connected_tv", "mobile", "tablet", "web"}
SUBSCRIPTION_TIERS = {"ad_supported", "standard", "premium"}
REQUIRED_STRING_FIELDS = (
    "event_id",
    "event_type",
    "event_ts",
    "ingestion_ts",
    "user_id",
    "profile_id",
    "session_id",
    "content_id",
    "device_type",
    "country",
    "subscription_tier",
)


@dataclass(frozen=True)
class ValidationResult:
    """Result returned by the event-contract validator."""

    is_valid: bool
    errors: tuple[str, ...]


def parse_utc_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp and require timezone information."""

    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _is_non_negative_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def validate_event(event: Any) -> ValidationResult:
    """Validate an event without mutating it.

    The checks intentionally model a Silver-layer quality gate: required keys,
    accepted dimensions, timestamps, numeric ranges, and schema compatibility.
    """

    if not isinstance(event, dict):
        return ValidationResult(False, ("record_must_be_an_object",))

    errors: list[str] = []
    for field in REQUIRED_STRING_FIELDS:
        if not isinstance(event.get(field), str) or not event[field].strip():
            errors.append(f"missing_or_invalid:{field}")

    if event.get("event_type") not in EVENT_TYPES:
        errors.append("unsupported:event_type")
    if event.get("device_type") not in DEVICE_TYPES:
        errors.append("unsupported:device_type")
    if event.get("subscription_tier") not in SUBSCRIPTION_TIERS:
        errors.append("unsupported:subscription_tier")
    if parse_utc_timestamp(event.get("event_ts")) is None:
        errors.append("invalid:event_ts")
    if parse_utc_timestamp(event.get("ingestion_ts")) is None:
        errors.append("invalid:ingestion_ts")

    for field in (
        "position_seconds",
        "watch_seconds",
        "startup_ms",
        "bitrate_kbps",
        "rebuffer_seconds",
    ):
        value = event.get(field)
        if value is not None and not _is_non_negative_number(value):
            errors.append(f"invalid_non_negative_number:{field}")

    watch_seconds = event.get("watch_seconds")
    rebuffer_seconds = event.get("rebuffer_seconds")
    if _is_non_negative_number(watch_seconds) and _is_non_negative_number(rebuffer_seconds):
        if rebuffer_seconds > watch_seconds:
            errors.append("invalid:rebuffer_exceeds_watch_time")

    if event.get("schema_version") != 1:
        errors.append("unsupported:schema_version")

    return ValidationResult(not errors, tuple(errors))

