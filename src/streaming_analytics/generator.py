"""Deterministic synthetic playback-event generator."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

CONTENT_CATALOG = (
    ("cnt_101", "Orbital Dawn"),
    ("cnt_102", "The Last Signal"),
    ("cnt_103", "Kitchen Passport"),
    ("cnt_104", "Wild Coast"),
    ("cnt_105", "Codebreakers"),
    ("cnt_106", "Midnight Metro"),
)
COUNTRIES = ("US", "CA", "GB", "IN", "MX")
DEVICES = ("connected_tv", "mobile", "tablet", "web")
TIERS = ("ad_supported", "standard", "premium")


def _hashed_id(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def generate_events(count: int, seed: int = 42) -> Iterator[dict[str, Any]]:
    """Yield deterministic, privacy-safe playback telemetry."""

    randomizer = random.Random(seed)
    base_time = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
    emitted = 0
    session_number = 0

    while emitted < count:
        session_number += 1
        raw_user = f"synthetic-user-{randomizer.randint(1, max(20, count // 4))}"
        user_id = _hashed_id("usr", raw_user)
        profile_id = _hashed_id("prf", f"{raw_user}-{randomizer.randint(1, 3)}")
        session_uuid = uuid.uuid5(uuid.NAMESPACE_URL, f"{seed}-session-{session_number}")
        session_id = f"ses_{session_uuid.hex[:16]}"
        content_id, content_title = randomizer.choice(CONTENT_CATALOG)
        device_type = randomizer.choices(DEVICES, weights=(38, 32, 10, 20), k=1)[0]
        country = randomizer.choices(COUNTRIES, weights=(58, 10, 10, 14, 8), k=1)[0]
        subscription_tier = randomizer.choices(TIERS, weights=(35, 40, 25), k=1)[0]
        event_time = base_time + timedelta(seconds=randomizer.randint(0, 172_799))
        ingestion_lag_seconds = randomizer.randint(0, 80)
        startup_ms = int(max(150, randomizer.gauss(1_250, 600)))
        bitrate = randomizer.choice((800, 1_500, 2_500, 4_000, 6_000))
        position = 0.0

        def make_event(event_type: str, watch_seconds: float, **overrides: Any) -> dict[str, Any]:
            nonlocal emitted, event_time, ingestion_lag_seconds, position
            event_number = emitted + 1
            event_uuid = uuid.uuid5(uuid.NAMESPACE_URL, f"{seed}-event-{event_number}")
            event = {
                "event_id": f"evt_{event_uuid.hex}",
                "event_type": event_type,
                "event_ts": _iso(event_time),
                "ingestion_ts": _iso(event_time + timedelta(seconds=ingestion_lag_seconds)),
                "user_id": user_id,
                "profile_id": profile_id,
                "session_id": session_id,
                "content_id": content_id,
                "content_title": content_title,
                "device_type": device_type,
                "country": country,
                "subscription_tier": subscription_tier,
                "position_seconds": round(position, 2),
                "watch_seconds": round(watch_seconds, 2),
                "startup_ms": startup_ms if event_type == "playback_started" else None,
                "bitrate_kbps": bitrate,
                "rebuffer_seconds": round(overrides.pop("rebuffer_seconds", 0.0), 2),
                "error_code": overrides.pop("error_code", None),
                "schema_version": 1,
            }
            event.update(overrides)
            emitted += 1
            return event

        yield make_event("playback_started", 0.0)
        if emitted >= count:
            continue

        for _ in range(randomizer.randint(1, 3)):
            if emitted >= count:
                break
            watched = float(randomizer.randint(45, 240))
            event_time += timedelta(seconds=int(watched))
            position += watched
            rebuffer = randomizer.choices(
                (0.0, randomizer.uniform(0.2, 8.0)), weights=(82, 18), k=1
            )[0]
            yield make_event("playback_progress", watched, rebuffer_seconds=rebuffer)

        if emitted >= count:
            continue
        event_time += timedelta(seconds=randomizer.randint(5, 60))
        if randomizer.random() < 0.08:
            yield make_event(
                "playback_error",
                0.0,
                error_code=randomizer.choice(("NETWORK_TIMEOUT", "DECODER_ERROR", "DRM_FAILURE")),
            )
        elif randomizer.random() < 0.72:
            yield make_event("playback_completed", 0.0)


def write_events(path: Path, events: list[dict[str, Any]], include_edge_cases: bool) -> int:
    """Write NDJSON and optionally append one duplicate and one invalid event."""

    path.parent.mkdir(parents=True, exist_ok=True)
    records = list(events)
    if include_edge_cases and records:
        records.append(dict(records[0]))
        invalid = dict(records[-2])
        invalid["event_id"] = "evt_invalid_contract_example"
        invalid["watch_seconds"] = -5
        invalid["schema_version"] = 99
        records.append(invalid)

    with path.open("w", encoding="utf-8") as output:
        for event in records:
            output.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
    return len(records)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=250, help="Number of valid events")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument("--output", type=Path, default=Path("data/sample/events.ndjson"))
    parser.add_argument("--include-edge-cases", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.count < 1:
        raise SystemExit("--count must be at least 1")
    total = write_events(
        args.output,
        list(generate_events(args.count, args.seed)),
        args.include_edge_cases,
    )
    print(f"Wrote {total} records to {args.output}")


if __name__ == "__main__":
    main()
