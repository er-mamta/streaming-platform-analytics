"""Dependency-free mini-batch implementation of the Medallion pipeline."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from streaming_analytics.contracts import validate_event
from streaming_analytics.metrics import aggregate_content_daily, aggregate_platform_hourly


def _write_ndjson(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def run_pipeline(
    input_path: Path, output_dir: Path, run_id: str | None = None
) -> dict[str, Any]:
    """Materialize Bronze, Silver, quarantine, and two Gold marts."""

    run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    bronze: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    valid_events: list[dict[str, Any]] = []
    seen_event_ids: set[str] = set()
    duplicate_records = 0

    with input_path.open("r", encoding="utf-8") as source:
        for line_number, raw_line in enumerate(source, start=1):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError as error:
                bronze.append(
                    {"raw_payload": raw_line, "source_line": line_number, "run_id": run_id}
                )
                quarantine.append(
                    {
                        "raw_payload": raw_line,
                        "source_line": line_number,
                        "errors": [f"invalid_json:{error.msg}"],
                        "run_id": run_id,
                    }
                )
                continue

            bronze.append({"event": event, "source_line": line_number, "run_id": run_id})
            validation = validate_event(event)
            if not validation.is_valid:
                quarantine.append(
                    {
                        "event": event,
                        "source_line": line_number,
                        "errors": list(validation.errors),
                        "run_id": run_id,
                    }
                )
                continue

            event_id = event["event_id"]
            if event_id in seen_event_ids:
                duplicate_records += 1
                continue
            seen_event_ids.add(event_id)
            valid_events.append(event)

    valid_events.sort(key=lambda event: (event["event_ts"], event["event_id"]))
    content_daily = aggregate_content_daily(valid_events)
    platform_hourly = aggregate_platform_hourly(valid_events)

    _write_ndjson(output_dir / "bronze" / "playback_events.ndjson", bronze)
    _write_ndjson(output_dir / "silver" / "playback_events.ndjson", valid_events)
    _write_ndjson(output_dir / "quarantine" / "playback_events.ndjson", quarantine)
    _write_csv(output_dir / "gold" / "content_daily.csv", content_daily)
    _write_csv(output_dir / "gold" / "platform_hourly.csv", platform_hourly)

    summary = {
        "run_id": run_id,
        "input_records": len(bronze),
        "valid_records": len(valid_events),
        "duplicate_records": duplicate_records,
        "quarantined_records": len(quarantine),
        "gold_content_daily_rows": len(content_daily),
        "gold_platform_hourly_rows": len(platform_hourly),
        "quality_pass_rate": round(len(valid_events) / len(bronze), 4) if bronze else 0.0,
    }
    (output_dir / "run_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/sample/events.ndjson"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = run_pipeline(args.input, args.output_dir, args.run_id)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
