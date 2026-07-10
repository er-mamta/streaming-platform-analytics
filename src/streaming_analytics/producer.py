"""Replay NDJSON playback events into a Kafka-compatible topic."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/sample/events.ndjson"))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "playback-events"))
    parser.add_argument("--events-per-second", type=float, default=25.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.events_per_second <= 0:
        raise SystemExit("--events-per-second must be greater than 0")

    try:
        from kafka import KafkaProducer
    except ImportError as error:
        raise SystemExit("Install the streaming extra: pip install -e '.[stream]'") from error

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
        compression_type="gzip",
    )
    delay_seconds = 1.0 / args.events_per_second
    sent = 0
    with args.input.open("r", encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                continue
            event = json.loads(line)
            producer.send(args.topic, key=event.get("session_id", "unknown"), value=event)
            sent += 1
            time.sleep(delay_seconds)
    producer.flush(timeout=30)
    producer.close()
    print(f"Published {sent} events to {args.topic} via {args.bootstrap_servers}")


if __name__ == "__main__":
    main()

