"""Parse, validate, quarantine, watermark, and deduplicate Bronze events."""

from __future__ import annotations

import os

from streaming_analytics.spark.common import build_spark_session, event_schema, quality_condition


def main() -> None:
    from pyspark.sql import functions as F

    spark = build_spark_session("streampulse-silver-quality")
    bronze_path = os.getenv("BRONZE_PATH", "output/delta/bronze_playback_events")
    silver_path = os.getenv("SILVER_PATH", "output/delta/silver_playback_events")
    quarantine_path = os.getenv("QUARANTINE_PATH", "output/delta/quarantine_playback_events")
    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "checkpoints")
    watermark_delay = os.getenv("WATERMARK_DELAY", "15 minutes")

    bronze = spark.readStream.format("delta").load(bronze_path)
    parsed = (
        bronze.withColumn("event", F.from_json("raw_payload", event_schema()))
        .select(
            "event.*",
            "topic",
            "partition",
            "offset",
            "broker_timestamp",
            "bronze_ingested_at",
            "raw_payload",
        )
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("source_ingestion_ts", F.to_timestamp("ingestion_ts"))
        .drop("ingestion_ts")
    )

    passes_quality = F.coalesce(quality_condition(), F.lit(False))

    valid = (
        parsed.filter(passes_quality)
        .withWatermark("event_ts", watermark_delay)
        .dropDuplicates(["event_id"])
        .withColumn("silver_processed_at", F.current_timestamp())
        .drop("raw_payload")
    )
    invalid = (
        parsed.filter(~passes_quality)
        .withColumn("quarantine_reason", F.lit("EVENT_CONTRACT_VIOLATION"))
        .withColumn("quarantined_at", F.current_timestamp())
    )

    valid_query = (
        valid.writeStream.format("delta")
        .queryName("silver_playback_events")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(checkpoint_root, "silver"))
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "15 seconds"))
        .start(silver_path)
    )
    invalid_query = (
        invalid.writeStream.format("delta")
        .queryName("quarantine_playback_events")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(checkpoint_root, "quarantine"))
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "15 seconds"))
        .start(quarantine_path)
    )
    spark.streams.awaitAnyTermination()
    valid_query.stop()
    invalid_query.stop()


if __name__ == "__main__":
    main()
