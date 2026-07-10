"""Ingest Kafka playback events into an immutable Bronze Delta table."""

from __future__ import annotations

import os

from streaming_analytics.spark.common import build_spark_session


def main() -> None:
    from pyspark.sql import functions as F

    spark = build_spark_session("streampulse-bronze-ingestion")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_TOPIC", "playback-events")
    bronze_path = os.getenv("BRONZE_PATH", "output/delta/bronze_playback_events")
    checkpoint = os.path.join(os.getenv("CHECKPOINT_ROOT", "checkpoints"), "bronze")

    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )
    bronze = kafka_events.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_payload"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("broker_timestamp"),
        F.current_timestamp().alias("bronze_ingested_at"),
    )

    query = (
        bronze.writeStream.format("delta")
        .queryName("bronze_playback_events")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "15 seconds"))
        .start(bronze_path)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

