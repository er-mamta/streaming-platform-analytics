"""Continuously upsert hourly content and QoE metrics into a Gold Delta table."""

from __future__ import annotations

import os

from streaming_analytics.spark.common import build_spark_session


def main() -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    spark = build_spark_session("streampulse-gold-content-hourly")
    silver_path = os.getenv("SILVER_PATH", "output/delta/silver_playback_events")
    gold_path = os.getenv("GOLD_PATH", "output/delta/gold_content_hourly")
    checkpoint = os.path.join(os.getenv("CHECKPOINT_ROOT", "checkpoints"), "gold")
    watermark_delay = os.getenv("WATERMARK_DELAY", "15 minutes")

    silver = spark.readStream.format("delta").load(silver_path)
    aggregates = (
        silver.withWatermark("event_ts", watermark_delay)
        .groupBy(
            F.window("event_ts", "1 hour"),
            "content_id",
            "content_title",
            "country",
            "subscription_tier",
            "device_type",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum(
                F.when(F.col("event_type") == "playback_started", 1).otherwise(0)
            ).alias("plays"),
            F.approx_count_distinct("user_id").alias("unique_viewers"),
            F.sum(
                F.when(F.col("event_type") == "playback_completed", 1).otherwise(0)
            ).alias("completes"),
            F.sum(
                F.when(F.col("event_type") == "playback_error", 1).otherwise(0)
            ).alias("playback_errors"),
            (
                F.sum(F.coalesce("watch_seconds", F.lit(0.0))) / F.lit(3_600.0)
            ).alias("watch_hours"),
            F.avg("startup_ms").alias("avg_startup_ms"),
            F.sum(F.coalesce("rebuffer_seconds", F.lit(0.0))).alias("rebuffer_seconds"),
            F.sum(F.coalesce("watch_seconds", F.lit(0.0))).alias("watch_seconds"),
        )
        .withColumn(
            "completion_rate",
            F.when(F.col("plays") > 0, F.col("completes") / F.col("plays")).otherwise(0.0),
        )
        .withColumn(
            "error_rate",
            F.when(
                F.col("plays") > 0, F.col("playback_errors") / F.col("plays")
            ).otherwise(0.0),
        )
        .withColumn(
            "rebuffer_ratio",
            F.when(
                F.col("watch_seconds") > 0,
                F.col("rebuffer_seconds") / F.col("watch_seconds"),
            ).otherwise(0.0),
        )
        .withColumn(
            "qoe_score",
            F.greatest(
                F.lit(0.0),
                F.lit(100.0)
                - F.least(F.coalesce("avg_startup_ms", F.lit(0.0)) / 100.0, F.lit(30.0))
                - F.least(F.col("rebuffer_ratio") * 500.0, F.lit(40.0))
                - F.least(F.col("error_rate") * 100.0, F.lit(30.0)),
            ),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "content_id",
            "content_title",
            "country",
            "subscription_tier",
            "device_type",
            "event_count",
            "plays",
            "unique_viewers",
            "completes",
            "playback_errors",
            "watch_hours",
            "completion_rate",
            "error_rate",
            "rebuffer_ratio",
            "avg_startup_ms",
            "qoe_score",
        )
    )

    merge_condition = " AND ".join(
        [
            "target.window_start = updates.window_start",
            "target.content_id = updates.content_id",
            "target.country = updates.country",
            "target.subscription_tier = updates.subscription_tier",
            "target.device_type = updates.device_type",
        ]
    )

    def upsert_gold(batch_df, batch_id: int) -> None:
        enriched = batch_df.withColumn("last_updated_at", F.current_timestamp()).withColumn(
            "source_batch_id", F.lit(batch_id)
        )
        if DeltaTable.isDeltaTable(spark, gold_path):
            (
                DeltaTable.forPath(spark, gold_path)
                .alias("target")
                .merge(enriched.alias("updates"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            enriched.write.format("delta").mode("overwrite").save(gold_path)

    query = (
        aggregates.writeStream.queryName("gold_content_hourly")
        .outputMode("update")
        .foreachBatch(upsert_gold)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=os.getenv("TRIGGER_INTERVAL", "15 seconds"))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
