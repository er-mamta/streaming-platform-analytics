"""Shared Spark configuration and playback-event schema."""

from __future__ import annotations

import os


def build_spark_session(app_name: str):
    """Create a Spark session configured for Delta Lake and Kafka."""

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    )
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    return configure_spark_with_delta_pip(builder, extra_packages=[kafka_package]).getOrCreate()


def event_schema():
    """Return the explicit Spark schema; inference is intentionally avoided."""

    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("event_type", T.StringType(), False),
            T.StructField("event_ts", T.StringType(), False),
            T.StructField("ingestion_ts", T.StringType(), False),
            T.StructField("user_id", T.StringType(), False),
            T.StructField("profile_id", T.StringType(), False),
            T.StructField("session_id", T.StringType(), False),
            T.StructField("content_id", T.StringType(), False),
            T.StructField("content_title", T.StringType(), True),
            T.StructField("device_type", T.StringType(), False),
            T.StructField("country", T.StringType(), False),
            T.StructField("subscription_tier", T.StringType(), False),
            T.StructField("position_seconds", T.DoubleType(), True),
            T.StructField("watch_seconds", T.DoubleType(), True),
            T.StructField("startup_ms", T.DoubleType(), True),
            T.StructField("bitrate_kbps", T.DoubleType(), True),
            T.StructField("rebuffer_seconds", T.DoubleType(), True),
            T.StructField("error_code", T.StringType(), True),
            T.StructField("schema_version", T.IntegerType(), False),
        ]
    )


def quality_condition():
    """Return the Silver quality predicate as a reusable Column expression."""

    from pyspark.sql import functions as F

    return (
        F.col("event_id").isNotNull()
        & F.col("event_type").isin(
            "playback_started",
            "playback_progress",
            "playback_completed",
            "playback_error",
        )
        & F.col("event_ts").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("session_id").isNotNull()
        & F.col("content_id").isNotNull()
        & F.col("device_type").isin("connected_tv", "mobile", "tablet", "web")
        & F.col("subscription_tier").isin("ad_supported", "standard", "premium")
        & (F.coalesce(F.col("watch_seconds"), F.lit(0.0)) >= 0)
        & (F.coalesce(F.col("rebuffer_seconds"), F.lit(0.0)) >= 0)
        & (
            F.coalesce(F.col("rebuffer_seconds"), F.lit(0.0))
            <= F.coalesce(F.col("watch_seconds"), F.lit(0.0))
        )
        & (F.col("schema_version") == 1)
    )
