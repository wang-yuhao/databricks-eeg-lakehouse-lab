"""
src/streaming/eeg_stream_processor.py
======================================
Day 9 – Structured Streaming: real-time EEG epoch ingestion pipeline.

Exam domains covered:
  - Structured Streaming triggers (ProcessingTime, AvailableNow, Once)
  - Watermarks for late-data handling
  - ForeachBatch sink for Delta writes
  - Output modes: append / update / complete

Research context:
  Simulates a real-time EEG data feed where new 30-second epochs arrive
  continuously from a physiological monitor.  Watermark of 5 minutes
  tolerates annotation delays common in sleep-staging systems.

Databricks exam tip:
  ``trigger(availableNow=True)`` is the modern replacement for
  ``trigger(once=True)`` — know both for the 2026 exam.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

EEG_EPOCH_SCHEMA = StructType(
    [
        StructField("subject_id", StringType(), nullable=False),
        StructField("epoch_id", IntegerType(), nullable=False),
        StructField("epoch_start_ts", TimestampType(), nullable=False),
        StructField("channel", StringType(), nullable=True),
        StructField("mean_amplitude_uv", DoubleType(), nullable=True),
        StructField("delta_power", DoubleType(), nullable=True),
        StructField("theta_power", DoubleType(), nullable=True),
        StructField("alpha_power", DoubleType(), nullable=True),
        StructField("spindle_count", IntegerType(), nullable=True),
        StructField("sleep_stage", StringType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Stream reader
# ---------------------------------------------------------------------------


def read_eeg_stream(spark: SparkSession, source_path: str) -> DataFrame:
    """Read a streaming Delta source containing raw EEG epochs.

    Args:
        spark: Active SparkSession.
        source_path: Delta table path or Auto Loader cloud path.

    Returns:
        Streaming DataFrame with ``epoch_start_ts`` as event-time column.

    Exam note:
        ``cloudFiles`` format triggers schema inference + evolution without
        manual ``DESCRIBE`` calls — covered in Auto Loader exam section.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{source_path}/_schema")
        .schema(EEG_EPOCH_SCHEMA)
        .load(source_path)
        .withWatermark("epoch_start_ts", "5 minutes")
    )


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------


def compute_band_ratios(df: DataFrame) -> DataFrame:
    """Derive delta/theta ratio and total spectral power per epoch.

    These features are downstream inputs to the TDA memory-consolidation
    classifier (Gold layer).  Computing them in the streaming layer avoids
    re-scanning Silver tables.

    Args:
        df: Streaming DataFrame with power band columns.

    Returns:
        DataFrame enriched with ``delta_theta_ratio`` and ``total_power``.
    """
    return df.withColumn(
        "delta_theta_ratio",
        F.when(F.col("theta_power") > 0, F.col("delta_power") / F.col("theta_power")).otherwise(
            F.lit(None).cast(DoubleType())
        ),
    ).withColumn(
        "total_power",
        F.col("delta_power") + F.col("theta_power") + F.col("alpha_power"),
    )


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Stamp each micro-batch with processing timestamp and pipeline version.

    Args:
        df: Streaming DataFrame.

    Returns:
        DataFrame with ``processed_at`` and ``pipeline_version`` columns.
    """
    return df.withColumn("processed_at", F.current_timestamp()).withColumn(
        "pipeline_version", F.lit("1.0.0")
    )


# ---------------------------------------------------------------------------
# ForeachBatch sink
# ---------------------------------------------------------------------------


def write_to_silver_foreach_batch(
    batch_df: DataFrame,
    batch_id: int,
    silver_table_path: str,
) -> None:
    """ForeachBatch writer: upsert each micro-batch into Silver Delta table.

    Using MERGE (upsert) ensures exactly-once semantics when the stream is
    restarted — duplicate epochs are silently skipped.

    Args:
        batch_df: Static DataFrame for the current micro-batch.
        batch_id: Monotonically increasing batch identifier.
        silver_table_path: Target Delta table path.

    Exam note:
        ForeachBatch is the recommended pattern for non-append sinks
        (e.g. MERGE, JDBC, external APIs) in Structured Streaming.
    """
    from delta.tables import DeltaTable  # imported here to keep module testable without Delta

    spark = batch_df.sparkSession

    if DeltaTable.isDeltaTable(spark, silver_table_path):
        delta_table = DeltaTable.forPath(spark, silver_table_path)
        (
            delta_table.alias("target")
            .merge(
                batch_df.alias("source"),
                "target.subject_id = source.subject_id AND target.epoch_id = source.epoch_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch_df.write.format("delta").mode("overwrite").save(silver_table_path)


# ---------------------------------------------------------------------------
# Main stream runner
# ---------------------------------------------------------------------------


def run_eeg_stream(
    spark: SparkSession,
    source_path: str,
    silver_table_path: str,
    checkpoint_path: str,
    trigger_interval: str = "30 seconds",
) -> None:
    """Launch the EEG streaming pipeline.

    Args:
        spark: Active SparkSession.
        source_path: Auto Loader source directory (new Parquet files arrive here).
        silver_table_path: Destination Delta table path in Silver layer.
        checkpoint_path: Checkpoint location for stream recovery.
        trigger_interval: Spark trigger interval string.
            Use ``"availableNow"`` for one-shot batch mode (replaces ``once=True``).

    Example:
        >>> run_eeg_stream(
        ...     spark,
        ...     source_path="dbfs:/mnt/landing/eeg/raw",
        ...     silver_table_path="dbfs:/mnt/silver/eeg_epochs",
        ...     checkpoint_path="dbfs:/checkpoints/eeg_stream",
        ...     trigger_interval="availableNow",
        ... )
    """
    raw_stream = read_eeg_stream(spark, source_path)
    enriched = add_processing_metadata(compute_band_ratios(raw_stream))

    query = (
        enriched.writeStream.format("delta")
        .foreachBatch(
            lambda batch_df, batch_id: write_to_silver_foreach_batch(
                batch_df, batch_id, silver_table_path
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()
