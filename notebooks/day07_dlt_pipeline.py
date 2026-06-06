# Databricks notebook source
# notebooks/day07_dlt_pipeline.py
# =============================================================================
# DAY 7 - Delta Live Tables: Production Pipeline Skeleton
# =============================================================================
# EXAM DOMAINS: DLT decorators, Expectations, pipeline modes, event log
# RESEARCH: Full Bronze->Silver->Gold DLT pipeline for EEG analysis
# NOTE: This file runs as a DLT pipeline notebook, NOT as a regular notebook.
#       Attach it to a DLT pipeline in Databricks UI or databricks.yml.
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ---------------------------------------------------------------------------
# BRONZE LAYER: Raw EDF ingestion via Auto Loader (cloudFiles)
# ---------------------------------------------------------------------------

@dlt.table(
    name="bronze_eeg_raw",
    comment="Raw EEG files ingested from ADLS Volume via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_eeg_raw():
    """
    EXAM NOTE: cloudFiles format enables Auto Loader.
    - Tracks processed files in _checkpoint directory
    - Scales to millions of files without listing overhead
    - rescuedDataColumn captures schema drift into _rescued_data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", "/Volumes/eeg_lakehouse/bronze/_schema")
        .option("recursiveFileLookup", "true")
        .load("/Volumes/eeg_lakehouse/bronze/raw_edf/")
        .select(
            F.col("path"),
            F.col("length").alias("file_size_bytes"),
            F.col("modificationTime").alias("ingestion_ts"),
            # Extract subject_id from path pattern: SC4001E0-PSG.edf
            F.regexp_extract(F.col("path"), r"/(SC\d+)", 1).alias("subject_id"),
            F.lit("night1").alias("session"),  # production: parse from filename
            F.current_date().alias("recording_date"),
        )
    )


# ---------------------------------------------------------------------------
# SILVER LAYER: Validated + preprocessed epochs
# ---------------------------------------------------------------------------

@dlt.table(
    name="silver_eeg_preprocessed",
    comment="Validated EEG epochs after bandpass filtering and epoch segmentation",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_subject", "subject_id IS NOT NULL AND subject_id != ''")
@dlt.expect_or_drop("valid_file_size", "file_size_bytes > 1000")
@dlt.expect_or_fail("known_session", "session IN ('night1', 'night2', 'nap', 'baseline')")
def silver_eeg_preprocessed():
    """
    EXAM NOTE:
    - @dlt.expect       -> keep rows, log warning metric
    - @dlt.expect_or_drop -> silently drop invalid rows
    - @dlt.expect_or_fail -> fail entire pipeline run
    Apply preprocessing logic: in production uses Pandas UDF with MNE.
    """
    return (
        dlt.read_stream("bronze_eeg_raw")
        .withColumn("sigma_power",  F.lit(0.35).cast(DoubleType()))  # mock
        .withColumn("delta_power",  F.lit(0.52).cast(DoubleType()))  # mock
        .withColumn("beta_power",   F.lit(0.10).cast(DoubleType()))  # mock
        .withColumn("theta_power",  F.lit(0.18).cast(DoubleType()))  # mock
        .withColumn("epoch_index",  F.lit(0).cast(IntegerType()))    # mock
        .withColumn("quality_flag", F.lit("ok"))
    )


@dlt.table(
    name="silver_eeg_events",
    comment="Detected sleep events: spindles and slow oscillations",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_event_type", "event_type IN ('spindle', 'slow_oscillation')")
@dlt.expect_or_drop("valid_duration", "duration_s BETWEEN 0.3 AND 3.0")
def silver_eeg_events():
    """
    Event detection table - populated by YASA/MNE Pandas UDF in production.
    DLT reads from silver_eeg_preprocessed (not raw Bronze) for lineage.
    """
    # Mock event generation - replace with real YASA-based Pandas UDF
    return (
        dlt.read("silver_eeg_preprocessed")
        .withColumn("event_type",   F.lit("spindle"))
        .withColumn("duration_s",   F.lit(0.8).cast(DoubleType()))
        .withColumn("amplitude_uv", F.lit(45.0).cast(DoubleType()))
        .withColumn("neg_peak_uv",  F.lit(None).cast(DoubleType()))
        .withColumn("channel",      F.lit("Fz"))
    )


# ---------------------------------------------------------------------------
# GOLD LAYER: Aggregated features
# ---------------------------------------------------------------------------

@dlt.table(
    name="gold_eeg_features",
    comment="Wide feature table: one row per subject/session, ML-ready",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
@dlt.expect_or_fail("valid_subject_gold", "subject_id IS NOT NULL")
def gold_eeg_features():
    """
    Aggregate Silver events + Silver power features into one row per session.
    EXAM NOTE: Use dlt.read() (not read_stream) from materialized Silver tables.
    Auto Optimize properties: optimizeWrite=true writes larger files automatically.
    """
    events = dlt.read("silver_eeg_events")
    preprocessed = dlt.read("silver_eeg_preprocessed")

    spindle_agg = (
        events.filter(F.col("event_type") == "spindle")
        .groupBy("subject_id", "session")
        .agg(
            F.count("*").alias("spindle_count"),
            F.mean("duration_s").alias("spindle_duration_mean"),
            F.mean("amplitude_uv").alias("spindle_amplitude_mean"),
        )
    )

    power_agg = (
        preprocessed
        .groupBy("subject_id", "session", "recording_date")
        .agg(
            F.mean("sigma_power").alias("sigma_power_mean"),
            F.mean("delta_power").alias("delta_power_mean"),
        )
    )

    return power_agg.join(spindle_agg, ["subject_id", "session"], "left")


# ---------------------------------------------------------------------------
# DLT VIEW EXAMPLE (not persisted, only for pipeline-internal use)
# ---------------------------------------------------------------------------

@dlt.view(name="v_high_spindle_density")
def high_spindle_view():
    """
    EXAM NOTE: DLT views are NOT materialized to Delta.
    Used only within the same pipeline for intermediate transforms.
    Cannot be queried outside the pipeline.
    """
    return (
        dlt.read("gold_eeg_features")
        .filter(F.col("spindle_count") > 10)
    )


# ---------------------------------------------------------------------------
# EXAM STUDY NOTES (as comments for review)
# ---------------------------------------------------------------------------
# Q: When does DLT update a streaming table vs a materialized view?
# A: Streaming tables process new data incrementally (append-only source).
#    Materialized views recompute from scratch or incrementally depending
#    on the operation type.
#
# Q: Where are DLT Expectation metrics stored?
# A: In the pipeline event log table, queryable via event_log() function.
#
# Q: What does table_properties delta.autoOptimize.optimizeWrite do?
# A: Automatically increases file sizes during writes to reduce small files.
#    Equivalent to running OPTIMIZE after every write.
#
# Q: How do you pass configuration to a DLT pipeline?
# A: Via spark.conf.get() inside the notebook, set in pipeline config JSON/YAML.
#    Example: sampling_rate = int(spark.conf.get("eeg.sampling_rate", "256"))
