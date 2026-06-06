"""
src/dlt/eeg_pipeline.py
========================
Day 7 – Delta Live Tables (DLT): declarative Bronze → Silver → Gold pipeline.

Exam domains covered:
  - @dlt.table and @dlt.view decorators
  - @dlt.expect / @dlt.expect_or_drop / @dlt.expect_or_fail data quality
  - pipeline_mode: triggered vs continuous
  - dlt.read() vs dlt.read_stream()
  - Medallion architecture within DLT

Research context:
  Implements the full EEG lakehouse pipeline as a DLT graph so that
  data quality violations (e.g. out-of-range amplitude values from
  PhysioNet Sleep-EDF Expanded) are quarantined rather than silently
  corrupting downstream TDA features.

Databricks exam tip:
  DLT pipelines run in their own isolated compute — you cannot import
  arbitrary libraries at the pipeline level unless they are installed on
  the cluster or as a wheel.  Know the difference between
  ``pipeline_mode=triggered`` (batch) and ``pipeline_mode=continuous``
  (low-latency streaming).
"""

from __future__ import annotations

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

# ---------------------------------------------------------------------------
# Configuration (injected via DLT pipeline parameters at runtime)
# ---------------------------------------------------------------------------

BRONZE_SOURCE_PATH = spark.conf.get("eeg.bronze.source_path", "dbfs:/mnt/landing/eeg/raw")
CATALOG = spark.conf.get("eeg.catalog", "eeg_catalog")
SCHEMA = spark.conf.get("eeg.schema", "silver")

# ---------------------------------------------------------------------------
# Bronze layer — raw ingestion with Auto Loader
# ---------------------------------------------------------------------------

RAW_SCHEMA = StructType([
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
])


@dlt.table(
    name="bronze_eeg_epochs",
    comment="Raw EEG epoch records ingested via Auto Loader from PhysioNet Sleep-EDF landing zone.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_eeg_epochs() -> DataFrame:
    """Ingest raw EEG Parquet files from cloud storage using Auto Loader.

    Returns:
        Streaming Bronze DataFrame with raw epoch records.

    Exam note:
        ``dlt.read_stream`` inside a DLT pipeline replaces
        ``spark.readStream`` — the pipeline runtime manages checkpoints.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{BRONZE_SOURCE_PATH}/_schema")
        .schema(RAW_SCHEMA)
        .load(BRONZE_SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


# ---------------------------------------------------------------------------
# Silver layer — validated and cleaned epochs
# ---------------------------------------------------------------------------

@dlt.table(
    name="silver_eeg_epochs",
    comment="Cleaned EEG epochs with amplitude and power band quality checks applied.",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_subject_id", "subject_id IS NOT NULL")
@dlt.expect_or_drop("amplitude_in_range", "mean_amplitude_uv BETWEEN -500 AND 500")
@dlt.expect_or_drop("positive_delta_power", "delta_power >= 0")
@dlt.expect_or_drop("valid_sleep_stage", "sleep_stage IN ('W', 'N1', 'N2', 'N3', 'R')")
def silver_eeg_epochs() -> DataFrame:
    """Validate and clean Bronze EEG epochs for downstream analysis.

    Quality rules:
        - subject_id must be present (warn on violation — pipeline continues)
        - amplitude must be within physiological range [-500, 500] µV (drop outliers)
        - delta power must be non-negative (drop negative values)
        - sleep stage must be a recognised PSG annotation (drop unknowns)

    Returns:
        Streaming Silver DataFrame with invalid records quarantined.

    Exam note:
        ``@dlt.expect`` = warn + track.  ``@dlt.expect_or_drop`` = remove row.
        ``@dlt.expect_or_fail`` = halt pipeline.  Metrics visible in DLT UI.
    """
    return (
        dlt.read_stream("bronze_eeg_epochs")
        .withColumn(
            "delta_theta_ratio",
            F.when(F.col("theta_power") > 0, F.col("delta_power") / F.col("theta_power"))
            .otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn(
            "total_spectral_power",
            F.col("delta_power") + F.col("theta_power") + F.col("alpha_power"),
        )
        .withColumn("_cleaned_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Gold layer — aggregated features for TDA and ML
# ---------------------------------------------------------------------------

@dlt.table(
    name="gold_eeg_subject_features",
    comment="Per-subject aggregate EEG features for TDA memory-consolidation classifier.",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def gold_eeg_subject_features() -> DataFrame:
    """Aggregate Silver epochs into per-subject feature vectors.

    Each row represents one subject's sleep session, summarising spectral
    power bands, spindle statistics, and sleep stage composition — inputs
    to the XGBoost + SHAP memory proxy prediction model (Day 10).

    Returns:
        Static Gold DataFrame (one row per subject).

    Exam note:
        Gold DLT tables use ``dlt.read`` (not ``dlt.read_stream``) because
        they perform full aggregations that require batch semantics.
    """
    return (
        dlt.read("silver_eeg_epochs")
        .groupBy("subject_id")
        .agg(
            F.mean("delta_power").alias("mean_delta_power"),
            F.mean("theta_power").alias("mean_theta_power"),
            F.mean("alpha_power").alias("mean_alpha_power"),
            F.mean("delta_theta_ratio").alias("mean_delta_theta_ratio"),
            F.sum("spindle_count").alias("total_spindle_count"),
            F.mean("spindle_count").alias("mean_spindle_density"),
            F.count("epoch_id").alias("total_epochs"),
            F.sum(F.when(F.col("sleep_stage") == "N3", 1).otherwise(0)).alias("n3_epochs"),
            F.sum(F.when(F.col("sleep_stage") == "R", 1).otherwise(0)).alias("rem_epochs"),
            F.max("_cleaned_at").alias("last_updated"),
        )
        .withColumn(
            "sws_ratio",  # slow-wave sleep ratio — proxy for memory consolidation
            F.col("n3_epochs") / F.col("total_epochs"),
        )
    )


# ---------------------------------------------------------------------------
# DLT View — real-time monitoring (not persisted)
# ---------------------------------------------------------------------------

@dlt.view(
    name="v_nrem_epochs",
    comment="Live view of NREM (N2+N3) epochs for streaming quality monitoring dashboard.",
)
def v_nrem_epochs() -> DataFrame:
    """Filter Silver epochs to NREM stages for operational monitoring.

    Returns:
        Streaming view of N2 and N3 epochs only.

    Exam note:
        DLT views are NOT materialised — they are re-evaluated on every
        downstream read.  Use tables for performance-critical paths.
    """
    return dlt.read_stream("silver_eeg_epochs").filter(
        F.col("sleep_stage").isin("N2", "N3")
    )
