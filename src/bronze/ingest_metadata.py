"""Bronze layer: Ingest subject metadata CSV into Delta table.

The Sleep-EDF dataset includes a companion CSV with subject demographics
(age, sex, medication, recording date). This module ingests that CSV into
the Bronze subject_metadata Delta table.

Exam Relevance:
- Demonstrates `spark.read.csv` with explicit schema vs schema inference.
- Shows how to write a batch DataFrame to a Delta table with `saveAsTable()`.
- `mode='overwrite'` with `overwriteSchema=True` is useful for full refreshes.
- Contrast with Auto Loader (streaming/incremental) in ingest_eeg_files.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DateType
)

from src.utils.config import AppConfig, DEFAULT_CONFIG
from src.utils.logging import get_logger

log = get_logger(__name__)


# Explicit schema for Sleep-EDF subject metadata CSV
# Exam note: Always prefer explicit schema over inferSchema in production.
# inferSchema requires a full pass over the data and may choose wrong types.
SUBJECT_METADATA_SCHEMA = StructType([
    StructField("subject_id",       StringType(),  nullable=False),  # e.g. SC4001
    StructField("age",              IntegerType(), nullable=True),
    StructField("sex",              StringType(),  nullable=True),   # M or F
    StructField("study_type",       StringType(),  nullable=True),   # SC or ST
    StructField("medication",       StringType(),  nullable=True),   # temazepam or placebo
    StructField("recording_date",   DateType(),    nullable=True),
    StructField("lights_off_time",  StringType(),  nullable=True),   # HH:MM format
    StructField("lights_on_time",   StringType(),  nullable=True),
    StructField("total_sleep_time_min", FloatType(), nullable=True),
    StructField("sleep_efficiency_pct", FloatType(), nullable=True),
])


def load_subject_metadata(
    spark: SparkSession,
    csv_path: str,
) -> DataFrame:
    """Load subject metadata from CSV with explicit schema.

    Args:
        spark: Active SparkSession.
        csv_path: Path to the Sleep-EDF metadata CSV file.

    Returns:
        DataFrame matching SUBJECT_METADATA_SCHEMA.
    """
    log.info(f"Loading subject metadata from: {csv_path}")
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("dateFormat", "dd.MM.yyyy")
        .option("nullValue", "NA")
        .schema(SUBJECT_METADATA_SCHEMA)
        .load(csv_path)
    )


def write_metadata_bronze(
    df: DataFrame,
    cfg: AppConfig = DEFAULT_CONFIG,
    mode: str = "overwrite",
) -> None:
    """Write subject metadata DataFrame to Bronze Delta table.

    Args:
        df: Subject metadata DataFrame.
        cfg: Application configuration.
        mode: 'overwrite' for full refresh, 'append' for incremental.
    """
    target = cfg.catalog.bronze_metadata_fqn
    log.info(f"Writing metadata to Bronze: {target} (mode={mode})")

    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")  # Safe for Bronze schema evolution
        .saveAsTable(target)
    )
    log.info(f"Metadata write complete: {df.count()} rows → {target}")
