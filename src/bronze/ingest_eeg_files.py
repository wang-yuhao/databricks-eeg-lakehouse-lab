"""Bronze layer: Auto Loader ingestion of Sleep EDF files.

This module implements the primary Bronze ingestion pattern for the EEG pipeline.
It uses Databricks Auto Loader (format "cloudFiles") to incrementally ingest
new EDF files from a Unity Catalog Volume into a Delta table.

Databricks Exam Relevance (Domain 3 - Incremental Data Processing):
- Auto Loader tracks new files via cloud object notifications (SNS/Event Grid)
  or directory listing, storing progress in a checkpoint directory.
- `cloudFiles.schemaLocation` stores inferred schema to handle new files
  without re-scanning all historical data.
- `mergeSchema` allows Bronze to absorb new metadata columns without failing.
- This is idempotent: re-running never re-ingests already-processed files.

Research Relevance:
- 197 subjects × ~2 nights = ~394 EDF files; Auto Loader handles the full corpus
  incrementally as new files are added (e.g., validation dataset CAP).
- Bronze stores file-level metadata (subject_id, session, path) — NOT raw signal arrays.
  Raw EDF binaries remain in the UC Volume; Silver UDFs read them via path.
"""

from __future__ import annotations

import re
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, TimestampType, BooleanType
)

from src.utils.config import AppConfig, DEFAULT_CONFIG
from src.utils.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Bronze EEG file registry schema
# ---------------------------------------------------------------------------

BRONZE_EEG_SCHEMA = StructType([
    StructField("file_path",        StringType(),    nullable=False),
    StructField("file_name",        StringType(),    nullable=False),
    StructField("file_size_bytes",  LongType(),      nullable=True),
    StructField("modification_time",TimestampType(), nullable=True),
    # Parsed from filename (e.g. SC4001E0-PSG.edf)
    StructField("subject_id",       StringType(),    nullable=False),
    StructField("study_type",       StringType(),    nullable=True),  # SC or ST
    StructField("session_id",       StringType(),    nullable=True),  # E0, E1
    StructField("is_hypnogram",     BooleanType(),   nullable=False),
    StructField("ingestion_date",   TimestampType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Filename parsing UDF
# ---------------------------------------------------------------------------

# Regex pattern for Sleep-EDF filenames:
# SC4001E0-PSG.edf  → groups: study_type=SC, subject_num=4001, session=E0, file_type=PSG
# SC4001EC-Hypnogram.edf → file_type=Hypnogram
_SLEEPDF_PATTERN = re.compile(
    r"^(SC|ST)(\d{4})(E\d|EC)-(PSG|Hypnogram)\.edf$",
    re.IGNORECASE
)


@F.udf(returnType=StructType([
    StructField("subject_id",  StringType(), nullable=True),
    StructField("study_type",  StringType(), nullable=True),
    StructField("session_id",  StringType(), nullable=True),
    StructField("is_hypnogram",BooleanType(),nullable=True),
]))
def _parse_edf_filename(file_name: str):
    """Parse Sleep-EDF filename into structured metadata fields.

    Exam note: Python UDFs serialize row-by-row (slower than Pandas UDFs).
    This is acceptable here because we only call it once during Bronze ingestion
    (few hundred files), not on millions of signal rows.

    Args:
        file_name: Raw filename string, e.g. 'SC4001E0-PSG.edf'

    Returns:
        StructType with subject_id, study_type, session_id, is_hypnogram
    """
    if file_name is None:
        return (None, None, None, None)
    m = _SLEEPDF_PATTERN.match(file_name)
    if not m:
        return (None, None, None, None)
    study_type, subject_num, session_id, file_type = m.groups()
    subject_id = f"{study_type}{subject_num}"  # e.g. "SC4001"
    is_hypnogram = (file_type.lower() == "hypnogram")
    return (subject_id, study_type.upper(), session_id.upper(), is_hypnogram)


# ---------------------------------------------------------------------------
# Core ingestion functions
# ---------------------------------------------------------------------------

def load_raw_files(
    spark: SparkSession,
    source_path: str,
    cfg: AppConfig = DEFAULT_CONFIG,
) -> DataFrame:
    """Load EDF file metadata from a directory using Auto Loader (streaming).

    This returns a STREAMING DataFrame. Use `.writeStream` to sink it,
    or `.display()` / `.show()` is not supported on streaming DataFrames directly.

    Auto Loader exam key points:
    - `cloudFiles.format`: inner format of the files (here: `binaryFile` for EDF)
    - `cloudFiles.schemaLocation`: persists inferred schema to avoid re-scanning
    - `cloudFiles.inferColumnTypes`: infer types from file metadata columns
    - `recursiveFileLookup`: traverse subdirectories

    Args:
        spark: Active SparkSession.
        source_path: Path to directory containing EDF files
                     (UC Volume path on Databricks, local path in tests).
        cfg: Application configuration.

    Returns:
        Streaming DataFrame with Auto Loader file metadata columns:
        path, name, length, modificationTime, content (binary).
    """
    log.info(f"Loading raw EDF files from: {source_path}")

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")  # EDF files are binary
        .option("cloudFiles.schemaLocation", cfg.paths.autoloader_schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.edf")           # Only EDF files
        .load(source_path)
    )


def load_raw_files_batch(
    spark: SparkSession,
    source_path: str,
) -> DataFrame:
    """Batch (non-streaming) version of load_raw_files for local testing and notebooks.

    Exam note: COPY INTO is the batch equivalent of Auto Loader for one-time loads.
    Auto Loader is preferred for continuous/incremental scenarios.
    Use this function when Auto Loader is not available (local dev, unit tests).

    Args:
        spark: Active SparkSession.
        source_path: Path to directory containing EDF files.

    Returns:
        Batch DataFrame with file metadata columns.
    """
    log.info(f"[BATCH] Loading EDF file metadata from: {source_path}")
    return (
        spark.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.edf")
        .load(source_path)
    )


def transform_file_metadata(df_raw: DataFrame) -> DataFrame:
    """Transform raw Auto Loader output into the Bronze EEG file registry schema.

    Steps:
    1. Extract filename from path.
    2. Apply `_parse_edf_filename` UDF to get subject_id, study_type, etc.
    3. Add ingestion timestamp.
    4. Select final Bronze schema columns.

    Args:
        df_raw: Raw DataFrame from Auto Loader (streaming or batch).

    Returns:
        Transformed DataFrame matching BRONZE_EEG_SCHEMA.
    """
    # Extract file name from full path (works for both local and cloud paths)
    df = df_raw.withColumn(
        "file_name", F.element_at(F.split(F.col("path"), "/"), -1)
    )

    # Parse filename into structured fields
    df = df.withColumn("parsed", _parse_edf_filename(F.col("file_name")))

    # Flatten the struct columns
    df = (
        df
        .withColumn("subject_id",   F.col("parsed.subject_id"))
        .withColumn("study_type",   F.col("parsed.study_type"))
        .withColumn("session_id",   F.col("parsed.session_id"))
        .withColumn("is_hypnogram", F.col("parsed.is_hypnogram"))
        .withColumn("ingestion_date", F.current_timestamp())
    )

    # Rename Auto Loader columns to match Bronze schema
    return df.select(
        F.col("path").alias("file_path"),
        F.col("file_name"),
        F.col("length").alias("file_size_bytes"),
        F.col("modificationTime").alias("modification_time"),
        F.col("subject_id"),
        F.col("study_type"),
        F.col("session_id"),
        F.col("is_hypnogram"),
        F.col("ingestion_date"),
    )


def create_bronze_table(
    spark: SparkSession,
    source_path: str,
    cfg: AppConfig = DEFAULT_CONFIG,
    trigger_once: bool = True,
) -> None:
    """Create or update the Bronze EEG file registry Delta table using Auto Loader.

    This is the main entry point for Bronze ingestion. It writes a streaming query
    that:
    - Reads new EDF files from source_path via Auto Loader
    - Transforms metadata via transform_file_metadata()
    - Writes incrementally to the Bronze Delta table
    - Stores checkpoint so re-runs are idempotent

    Exam note on trigger types:
    - `availableNow` (Databricks recommended): processes all available data then stops.
      Equivalent to `once=True` but more efficient (multiple micro-batches).
    - `once=True`: one micro-batch, then stops. Legacy.
    - `processingTime='30 seconds'`: continuous micro-batch mode.
    - `continuous='1 second'`: low-latency continuous mode (experimental).

    Args:
        spark: Active SparkSession.
        source_path: Source directory with EDF files.
        cfg: Application configuration.
        trigger_once: If True, use availableNow trigger (batch-style). 
                      If False, run continuously.
    """
    target_table = cfg.catalog.bronze_edf_fqn
    checkpoint = cfg.paths.autoloader_checkpoint

    log.info(f"Starting Bronze ingestion: {source_path} → {target_table}")

    df_raw = load_raw_files(spark, source_path, cfg)
    df_bronze = transform_file_metadata(df_raw)

    writer = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")           # Only new files appended
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")  # Allow schema evolution
    )

    if trigger_once:
        # availableNow: process all pending files, then stop
        # Exam: preferred trigger for scheduled jobs (cost-efficient)
        query = writer.trigger(availableNow=True).toTable(target_table)
    else:
        # Continuous: keep running for real-time ingestion
        query = writer.trigger(processingTime="30 seconds").toTable(target_table)

    query.awaitTermination()
    log.info(f"Bronze ingestion complete. Table: {target_table}")
