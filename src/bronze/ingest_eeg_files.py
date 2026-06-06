"""Bronze layer: EEG EDF file ingestion using Databricks Auto Loader.

This module implements the primary ingestion path for raw EDF files from the
PhysioNet Sleep-EDF Expanded dataset into a Bronze Delta table.

Exam domains covered:
- Auto Loader (cloudFiles): schema inference, checkpointing, schema evolution
- Delta Lake: Bronze table creation, ACID writes, schema enforcement
- Binary file ingestion: binaryFile format, metadata extraction

Research relevance:
- Idempotent ingestion across 197 subjects × 2 nights = ~394 EDF files
- Checkpoint ensures re-runs skip already-processed files

Usage::

    from src.bronze.ingest_eeg_files import create_bronze_edf_table
    create_bronze_edf_table(spark, cfg)
"""

import re
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from src.utils.config import AppConfig
from src.utils.logging import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Bronze schema definitions
# ---------------------------------------------------------------------------

# Schema for the raw EDF file registry (Bronze)
# Note: _metadata is a special Auto Loader column injected automatically
BRONZE_EDF_SCHEMA = StructType([
    StructField("subject_id",     StringType(),    nullable=False),
    StructField("night_index",    IntegerType(),   nullable=True),
    StructField("file_type",      StringType(),    nullable=False),  # PSG | Hypnogram
    StructField("file_path",      StringType(),    nullable=False),
    StructField("file_name",      StringType(),    nullable=False),
    StructField("file_size_bytes", LongType(),     nullable=True),
    StructField("dataset_source", StringType(),    nullable=True),   # sleep-edf | cap
    StructField("ingestion_ts",   TimestampType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Helper: parse subject_id and night_index from EDF file name
# ---------------------------------------------------------------------------

def parse_edf_filename(file_name: str) -> dict:
    """Extract subject_id, night_index, and file_type from an EDF file name.

    Sleep-EDF Expanded naming convention:
    - PSG:       SC4001E0-PSG.edf   → subject_id='SC4001', night_index=0
    - Hypnogram: SC4001EC-Hypnogram.edf → subject_id='SC4001'

    Args:
        file_name: Base file name, e.g. 'SC4001E0-PSG.edf'

    Returns:
        Dict with keys: subject_id, night_index, file_type

    Example::

        parse_edf_filename("SC4001E0-PSG.edf")
        # {'subject_id': 'SC4001', 'night_index': 0, 'file_type': 'PSG'}
    """
    result = {"subject_id": None, "night_index": None, "file_type": "unknown"}

    # Sleep-EDF cassette/telemetry pattern
    # SC/ST + 4-digit subject + E + night_digit + optional_char + '-' + type + '.edf'
    psg_match = re.match(
        r"([A-Z]{2}\d{4})E(\d)[A-Z]?-PSG\.edf$", file_name, re.IGNORECASE
    )
    hyp_match = re.match(
        r"([A-Z]{2}\d{4})E[0-9A-Z]?-Hypnogram\.edf$", file_name, re.IGNORECASE
    )

    if psg_match:
        result["subject_id"] = psg_match.group(1).upper()
        result["night_index"] = int(psg_match.group(2))
        result["file_type"] = "PSG"
    elif hyp_match:
        result["subject_id"] = hyp_match.group(1).upper()
        result["file_type"] = "Hypnogram"
        # night_index for hypnogram is derived from the 'C' character (cassette convention)
        result["night_index"] = None
    else:
        # Generic fallback: try to extract any alphanumeric prefix
        generic = re.match(r"([A-Z0-9]{4,8})", file_name, re.IGNORECASE)
        if generic:
            result["subject_id"] = generic.group(1).upper()

    return result


# Spark UDF version of parse_edf_filename for use in DataFrames
_parse_filename_udf = F.udf(
    lambda fname: parse_edf_filename(fname)["subject_id"],
    StringType(),
)
_parse_night_udf = F.udf(
    lambda fname: parse_edf_filename(fname)["night_index"],
    IntegerType(),
)
_parse_type_udf = F.udf(
    lambda fname: parse_edf_filename(fname)["file_type"],
    StringType(),
)


# ---------------------------------------------------------------------------
# Core ingestion functions
# ---------------------------------------------------------------------------

def load_raw_files(
    spark: SparkSession,
    source_path: str,
    use_autoloader: bool = True,
    checkpoint_path: Optional[str] = None,
    schema_location: Optional[str] = None,
) -> DataFrame:
    """Load raw EDF files from a directory into a streaming/batch DataFrame.

    Uses Databricks Auto Loader (cloudFiles) when available for incremental,
    exactly-once ingestion with automatic schema inference and evolution.

    Auto Loader key concepts (exam):
    - ``format("cloudFiles")`` enables Auto Loader
    - ``cloudFiles.format`` = inner file format (binaryFile for EDF)
    - ``cloudFiles.schemaLocation`` = persistent path to store inferred schema
    - ``cloudFiles.inferColumnTypes`` = derive types from content
    - Checkpoint location = tracks which files were already processed

    Args:
        spark: Active SparkSession.
        source_path: Directory containing EDF files.
        use_autoloader: Use Auto Loader (True) or plain spark.read (False).
        checkpoint_path: Cloud path for Auto Loader checkpoints.
        schema_location: Cloud path for Auto Loader schema storage.

    Returns:
        A streaming DataFrame (Auto Loader) or batch DataFrame (plain read).
        Columns: path, name, length, modificationTime, content (binary).
    """
    if use_autoloader:
        log.info("Starting Auto Loader ingestion", source=source_path)
        reader = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")   # EDF = binary
            .option("pathGlobFilter", "*.edf")           # Only EDF files
            .option("recursiveFileLookup", "true")        # Scan subdirectories
        )
        if schema_location:
            reader = reader.option("cloudFiles.schemaLocation", schema_location)
        if checkpoint_path:
            # checkpoint is set on writeStream, not readStream
            # storing here for reference when building the write stream
            reader = reader.option("_checkpointLocation", checkpoint_path)
        return reader.load(source_path)
    else:
        # Batch fallback for local development / unit tests
        log.info("Loading EDF files (batch mode)", source=source_path)
        return (
            spark.read
            .format("binaryFile")
            .option("pathGlobFilter", "*.edf")
            .option("recursiveFileLookup", "true")
            .load(source_path)
        )


def enrich_bronze_df(df: DataFrame) -> DataFrame:
    """Add derived columns (subject_id, night_index, file_type) to raw file DataFrame.

    This transformation extracts structured metadata from the file name,
    turning the raw binaryFile row into a proper Bronze schema record.

    Args:
        df: Raw DataFrame from load_raw_files() with columns:
            path, name, length, modificationTime

    Returns:
        DataFrame matching BRONZE_EDF_SCHEMA (minus content binary for storage).
    """
    return df.select(
        _parse_filename_udf(F.col("name")).alias("subject_id"),
        _parse_night_udf(F.col("name")).alias("night_index"),
        _parse_type_udf(F.col("name")).alias("file_type"),
        F.col("path").alias("file_path"),
        F.col("name").alias("file_name"),
        F.col("length").alias("file_size_bytes"),
        F.lit("sleep-edf").alias("dataset_source"),
        F.current_timestamp().alias("ingestion_ts"),
    )


def create_bronze_table(
    spark: SparkSession,
    cfg: AppConfig,
    trigger_once: bool = True,
) -> None:
    """Create or update the Bronze EDF file registry table using Auto Loader.

    Writes a streaming query from the EDF source directory to the Bronze
    Delta table. Using ``trigger(availableNow=True)`` (equivalent to
    trigger-once in newer Spark) processes all available files and stops.

    Exam note: ``trigger(availableNow=True)`` is the modern replacement
    for ``trigger(once=True)``. Both process all backlog files then stop.
    Use ``trigger(processingTime='10 minutes')`` for continuous micro-batch.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration.
        trigger_once: If True, use availableNow trigger (process all + stop).
    """
    source_path = cfg.paths.edf_source_path
    checkpoint = cfg.paths.autoloader_checkpoint
    schema_loc = cfg.paths.autoloader_schema_location
    table_name = cfg.catalog.bronze_edf_fqn

    log.info("Creating Bronze table", table=table_name, source=source_path)

    raw_df = load_raw_files(
        spark,
        source_path,
        use_autoloader=True,
        checkpoint_path=checkpoint,
        schema_location=schema_loc,
    )
    bronze_df = enrich_bronze_df(raw_df)

    write_query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")          # Append-only for Bronze (immutable raw data)
        .option("checkpointLocation", checkpoint)
        .option(
            "mergeSchema", "true",     # Allow schema evolution without failing
        )
    )

    if table_name.count(".") == 2:     # Unity Catalog FQN (catalog.schema.table)
        write_query = write_query.toTable(table_name)
    else:
        write_query = write_query.start(table_name)  # Path-based fallback

    if trigger_once:
        # availableNow=True: process all queued files, then terminate
        query = (
            bronze_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(table_name)
        )
        query.awaitTermination()
        log.info("Bronze ingestion complete", table=table_name)
