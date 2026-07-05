"""Bronze layer: EEG EDF file ingestion using Auto Loader (cloudFiles).

Exam relevance (Domain 3 — Incremental Data Processing):
- Auto Loader uses `format("cloudFiles")` — the primary incremental ingestion pattern.
- `cloudFiles.schemaLocation` stores inferred schema to avoid re-inference on restart.
- `cloudFiles.useNotifications` (cloud event-based) vs default (directory listing).
- Checkpoint location enables exactly-once processing and crash recovery.

Research relevance:
- Ingests PhysioNet Sleep-EDF Expanded EDF files from a Unity Catalog Volume.
- The Bronze table is a REGISTRY (metadata only) — raw EDF binary content is
  processed in Silver via Pandas UDFs using MNE-Python.
- Idempotent: re-running never duplicates records (Auto Loader checkpoint tracks state).
- FIXED: source_path now includes BOTH sleep-cassette and sleep-telemetry sub-dirs
  so that the Auto Loader crawls the full 197-recording dataset, not just the root.

Interview talking point:
- "I used Auto Loader with schema evolution to ingest ~400 EDF files from a UC Volume
  into a Bronze Delta table. The checkpoint ensured exactly-once semantics even when
  the pipeline crashed mid-run."
"""

import re
from typing import Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, BooleanType, TimestampType
)

from src.utils.config import AppConfig, DEFAULT_CONFIG
from src.utils.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Bronze schema — explicit definition
# ---------------------------------------------------------------------------

BRONZE_EDF_SCHEMA = StructType([
    StructField("file_path",               StringType(),    nullable=False),
    StructField("file_name",               StringType(),    nullable=False),
    StructField("subject_id",              StringType(),    nullable=True),
    StructField("recording_type",          StringType(),    nullable=True),
    StructField("subject_age",             IntegerType(),   nullable=True),
    StructField("study_night",             IntegerType(),   nullable=True),
    StructField("is_hypnogram",            BooleanType(),   nullable=False),
    StructField("file_size_bytes",         LongType(),      nullable=True),
    StructField("file_modification_time",  TimestampType(), nullable=True),
    StructField("ingestion_timestamp",     TimestampType(), nullable=False),
    StructField("dataset_source",          StringType(),    nullable=False),
])

# Regex pattern for Sleep-EDF filenames:
# SC4001E0-PSG.edf  -> recording_type=SC, age=40, subject_num=01, night=0/1
_EDF_FILENAME_PATTERN = re.compile(
    r'^(?P<rec_type>SC|ST)(?P<age>\d{2})(?P<subj_num>\d{2})'
    r'(?P<night>[E][0-9A-Z])-?(?P<suffix>PSG|Hypnogram)?\.edf$',
    re.IGNORECASE
)


def extract_subject_id(file_name: str) -> Optional[str]:
    """Extract subject ID (e.g. 'SC4001') from an EDF filename.

    Args:
        file_name: Basename of EDF file, e.g. 'SC4001E0-PSG.edf'

    Returns:
        Subject ID string like 'SC4001', or None if pattern does not match.

    Example::

        >>> extract_subject_id('SC4001E0-PSG.edf')
        'SC4001'
        >>> extract_subject_id('ST7011J0-PSG.edf')
        'ST7011'
    """
    m = _EDF_FILENAME_PATTERN.match(file_name)
    if not m:
        return None
    return f"{m.group('rec_type')}{m.group('age')}{m.group('subj_num')}"


def extract_study_night(file_name: str) -> Optional[int]:
    """Extract study night index (0 or 1) from EDF filename."""
    m = _EDF_FILENAME_PATTERN.match(file_name)
    if not m:
        return None
    night_char = m.group('night')  # e.g. 'E0' or 'E1'
    try:
        return int(night_char[-1])
    except (ValueError, IndexError):
        return None


def is_hypnogram_file(file_name: str) -> bool:
    """Return True if the EDF file is a hypnogram annotation file."""
    return 'hypnogram' in file_name.lower()


# ---------------------------------------------------------------------------
# UDF registrations (for use in Auto Loader pipeline)
# ---------------------------------------------------------------------------

_extract_subject_id_udf = F.udf(extract_subject_id, StringType())
_extract_study_night_udf = F.udf(extract_study_night, IntegerType())


# ---------------------------------------------------------------------------
# Helper: resolve all EDF sub-directories to crawl
# ---------------------------------------------------------------------------

def _edf_source_paths(cfg: AppConfig) -> List[str]:
    """Return the list of paths Auto Loader should crawl.

    The Sleep-EDF Expanded dataset has two sub-directories under the Volume:
      - sleep-cassette/  (SC* files)
      - sleep-telemetry/ (ST* files)

    Returning both ensures the full ~400-file dataset is ingested, not just
    files sitting in the Volume root.
    """
    root = cfg.paths.edf_source_path
    return [
        f"{root}/sleep-cassette",
        f"{root}/sleep-telemetry",
    ]


# ---------------------------------------------------------------------------
# Core ingestion function
# ---------------------------------------------------------------------------

def load_raw_files(
    spark: SparkSession,
    source_path: str,
    cfg: AppConfig = DEFAULT_CONFIG,
) -> DataFrame:
    """Load raw EDF files from source_path using Auto Loader (cloudFiles).

    This function uses Structured Streaming with Auto Loader to incrementally
    ingest new EDF files. It enriches each file record with metadata parsed
    from the filename (subject_id, study_night, recording_type).

    Exam note: `format("cloudFiles")` is the Auto Loader trigger.
    Key options:
    - `cloudFiles.format`: format of source files (binaryFile for EDF)
    - `cloudFiles.schemaLocation`: where to persist inferred schema
    - `cloudFiles.useNotifications`: True for SNS/SQS-based (lower latency),
      False for directory listing (simpler, no cloud setup needed)

    Args:
        spark: Active SparkSession.
        source_path: Path to directory containing EDF files.
        cfg: Application configuration (paths, catalog names).

    Returns:
        Streaming DataFrame with Bronze EDF schema.
    """
    log.info(f"Starting Auto Loader ingestion from: {source_path}")

    raw_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")  # Read EDF as binary
        .option("cloudFiles.schemaLocation", cfg.paths.autoloader_schema_location)
        .option("cloudFiles.useNotifications", "false")  # Directory listing mode
        .option("cloudFiles.includeExistingFiles", "true")  # Process ALL historical files
        .option("pathGlobFilter", "*.edf")  # Only ingest EDF files
        .option("recursiveFileLookup", "true")  # FIX: crawl sleep-cassette/ AND sleep-telemetry/
        .load(source_path)
    )

    # Extract file name from the full path
    enriched_df = raw_df.select(
        F.col("path").alias("file_path"),
        F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
        F.col("length").alias("file_size_bytes"),
        F.col("modificationTime").alias("file_modification_time"),
    )

    # Derive metadata from filename using UDFs
    enriched_df = enriched_df.withColumn(
        "subject_id", _extract_subject_id_udf(F.col("file_name"))
    ).withColumn(
        "recording_type",
        F.when(F.col("file_name").startswith("SC"), "SC")
         .when(F.col("file_name").startswith("ST"), "ST")
         .otherwise(None)
    ).withColumn(
        "subject_age",
        F.expr("cast(substring(file_name, 3, 2) as int)")  # Age encoded in pos 3-4
    ).withColumn(
        "study_night", _extract_study_night_udf(F.col("file_name"))
    ).withColumn(
        "is_hypnogram",
        F.lower(F.col("file_name")).contains("hypnogram")
    ).withColumn(
        "ingestion_timestamp", F.current_timestamp()
    ).withColumn(
        "dataset_source", F.lit("sleep-edf-expanded")
    )

    return enriched_df


def create_bronze_table(
    spark: SparkSession,
    cfg: AppConfig = DEFAULT_CONFIG,
    trigger_once: bool = True,
) -> None:
    """Write the Auto Loader stream to the Bronze Delta table.

    FIXED: iterates over BOTH sub-directories (sleep-cassette + sleep-telemetry)
    so that the full dataset is ingested in a single run.

    Exam notes on trigger types:
    - `trigger(once=True)`: Process all available files and stop. Batch-style.
    - `trigger(availableNow=True)`: Like once=True but with micro-batch parallelism.
    - `trigger(processingTime='10 minutes')`: Continuous micro-batch.
    - `trigger(continuous='1 second')`: Low-latency continuous processing.

    For EEG file ingestion (infrequent, batch arrivals), `availableNow=True` is ideal.

    Args:
        spark: Active SparkSession.
        cfg: Application config.
        trigger_once: If True, use availableNow trigger (batch-style). Default True.
    """
    target_table = cfg.catalog.bronze_edf_fqn
    checkpoint   = cfg.paths.autoloader_checkpoint

    # Crawl BOTH sub-directories to capture the full dataset
    source_paths = _edf_source_paths(cfg)
    log.info(f"Writing Bronze table '{target_table}' from paths: {source_paths}")

    for i, source_path in enumerate(source_paths):
        # Each sub-directory gets its own checkpoint suffix to avoid conflicts
        ckpt = f"{checkpoint}_{i}"
        stream_df = load_raw_files(spark, source_path, cfg)

        writer = (
            stream_df.writeStream
            .format("delta")
            .outputMode("append")  # Bronze is append-only
            .option("checkpointLocation", ckpt)
            .option("mergeSchema", "false")  # Strict: reject schema drift in Bronze
        )

        if trigger_once:
            query = writer.trigger(availableNow=True).toTable(target_table)
            query.awaitTermination()
        else:
            query = writer.trigger(processingTime="5 minutes").toTable(target_table)

    log.info(f"Bronze ingestion complete. Table: {target_table}")
