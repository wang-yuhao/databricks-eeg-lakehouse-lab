"""Bronze layer: Subject metadata ingestion from CSV.

Ingests a simple CSV subject manifest (subject_id, dataset, n_nights, notes)
into the Bronze subject_metadata Delta table.

Exam relevance: Demonstrates COPY INTO (idempotent batch load) and schema
enforcement on structured files, contrasting with Auto Loader for binary files.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, StringType, StructField, StructType,
)

from src.utils.config import AppConfig
from src.utils.logging import get_logger

log = get_logger(__name__)

BRONZE_METADATA_SCHEMA = StructType([
    StructField("subject_id",    StringType(),  nullable=False),
    StructField("dataset",       StringType(),  nullable=False),  # SC | ST | CAP
    StructField("n_nights",      IntegerType(), nullable=True),
    StructField("has_psg",       BooleanType(), nullable=True),
    StructField("has_hypnogram", BooleanType(), nullable=True),
    StructField("notes",         StringType(),  nullable=True),
])


def load_metadata_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    """Load subject metadata CSV with explicit schema (no inference).

    Using explicit schema (not inferSchema=True) is production best practice:
    - Deterministic types across runs
    - Fails fast if source CSV has wrong columns
    - Avoids full-file scan for schema inference

    Args:
        spark: Active SparkSession.
        csv_path: Path to the subject metadata CSV.

    Returns:
        DataFrame with BRONZE_METADATA_SCHEMA columns.
    """
    log.info("Loading metadata CSV", path=csv_path)
    return (
        spark.read
        .format("csv")
        .schema(BRONZE_METADATA_SCHEMA)  # Explicit schema — no inference
        .option("header", "true")
        .option("nullValue", "")
        .option("mode", "FAILFAST")       # Fail on malformed rows (not PERMISSIVE)
        .load(csv_path)
    )


def write_metadata_table(
    spark: SparkSession,
    df: DataFrame,
    cfg: AppConfig,
    mode: str = "overwrite",
) -> None:
    """Write metadata DataFrame to Bronze Delta table.

    Exam note: For metadata that changes rarely, a full overwrite is simpler
    than MERGE INTO. For large/incremental metadata, use MERGE INTO (Day 13).

    Args:
        spark: Active SparkSession.
        df: Validated metadata DataFrame.
        cfg: Application configuration.
        mode: Write mode ('overwrite' or 'append').
    """
    table_name = cfg.catalog.bronze_metadata_fqn
    log.info("Writing metadata to Bronze", table=table_name)
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")  # Needed for overwrite with schema change
        .saveAsTable(table_name)
    )
    log.info("Metadata written", table=table_name, rows=df.count())


def generate_metadata_from_edf_registry(
    spark: SparkSession, cfg: AppConfig
) -> DataFrame:
    """Derive subject metadata from the Bronze EDF file registry.

    When no external CSV is available, infer subject metadata from the
    file registry: which subjects have PSG, Hypnogram, how many nights.

    Args:
        spark: Active SparkSession.
        cfg: Application configuration.

    Returns:
        DataFrame with BRONZE_METADATA_SCHEMA.
    """
    registry = spark.read.format("delta").table(cfg.catalog.bronze_edf_fqn)

    return (
        registry
        .groupBy("subject_id", "dataset_source")
        .agg(
            F.countDistinct("night_index").alias("n_nights"),
            F.max(F.when(F.col("file_type") == "PSG", True)).alias("has_psg"),
            F.max(F.when(F.col("file_type") == "Hypnogram", True)).alias("has_hypnogram"),
            F.lit(None).cast(StringType()).alias("notes"),
        )
        .withColumnRenamed("dataset_source", "dataset")
    )
