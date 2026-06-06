"""Bronze layer: Subject metadata CSV ingestion.

The Sleep-EDF Expanded dataset ships with a companion CSV file containing
subject demographics (age, sex, lights-off/on times). This module ingests
that CSV into the Bronze metadata table.

Exam relevance:
- Demonstrates reading structured CSV with explicit schema (avoids schema
  inference cost on production pipelines).
- COPY INTO pattern for idempotent batch CSV loads (alternative to Auto Loader
  when files are known and finite).

Research relevance:
- Subject age and sex are covariates in the linear mixed-effects model.
- Lights-off/on times are needed to align sleep stage annotations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType
)

from src.utils.config import AppConfig, DEFAULT_CONFIG
from src.utils.logging import get_logger

log = get_logger(__name__)


SUBJECT_METADATA_SCHEMA = StructType([
    StructField("subject_id",      StringType(),  nullable=False),
    StructField("recording_type",  StringType(),  nullable=True),
    StructField("age",             IntegerType(), nullable=True),
    StructField("sex",             StringType(),  nullable=True),
    StructField("lightsoff_time",  StringType(),  nullable=True),
    StructField("lightson_time",   StringType(),  nullable=True),
    StructField("dataset_source",  StringType(),  nullable=True),
])


def load_subject_metadata(
    spark: SparkSession,
    csv_path: str,
    cfg: AppConfig = DEFAULT_CONFIG,
) -> DataFrame:
    """Load subject metadata from companion CSV into a DataFrame.

    Args:
        spark: Active SparkSession.
        csv_path: Path to the Sleep-EDF companion CSV.
        cfg: App config.

    Returns:
        DataFrame matching SUBJECT_METADATA_SCHEMA.
    """
    log.info(f"Loading subject metadata from: {csv_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # Exam tip: explicit schema >> inferSchema
        .schema(SUBJECT_METADATA_SCHEMA)
        .csv(csv_path)
    )

    # Add ingestion timestamp for audit
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())

    return df


def create_bronze_metadata_table(
    spark: SparkSession,
    csv_path: str,
    cfg: AppConfig = DEFAULT_CONFIG,
) -> None:
    """Write subject metadata to Bronze Delta table (overwrite mode).

    Exam note: For small, slowly-changing reference tables (like subject
    demographics), OVERWRITE mode is appropriate. For large append-only
    tables, use APPEND or MERGE INTO.
    """
    target_table = cfg.catalog.bronze_metadata_fqn
    log.info(f"Writing metadata to: {target_table}")

    df = load_subject_metadata(spark, csv_path, cfg)

    (
        df.write
        .format("delta")
        .mode("overwrite")  # Idempotent: safe to re-run
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    log.info(f"Metadata table written: {target_table} ({df.count()} rows)")
