from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType
)
from src.utils.config import AppConfig, DEFAULT_CONFIG
from src.utils.logging import get_logger
import os

log = get_logger(__name__)

SUBJECT_METADATA_SCHEMA = StructType([
    StructField("subject_id", StringType(), nullable=False),
    StructField("recording_type", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("sex", StringType(), nullable=True),
    StructField("lightsoff_time", StringType(), nullable=True),
    StructField("lightson_time", StringType(), nullable=True),
    StructField("dataset_source", StringType(), nullable=True),
])

def _load_from_csv(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    log.info(f"Loading subject metadata from CSV: {path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(SUBJECT_METADATA_SCHEMA)
        .csv(path)
    )
    return df

def _load_from_excel(
    spark: SparkSession,
    path: str,
    sheet_name: str = None,
) -> DataFrame:
    """
    Load Excel (xls/xlsx) using pandas or pandas-on-Spark and then convert to Spark DataFrame.
    This keeps the Bronze schema explicit.
    """
    import pandas as pd

    log.info(f"Loading subject metadata from Excel: {path}, sheet={sheet_name or 'default'}")
    pdf = pd.read_excel(path, sheet_name=sheet_name)

    # Normalize column names to match SUBJECT_METADATA_SCHEMA
    pdf = pdf.rename(columns={
        "subject": "subject_id",
        "sex": "sex",
        "age": "age",
        # add other mappings depending on SC/ST subjects layout
    })

    # Select only the expected columns; fill missing ones if needed
    expected_cols = [field.name for field in SUBJECT_METADATA_SCHEMA]
    for col in expected_cols:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[expected_cols]

    df = spark.createDataFrame(pdf, schema=SUBJECT_METADATA_SCHEMA)
    return df

def _load_from_text(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    """
    Example loader for line-oriented text metadata (e.g., a custom RECORDS-style file).
    You’d parse the text into columns, then apply SUBJECT_METADATA_SCHEMA.
    """
    log.info(f"Loading subject metadata from text: {path}")
    raw_df = spark.read.text(path)

    # TODO: implement proper parsing of raw_df["value"] into columns:
    # subject_id, recording_type, etc.
    # For now, raise to force you to implement this per format.
    raise NotImplementedError("Text metadata loader not yet implemented.")

def load_subject_metadata(
    spark: SparkSession,
    path: str,
    file_type: str = "csv",
    cfg: AppConfig = DEFAULT_CONFIG,
) -> DataFrame:
    """
    Dispatcher: load subject metadata from CSV, Excel, text, or other source.
    file_type: 'csv', 'excel', 'text', or 'auto'.
    """
    file_type = file_type.lower()
    ext = os.path.splitext(path)[1].lower()

    if file_type == "auto":
        # Infer from extension
        if ext in [".csv"]:
            file_type = "csv"
        elif ext in [".xls", ".xlsx"]:
            file_type = "excel"
        elif ext in [".txt"]:
            file_type = "text"
        else:
            raise ValueError(f"Unsupported extension for auto mode: {ext}")

    if file_type == "csv":
        df = _load_from_csv(spark, path)
    elif file_type == "excel":
        df = _load_from_excel(spark, path)
    elif file_type == "text":
        df = _load_from_text(spark, path)
    else:
        raise ValueError(f"Unsupported file_type: {file_type}")

    # Add ingestion timestamp for audit
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    return df

def create_bronze_metadata_table(
    spark: SparkSession,
    path: str,
    file_type: str = "auto",
    cfg: AppConfig = DEFAULT_CONFIG,
) -> None:
    """
    Write subject metadata to Bronze Delta table (overwrite mode),
    from CSV, Excel, or other supported formats.
    """
    target_table = cfg.catalog.bronze_metadata_fqn
    log.info(f"Writing metadata to: {target_table}")
    log.info(f"Loading subject metadata from: {path} (file_type={file_type})")

    df = load_subject_metadata(spark, path, file_type=file_type, cfg=cfg)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    log.info(f"Metadata table written: {target_table} ({df.count()} rows)")