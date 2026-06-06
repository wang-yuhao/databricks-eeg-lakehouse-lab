"""Shared pytest fixtures for the EEG Lakehouse test suite.

A SparkSession is expensive to create; we create one per test session
and reuse it across all test modules. This pattern is idiomatic for
PySpark unit testing and appears frequently in Databricks exam scenarios.

Exam link: SDLC / CI/CD domain — understanding how to test Spark pipelines.
"""

import pytest

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession with Delta Lake support for unit testing.

    Skips automatically if PySpark is not installed (e.g. pure-Python CI).
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("eeg-lakehouse-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def sample_eeg_metadata(spark):
    """Return a small synthetic Bronze-style metadata DataFrame for tests."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
    from datetime import datetime

    schema = StructType([
        StructField("subject_id", StringType(), nullable=False),
        StructField("night", IntegerType(), nullable=False),
        StructField("file_name", StringType(), nullable=False),
        StructField("file_size_bytes", LongType(), nullable=True),
    ])

    data = [
        ("SC4001", 1, "SC4001E0-PSG.edf", 1024 * 1024 * 50),
        ("SC4002", 1, "SC4002E0-PSG.edf", 1024 * 1024 * 48),
        ("SC4003", 2, "SC4003E0-PSG.edf", 1024 * 1024 * 52),
        (None,    1, "CORRUPT.edf",       100),  # Intentionally bad row for quality tests
    ]
    return spark.createDataFrame(data, schema)
