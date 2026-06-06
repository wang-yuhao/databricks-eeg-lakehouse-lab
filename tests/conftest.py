"""Pytest configuration and shared fixtures for all test modules.

This conftest creates a local SparkSession with Delta Lake extensions for testing.
All test modules import fixtures from here automatically (pytest conftest.py magic).

Exam relevance: Demonstrates production testing discipline;
knowing how to test PySpark code is expected of senior data engineers.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession with Delta Lake for the test suite.

    The ``scope="session"`` means one Spark instance is reused across all tests
    in a pytest session — avoids expensive SparkContext restarts.
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("eeg-lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")  # Small value for fast tests
        .config("spark.ui.enabled", "false")           # Disable Spark UI in tests
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def app_config():
    """Return the default AppConfig singleton for test use."""
    from src.utils.config import AppConfig
    return AppConfig()
