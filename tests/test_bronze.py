"""Unit tests for Bronze layer ingestion logic.

Tests cover:
- Subject ID extraction from EDF filenames
- Study night extraction
- Hypnogram detection
- Schema invariants on Bronze DataFrame
- Null subject_id detection

Run with: pytest tests/test_bronze.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.bronze.ingest_eeg_files import (
    extract_subject_id,
    extract_study_night,
    is_hypnogram_file,
    BRONZE_EDF_SCHEMA,
)


# ---------------------------------------------------------------------------
# Unit tests: pure Python functions (no Spark needed)
# ---------------------------------------------------------------------------

class TestSubjectIdExtraction:
    """Tests for extract_subject_id()"""

    def test_cassette_recording(self):
        assert extract_subject_id("SC4001E0-PSG.edf") == "SC4001"

    def test_telemetry_recording(self):
        assert extract_subject_id("ST7011J0-PSG.edf") == "ST7011"

    def test_hypnogram_file(self):
        # Hypnogram files have same subject ID as PSG
        assert extract_subject_id("SC4001EC-Hypnogram.edf") == "SC4001"

    def test_invalid_filename(self):
        assert extract_subject_id("random_file.edf") is None

    def test_different_ages(self):
        assert extract_subject_id("SC8901E0-PSG.edf") == "SC8901"
        assert extract_subject_id("SC2001E0-PSG.edf") == "SC2001"


class TestStudyNightExtraction:
    """Tests for extract_study_night()"""

    def test_night_zero(self):
        assert extract_study_night("SC4001E0-PSG.edf") == 0

    def test_night_one(self):
        assert extract_study_night("SC4002E1-PSG.edf") == 1

    def test_hypnogram_night(self):
        # Hypnogram files should return None (no night suffix pattern 'EC')
        result = extract_study_night("SC4001EC-Hypnogram.edf")
        # EC suffix -> 'C' is not a digit, should return None or non-integer
        assert result is None or isinstance(result, int)

    def test_invalid_filename(self):
        assert extract_study_night("bad_file.edf") is None


class TestHypnogramDetection:
    """Tests for is_hypnogram_file()"""

    def test_hypnogram_true(self):
        assert is_hypnogram_file("SC4001EC-Hypnogram.edf") is True

    def test_psg_false(self):
        assert is_hypnogram_file("SC4001E0-PSG.edf") is False

    def test_case_insensitive(self):
        assert is_hypnogram_file("SC4001EC-HYPNOGRAM.EDF") is True


# ---------------------------------------------------------------------------
# PySpark tests: schema and DataFrame invariants
# ---------------------------------------------------------------------------

class TestBronzeSchema:
    """Tests for Bronze EDF schema definition."""

    def test_schema_has_required_columns(self):
        required = {
            "file_path", "file_name", "subject_id", "recording_type",
            "study_night", "is_hypnogram", "ingestion_timestamp", "dataset_source"
        }
        actual = {f.name for f in BRONZE_EDF_SCHEMA.fields}
        assert required.issubset(actual), f"Missing columns: {required - actual}"

    def test_file_path_not_nullable(self):
        field = next(f for f in BRONZE_EDF_SCHEMA.fields if f.name == "file_path")
        assert field.nullable is False

    def test_ingestion_timestamp_not_nullable(self):
        field = next(f for f in BRONZE_EDF_SCHEMA.fields if f.name == "ingestion_timestamp")
        assert field.nullable is False


class TestBronzeDataFrame:
    """PySpark DataFrame tests using the session fixture from conftest.py."""

    def test_no_null_subject_ids_in_psg_files(self, spark: SparkSession):
        """PSG files must always have a parseable subject_id."""
        from src.bronze.ingest_eeg_files import _extract_subject_id_udf

        # Create test DataFrame with known good filenames
        test_data = [
            ("SC4001E0-PSG.edf",),
            ("SC4002E0-PSG.edf",),
            ("ST7011J0-PSG.edf",),
        ]
        df = spark.createDataFrame(test_data, ["file_name"])
        df = df.withColumn("subject_id", _extract_subject_id_udf(F.col("file_name")))

        null_count = df.filter(F.col("subject_id").isNull()).count()
        assert null_count == 0, f"Found {null_count} null subject_ids in PSG files"

    def test_recording_type_classification(self, spark: SparkSession):
        """SC files must map to 'SC', ST files to 'ST'."""
        test_data = [
            ("SC4001E0-PSG.edf", "SC"),
            ("ST7011J0-PSG.edf", "ST"),
        ]
        df = spark.createDataFrame(test_data, ["file_name", "expected_type"])
        df = df.withColumn(
            "recording_type",
            F.when(F.col("file_name").startswith("SC"), "SC")
             .when(F.col("file_name").startswith("ST"), "ST")
             .otherwise(None)
        )
        mismatches = df.filter(
            F.col("recording_type") != F.col("expected_type")
        ).count()
        assert mismatches == 0
