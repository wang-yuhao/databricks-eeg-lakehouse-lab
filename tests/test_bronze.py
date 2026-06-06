"""Unit tests for Bronze ingestion module.

Tests cover:
1. File name parsing (Python utility function)
2. DataFrame schema invariants (no null subject_id)
3. Enrichment UDFs produce expected columns

Run: pytest tests/test_bronze.py -v

Exam relevance: Demonstrates testing PySpark code — expected of senior engineers.
Research relevance: Ensures 197-subject EDF corpus is parsed correctly.
"""

import pytest
from pyspark.sql import functions as F
from src.bronze.ingest_eeg_files import (
    parse_edf_filename,
    enrich_bronze_df,
    BRONZE_EDF_SCHEMA,
)


# ---------------------------------------------------------------------------
# parse_edf_filename tests (pure Python, no Spark needed)
# ---------------------------------------------------------------------------

class TestParseEdfFilename:
    def test_psg_cassette(self):
        result = parse_edf_filename("SC4001E0-PSG.edf")
        assert result["subject_id"] == "SC4001"
        assert result["night_index"] == 0
        assert result["file_type"] == "PSG"

    def test_psg_second_night(self):
        result = parse_edf_filename("SC4001E1-PSG.edf")
        assert result["subject_id"] == "SC4001"
        assert result["night_index"] == 1
        assert result["file_type"] == "PSG"

    def test_hypnogram(self):
        result = parse_edf_filename("SC4001EC-Hypnogram.edf")
        assert result["subject_id"] == "SC4001"
        assert result["file_type"] == "Hypnogram"

    def test_telemetry_subject(self):
        result = parse_edf_filename("ST7011J0-PSG.edf")
        assert result["subject_id"] == "ST7011"
        assert result["file_type"] == "PSG"

    def test_unknown_file(self):
        result = parse_edf_filename("unknown.edf")
        # Should not crash; subject_id may be None or partial
        assert isinstance(result, dict)
        assert "subject_id" in result

    def test_case_insensitive(self):
        result = parse_edf_filename("sc4001e0-psg.edf")
        assert result["subject_id"] == "SC4001"


# ---------------------------------------------------------------------------
# DataFrame-level tests (require Spark fixture from conftest.py)
# ---------------------------------------------------------------------------

class TestBronzeDataFrame:
    @pytest.fixture
    def bronze_df(self, spark):
        """Create a synthetic raw binaryFile-style DataFrame."""
        data = [
            ("/data/SC4001E0-PSG.edf",       "SC4001E0-PSG.edf",       130_000_000),
            ("/data/SC4001EC-Hypnogram.edf", "SC4001EC-Hypnogram.edf",      1_200),
            ("/data/SC4002E0-PSG.edf",       "SC4002E0-PSG.edf",       131_000_000),
        ]
        return spark.createDataFrame(data, ["path", "name", "length"])

    def test_enrich_adds_subject_id(self, spark, bronze_df):
        enriched = enrich_bronze_df(bronze_df)
        assert "subject_id" in enriched.columns

    def test_no_null_subject_id(self, spark, bronze_df):
        enriched = enrich_bronze_df(bronze_df)
        null_count = enriched.filter(F.col("subject_id").isNull()).count()
        assert null_count == 0, "All valid EDF files must have a parsed subject_id"

    def test_file_types_classified(self, spark, bronze_df):
        enriched = enrich_bronze_df(bronze_df)
        types = {row.file_type for row in enriched.select("file_type").collect()}
        assert types.issubset({"PSG", "Hypnogram", "unknown"})

    def test_required_columns_present(self, spark, bronze_df):
        enriched = enrich_bronze_df(bronze_df)
        required = {"subject_id", "night_index", "file_type", "file_path",
                    "file_name", "file_size_bytes", "dataset_source", "ingestion_ts"}
        assert required.issubset(set(enriched.columns))

    def test_file_size_non_negative(self, spark, bronze_df):
        enriched = enrich_bronze_df(bronze_df)
        negative = enriched.filter(F.col("file_size_bytes") < 0).count()
        assert negative == 0
