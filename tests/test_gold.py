# tests/test_gold.py
# =============================================================================
# Day 10-11: Gold layer tests + MLflow mock tests
# =============================================================================

import pytest
import pandas as pd
import numpy as np
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def silver_df(spark):
    """Mock Silver preprocessed DataFrame."""
    schema = StructType([
        StructField("subject_id",    StringType(),  False),
        StructField("session",       StringType(),  False),
        StructField("epoch_index",   IntegerType(), False),
        StructField("recording_date",DateType(),    True),
        StructField("sigma_power",   DoubleType(),  True),
        StructField("delta_power",   DoubleType(),  True),
        StructField("beta_power",    DoubleType(),  True),
        StructField("theta_power",   DoubleType(),  True),
    ])
    rows = [
        ("SC4001", "night1", i, date(2024, 1, 10), 0.4 + i*0.01, 0.5, 0.1, 0.2)
        for i in range(10)
    ] + [
        ("SC4002", "night1", i, date(2024, 1, 11), 0.2, 0.6, 0.15, 0.25)
        for i in range(10)
    ]
    return spark.createDataFrame(rows, schema=schema)


@pytest.fixture(scope="module")
def events_df(spark):
    """Mock Silver events DataFrame."""
    schema = StructType([
        StructField("subject_id",   StringType(),  False),
        StructField("session",      StringType(),  False),
        StructField("epoch_index",  IntegerType(), False),
        StructField("event_type",   StringType(),  False),
        StructField("duration_s",   DoubleType(),  True),
        StructField("amplitude_uv", DoubleType(),  True),
        StructField("neg_peak_uv",  DoubleType(),  True),
        StructField("channel",      StringType(),  True),
    ])
    rows = []
    for i in range(8):  # 8 spindles for SC4001
        rows.append(("SC4001", "night1", i % 10, "spindle", 0.8, 45.0, None, "Fz"))
    for i in range(4):  # 4 SOs for SC4001
        rows.append(("SC4001", "night1", i % 10, "slow_oscillation", 1.0, None, -80.0, "Fz"))
    for i in range(3):  # 3 spindles for SC4002
        rows.append(("SC4002", "night1", i % 10, "spindle", 0.9, 40.0, None, "Fz"))
    return spark.createDataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGoldFeatureTable:
    """Tests for build_feature_table() in src/gold/build_features.py."""

    def test_output_row_count(self, spark, silver_df, events_df):
        """One row per (subject_id, session) in Gold output."""
        import sys; sys.path.insert(0, ".")
        from src.gold.build_features import build_feature_table
        df_gold = build_feature_table(silver_df, events_df)
        assert df_gold.count() == 2  # SC4001, SC4002

    def test_required_columns_present(self, spark, silver_df, events_df):
        """All expected feature columns must exist."""
        from src.gold.build_features import build_feature_table
        df_gold = build_feature_table(silver_df, events_df)
        required_cols = [
            "subject_id", "session",
            "spindle_count", "spindle_density", "spindle_amplitude_mean",
            "so_count", "so_density",
            "pac_mi",
            "sigma_power_mean", "delta_power_mean",
            "memory_score",
        ]
        actual_cols = df_gold.columns
        for col in required_cols:
            assert col in actual_cols, f"Missing column: {col}"

    def test_spindle_count_correct(self, spark, silver_df, events_df):
        """SC4001 should have 8 spindles; SC4002 should have 3."""
        from src.gold.build_features import build_feature_table
        df_gold = build_feature_table(silver_df, events_df)
        pd_gold = df_gold.toPandas().set_index("subject_id")
        assert int(pd_gold.loc["SC4001", "spindle_count"]) == 8
        assert int(pd_gold.loc["SC4002", "spindle_count"]) == 3

    def test_pac_mi_non_negative(self, spark, silver_df, events_df):
        """PAC MI must be >= 0 for all subjects."""
        from src.gold.build_features import build_feature_table
        df_gold = build_feature_table(silver_df, events_df)
        min_pac = df_gold.agg(F.min("pac_mi")).collect()[0][0]
        assert min_pac >= 0.0

    def test_memory_score_binary(self, spark, silver_df, events_df):
        """memory_score must be 0 or 1 only."""
        from src.gold.build_features import build_feature_table
        df_gold = build_feature_table(silver_df, events_df)
        invalid = df_gold.filter(~F.col("memory_score").isin(0, 1)).count()
        assert invalid == 0, "memory_score contains values other than 0 or 1"

    def test_no_null_subject_id(self, spark, silver_df, events_df):
        """subject_id must never be null in Gold."""
        from src.gold.build_features import build_feature_table
        df_gold = build_feature_table(silver_df, events_df)
        null_count = df_gold.filter(F.col("subject_id").isNull()).count()
        assert null_count == 0

    def test_so_count_zero_filled(self, spark, silver_df, events_df):
        """Subjects with no SOs should have so_count=0, not null."""
        from src.gold.build_features import build_feature_table
        df_gold = build_feature_table(silver_df, events_df)
        # SC4002 has no SOs -> so_count should be 0
        sc4002 = df_gold.filter(F.col("subject_id") == "SC4002").toPandas()
        assert int(sc4002["so_count"].values[0]) == 0


class TestMockGoldData:
    """Tests for create_mock_gold_data() used in Day 10 MLflow training."""

    def test_mock_data_shape(self):
        from src.gold.train_ml_model import create_mock_gold_data
        df = create_mock_gold_data(n=100)
        assert len(df) == 100
        assert "memory_score" in df.columns

    def test_memory_score_balance(self):
        """Binary target should not be completely imbalanced."""
        from src.gold.train_ml_model import create_mock_gold_data
        df = create_mock_gold_data(n=200)
        pos_rate = df["memory_score"].mean()
        # Should be roughly 25% (top quartile of spindle AND sigma)
        assert 0.1 < pos_rate < 0.9, f"Unexpected class balance: {pos_rate:.2f}"

    def test_no_nulls_in_features(self):
        from src.gold.train_ml_model import create_mock_gold_data, FEATURE_COLS
        df = create_mock_gold_data(n=50)
        assert df[FEATURE_COLS].isnull().sum().sum() == 0
