# =============================================================================
# src/gold/build_features.py
# Day 6: Gold Feature Table Builder
# Exam domain: Data Modeling & Optimization (OPTIMIZE, ZORDER, feature tables)
# Research: Builds ML-ready feature vectors for TDA + spindle/SO analysis
# =============================================================================
"""
Gold layer: join Silver preprocessed EEG + Silver event detections
to produce one wide row per (subject_id, session, recording_date).

Key columns produced
--------------------
spindle_density        : spindles / minute of N2/N3 sleep
spindle_amplitude_mean : mean peak-to-peak amplitude across spindles (uV)
spindle_duration_mean  : mean spindle duration (s)
so_density             : slow oscillations / minute
so_neg_peak_mean       : mean SO negative-peak amplitude (uV)
pac_mi                 : Phase-Amplitude Coupling modulation index (proxy)
sigma_power_mean       : mean sigma-band (12-15 Hz) power in N2/N3
beta_power_mean        : mean beta-band (15-30 Hz) power
delta_power_mean       : mean delta-band (0.5-4 Hz) power
memory_score           : synthetic target label (high/low) for Day-10 ML

Databricks exam patterns used
------------------------------
- Delta MERGE INTO for idempotent Gold writes
- OPTIMIZE + ZORDER BY (subject_id, recording_date)
- DESCRIBE DETAIL / DESCRIBE HISTORY queries shown in notebook
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Feature aggregation helpers
# ---------------------------------------------------------------------------

def _spindle_features(df_events: DataFrame) -> DataFrame:
    """
    Aggregate spindle event rows -> one row per (subject_id, session).

    Expects df_events to have columns:
      subject_id, session, event_type, duration_s, amplitude_uv, channel
    """
    spindles = df_events.filter(F.col("event_type") == "spindle")

    # Recording length proxy: total distinct 30-s epochs flagged with spindles.
    # In production this comes from EEG metadata; we proxy with epoch counts.
    spindle_agg = (
        spindles.groupBy("subject_id", "session")
        .agg(
            F.count("*").alias("spindle_count"),
            F.mean("duration_s").alias("spindle_duration_mean"),
            F.mean("amplitude_uv").alias("spindle_amplitude_mean"),
            F.stddev("amplitude_uv").alias("spindle_amplitude_std"),
            # Density: spindles per minute (assume 1 epoch = 30 s)
            (F.count("*") / (F.countDistinct("epoch_index") * 0.5)).alias(
                "spindle_density"
            ),
        )
    )
    return spindle_agg


def _so_features(df_events: DataFrame) -> DataFrame:
    """Aggregate slow-oscillation event rows."""
    sos = df_events.filter(F.col("event_type") == "slow_oscillation")
    so_agg = (
        sos.groupBy("subject_id", "session")
        .agg(
            F.count("*").alias("so_count"),
            F.mean("neg_peak_uv").alias("so_neg_peak_mean"),
            F.mean("duration_s").alias("so_duration_mean"),
            (F.count("*") / (F.countDistinct("epoch_index") * 0.5)).alias(
                "so_density"
            ),
        )
    )
    return so_agg


def _power_features(df_silver: DataFrame) -> DataFrame:
    """
    Band-power features aggregated per (subject_id, session).
    df_silver must have columns: sigma_power, delta_power, beta_power
    from Day-4 preprocessing.
    """
    return (
        df_silver.groupBy("subject_id", "session")
        .agg(
            F.mean("sigma_power").alias("sigma_power_mean"),
            F.mean("delta_power").alias("delta_power_mean"),
            F.mean("beta_power").alias("beta_power_mean"),
            F.mean("theta_power").alias("theta_power_mean"),
        )
    )


def _pac_proxy(df_events: DataFrame) -> DataFrame:
    """
    Proxy Phase-Amplitude Coupling (PAC) Modulation Index:
    spindle_count / so_count per session.  Real PAC (Tort MI) computed
    in Day-10 MLflow experiment using scipy hilbert.
    """
    spindle_counts = (
        df_events.filter(F.col("event_type") == "spindle")
        .groupBy("subject_id", "session")
        .agg(F.count("*").alias("_n_spindles"))
    )
    so_counts = (
        df_events.filter(F.col("event_type") == "slow_oscillation")
        .groupBy("subject_id", "session")
        .agg(F.count("*").alias("_n_sos"))
    )
    pac = spindle_counts.join(so_counts, ["subject_id", "session"], "left").withColumn(
        "pac_mi",
        F.when(
            F.col("_n_sos") > 0,
            F.col("_n_spindles") / F.col("_n_sos").cast(DoubleType()),
        ).otherwise(F.lit(0.0)),
    ).select("subject_id", "session", "pac_mi")
    return pac


def _synthetic_memory_label(df_features: DataFrame) -> DataFrame:
    """
    Synthetic binary memory label for Day-10 ML demo.
    Rule: high spindle density (> median) AND high sigma power -> label=1.
    In production, replace with actual declarative memory task scores
    from the Sleep-EDF / SHHS phenotype files.
    """
    median_spindle = df_features.approxQuantile("spindle_density", [0.5], 0.01)[0]
    median_sigma = df_features.approxQuantile("sigma_power_mean", [0.5], 0.01)[0]
    return df_features.withColumn(
        "memory_score",
        F.when(
            (F.col("spindle_density") > median_spindle)
            & (F.col("sigma_power_mean") > median_sigma),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )


# ---------------------------------------------------------------------------
# 2. Main builder
# ---------------------------------------------------------------------------

def build_feature_table(
    df_silver_preprocessed: DataFrame,
    df_events: DataFrame,
    recording_date_col: str = "recording_date",
) -> DataFrame:
    """
    Join Silver preprocessed signal features + Silver event detections
    into one wide Gold feature table.

    Parameters
    ----------
    df_silver_preprocessed : DataFrame
        Output of Day-4 preprocess_eeg() - one row per epoch.
    df_events : DataFrame
        Output of Day-5 detect_spindles() / detect_slow_oscillations().
    recording_date_col : str
        Column name for the recording date partition key.

    Returns
    -------
    DataFrame
        One row per (subject_id, session) with all feature columns.
    """
    logger.info("Building Gold feature table...")

    # --- aggregate each feature group ---
    power_feats = _power_features(df_silver_preprocessed)
    spindle_feats = _spindle_features(df_events)
    so_feats = _so_features(df_events)
    pac_feats = _pac_proxy(df_events)

    # --- join all feature groups on (subject_id, session) ---
    join_keys = ["subject_id", "session"]
    df_gold = (
        power_feats
        .join(spindle_feats, join_keys, "left")
        .join(so_feats, join_keys, "left")
        .join(pac_feats, join_keys, "left")
    )

    # --- carry through recording_date for partitioning ---
    if recording_date_col in df_silver_preprocessed.columns:
        date_df = (
            df_silver_preprocessed
            .select("subject_id", "session", recording_date_col)
            .distinct()
        )
        df_gold = df_gold.join(date_df, join_keys, "left")

    # --- fill nulls (subjects with 0 events) ---
    fill_map = {
        "spindle_count": 0, "spindle_density": 0.0,
        "so_count": 0, "so_density": 0.0, "pac_mi": 0.0,
    }
    df_gold = df_gold.fillna(fill_map)

    # --- synthetic memory label ---
    df_gold = _synthetic_memory_label(df_gold)

    logger.info("Gold feature table built with %d columns.", len(df_gold.columns))
    return df_gold


# ---------------------------------------------------------------------------
# 3. Delta write + OPTIMIZE
# ---------------------------------------------------------------------------

def write_gold_table(
    spark: SparkSession,
    df_gold: DataFrame,
    table_name: str = "eeg_lakehouse.gold.eeg_features",
    optimize_zorder: bool = True,
) -> None:
    """
    Write Gold feature table to Delta using MERGE INTO for idempotency.
    Then run OPTIMIZE ... ZORDER BY (subject_id, recording_date).

    Exam pattern: MERGE INTO is the canonical upsert in Delta Lake.
    ZORDER BY co-locates data accessed together -> faster queries on
    specific subjects / date ranges.
    """
    # -- write (overwrite in dev; MERGE in prod) --
    (
        df_gold.write
        .format("delta")
        .mode("overwrite")  # swap for MERGE in production (Day 13)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    logger.info("Written Gold table: %s", table_name)

    if optimize_zorder:
        logger.info("Running OPTIMIZE + ZORDER on %s ...", table_name)
        spark.sql(
            f"OPTIMIZE {table_name} ZORDER BY (subject_id, recording_date)"
        )
        logger.info("OPTIMIZE complete.")


def show_table_history(spark: SparkSession, table_name: str) -> None:
    """Print DESCRIBE HISTORY for exam demo / audit trail."""
    spark.sql(f"DESCRIBE HISTORY {table_name}").show(5, truncate=False)


def show_table_detail(spark: SparkSession, table_name: str) -> None:
    """Print DESCRIBE DETAIL (file count, size, partitions)."""
    spark.sql(f"DESCRIBE DETAIL {table_name}").show(1, truncate=False)
