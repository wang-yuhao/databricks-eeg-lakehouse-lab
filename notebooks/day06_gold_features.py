# Databricks notebook source
# notebooks/day06_gold_features.py
# =============================================================================
# DAY 6 - Gold Feature Table: OPTIMIZE, ZORDER, DESCRIBE DETAIL/HISTORY
# =============================================================================
# EXAM DOMAINS COVERED:
#   - Data Modeling & Optimization: medallion design, wide feature tables
#   - OPTIMIZE + ZORDER BY: Z-ordering for query skipping
#   - DESCRIBE DETAIL / DESCRIBE HISTORY: Delta audit trail
#   - MERGE INTO (preview for Day 13)
# RESEARCH VALUE:
#   - Produces final input features for TDA + memory consolidation ML model
#   - Spindle density, SO density, PAC MI, band powers -> Day 10 MLflow
# =============================================================================

# COMMAND ----------
# %md
# ## Day 6: Build Gold Feature Table
# **Goal**: Join Silver preprocessed EEG + Silver events -> wide Gold feature table
# that is optimized for fast queries and ML training.
#
# | Step | Operation | Exam Pattern |
# |------|-----------|---------------|
# | 1 | Build mock Silver data | DataFrame API |
# | 2 | Call build_feature_table() | Joins + agg |
# | 3 | Write to Delta (overwrite) | saveAsTable |
# | 4 | OPTIMIZE + ZORDER BY | Performance |
# | 5 | DESCRIBE DETAIL/HISTORY | Delta metadata |
# | 6 | Time travel query | SELECT VERSION AS OF |

# COMMAND ----------
# %md ### Setup

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys
sys.path.insert(0, "/Workspace/Repos/wang-yuhao/databricks-eeg-lakehouse-lab")

from src.gold.build_features import build_feature_table, write_gold_table
from src.utils.config import AppConfig

cfg = AppConfig()
print("Gold table FQN:", cfg.catalog.gold_features_fqn)

# COMMAND ----------
# %md ### Step 1: Create Mock Silver DataFrames
# In production, read from: spark.table(cfg.catalog.silver_preprocessed_fqn)

SPARK_SEED = 42

# -- Silver preprocessed: one row per (subject_id, session, epoch) --
silver_schema = StructType([
    StructField("subject_id",    StringType(),  False),
    StructField("session",       StringType(),  False),
    StructField("epoch_index",   IntegerType(), False),
    StructField("recording_date",DateType(),    True),
    StructField("sigma_power",   DoubleType(),  True),
    StructField("delta_power",   DoubleType(),  True),
    StructField("beta_power",    DoubleType(),  True),
    StructField("theta_power",   DoubleType(),  True),
])

from datetime import date
silver_rows = [
    ("SC4001", "night1", i, date(2024, 1, 10),
     0.3 + i*0.01, 0.5 + i*0.005, 0.1, 0.2)
    for i in range(20)
] + [
    ("SC4002", "night1", i, date(2024, 1, 11),
     0.6 + i*0.01, 0.4, 0.15, 0.25)
    for i in range(20)
]
df_silver = spark.createDataFrame(silver_rows, schema=silver_schema)
print(f"Silver rows: {df_silver.count()}")
df_silver.show(3)

# COMMAND ----------
# -- Silver events: one row per event (spindles + SOs) --
event_schema = StructType([
    StructField("subject_id",    StringType(),  False),
    StructField("session",       StringType(),  False),
    StructField("epoch_index",   IntegerType(), False),
    StructField("event_type",    StringType(),  False),
    StructField("duration_s",    DoubleType(),  True),
    StructField("amplitude_uv",  DoubleType(),  True),
    StructField("neg_peak_uv",   DoubleType(),  True),
    StructField("channel",       StringType(),  True),
])

import random
random.seed(SPARK_SEED)
events = []
for subj, sess, n_spindles, n_sos in [
    ("SC4001", "night1", 15, 8),
    ("SC4002", "night1", 6,  12),
]:
    for i in range(n_spindles):
        events.append((subj, sess, i % 20, "spindle",
                       random.uniform(0.5, 1.5),
                       random.uniform(30, 80), None, "Fz"))
    for i in range(n_sos):
        events.append((subj, sess, i % 20, "slow_oscillation",
                       random.uniform(0.8, 1.2), None,
                       random.uniform(-120, -60), "Fz"))

df_events = spark.createDataFrame(events, schema=event_schema)
print(f"Event rows: {df_events.count()}")
df_events.groupBy("subject_id", "event_type").count().show()

# COMMAND ----------
# %md ### Step 2: Build Gold Feature Table

df_gold = build_feature_table(df_silver, df_events)
print(f"Gold rows: {df_gold.count()}, columns: {df_gold.columns}")
df_gold.show(truncate=False)

# COMMAND ----------
# %md ### Step 3: Write to Delta Table
# EXAM NOTE: .saveAsTable() registers in the metastore.
# Use Unity Catalog FQN: catalog.schema.table

GOLD_TABLE = "eeg_lakehouse.gold.eeg_features"

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("recording_date")     # <-- partition for date-range queries
    .saveAsTable(GOLD_TABLE)
)
print(f"Written: {GOLD_TABLE}")

# COMMAND ----------
# %md ### Step 4: OPTIMIZE + ZORDER BY
# EXAM NOTE:
# - OPTIMIZE compacts small files into larger ones (reduces file listing overhead)
# - ZORDER BY co-locates related data -> data skipping on subject_id filter queries
# - Run after large writes; schedule nightly in prod
# - ZORDER != partitioning: partitioning is coarse (by date), ZORDER is fine

spark.sql(f"""
    OPTIMIZE {GOLD_TABLE}
    ZORDER BY (subject_id, recording_date)
""")
print("OPTIMIZE + ZORDER complete")

# COMMAND ----------
# %md ### Step 5: DESCRIBE DETAIL and DESCRIBE HISTORY
# EXAM NOTE:
# - DESCRIBE DETAIL: physical metadata (numFiles, sizeInBytes, partitionColumns)
# - DESCRIBE HISTORY: version log (operation, operationParameters, timestamp)
# - Both are Delta-specific; not available on Parquet

print("=== DESCRIBE DETAIL ===")
spark.sql(f"DESCRIBE DETAIL {GOLD_TABLE}").show(1, truncate=False)

print("=== DESCRIBE HISTORY ===")
spark.sql(f"DESCRIBE HISTORY {GOLD_TABLE}").show(5, truncate=False)

# COMMAND ----------
# %md ### Step 6: Time Travel
# EXAM NOTE:
# - VERSION AS OF <n>  queries historical snapshot at version n
# - TIMESTAMP AS OF '2024-01-01' queries by timestamp
# - Useful for: auditing, rollback, ML reproducibility
# - VACUUM removes old files (default 7-day retention)

print("=== TIME TRAVEL: VERSION AS OF 0 ===")
spark.sql(f"SELECT * FROM {GOLD_TABLE} VERSION AS OF 0").show()

# --- Preview VACUUM (don't actually run in demo) ---
# EXAM NOTE: VACUUM removes Delta history files older than retention threshold
# Default retention: 168 hours (7 days)
# VACUUM eeg_lakehouse.gold.eeg_features RETAIN 168 HOURS
# DRY RUN first!
print("VACUUM DRY RUN:")
spark.sql(f"VACUUM {GOLD_TABLE} RETAIN 168 HOURS DRY RUN").show()

# COMMAND ----------
# %md ### Step 7: Feature Summary Statistics
# Validate feature distributions before feeding to ML (Day 10)

print("=== Gold Feature Statistics ===")
spark.sql(f"""
    SELECT
        count(*) as n_subjects,
        round(avg(spindle_density), 3)        as avg_spindle_density,
        round(avg(so_density), 3)             as avg_so_density,
        round(avg(pac_mi), 3)                 as avg_pac_mi,
        round(avg(sigma_power_mean), 4)       as avg_sigma_power,
        sum(memory_score)                     as high_memory_count
    FROM {GOLD_TABLE}
""").show()

# COMMAND ----------
# %md
# ## Summary: Day 6 Exam Patterns
# | Pattern | Command | When to use |
# |---------|---------|-------------|
# | Compact files | OPTIMIZE table | After large appends |
# | Query skipping | ZORDER BY cols | Frequent filter columns |
# | Physical metadata | DESCRIBE DETAIL | Debug file count/size |
# | Audit trail | DESCRIBE HISTORY | Compliance, rollback |
# | Point-in-time query | VERSION AS OF N | ML reproducibility |
# | Safe cleanup | VACUUM RETAIN N HOURS | Cost management |
# | Idempotent upsert | MERGE INTO | Production Gold writes |

print("Day 6 complete! Proceed to Day 7: DLT pipeline skeleton.")
