# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5: Silver Event Detection — Spindles, SOs, Nested Structs
# MAGIC
# MAGIC **Learning objectives:**
# MAGIC - Detect spindle and SO events using `mapInPandas`
# MAGIC - Use `explode()` to flatten nested event arrays
# MAGIC - Aggregate events per subject/stage (spindle density)
# MAGIC - Understand StructType / ArrayType for event modeling
# MAGIC
# MAGIC **Exam domains:** Nested data types, `explode()`, window aggregations (Domain 2)

# COMMAND ----------

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.silver.detect_events import (
    detect_spindles, detect_slow_oscillations, compute_spindle_density,
    SILVER_SPINDLE_SCHEMA, SILVER_SO_SCHEMA
)
from pyspark.sql import functions as F
from pyspark.sql.types import *

cfg = AppConfig()
SILVER_TABLE = cfg.catalog.silver_epochs_fqn
print(f"Silver table: {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Nested Data Type Patterns (Exam Critical)
# MAGIC
# MAGIC ### ArrayType + StructType: model EEG events as embedded objects

# COMMAND ----------

# Demonstrate nested struct creation and explode
from pyspark.sql.types import ArrayType, StructType, StructField, FloatType, StringType

# Create sample DataFrame with spindle arrays
sample_data = [
    ("SC4001", 0, "N2", [
        {"start_sec": 5.2, "duration_sec": 1.3, "channel": "Fpz-Cz"},
        {"start_sec": 15.8, "duration_sec": 0.8, "channel": "Fpz-Cz"},
    ]),
    ("SC4001", 1, "N2", [
        {"start_sec": 35.1, "duration_sec": 2.1, "channel": "Pz-Oz"},
    ]),
    ("SC4002", 0, "N3", []),  # Epoch with no spindles
]

spindle_array_schema = StructType([
    StructField("subject_id", StringType()),
    StructField("epoch_idx",  __import__('pyspark.sql.types', fromlist=['IntegerType']).IntegerType()),
    StructField("sleep_stage",StringType()),
    StructField("spindles", ArrayType(StructType([
        StructField("start_sec",     FloatType()),
        StructField("duration_sec",  FloatType()),
        StructField("channel",       StringType()),
    ])))
])

# Note: F.array() and F.struct() can build these dynamically

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. explode() vs posexplode() vs inline()
# MAGIC
# MAGIC | Function | Use case |
# MAGIC |----------|----------|
# MAGIC | `F.explode(col)` | Flatten array into rows; null arrays produce no rows |
# MAGIC | `F.explode_outer(col)` | Like explode but null arrays produce one null row |
# MAGIC | `F.posexplode(col)` | Like explode + adds position index column |
# MAGIC | `F.inline(col)` | Flatten array of structs into separate columns |

# COMMAND ----------

# Demonstrate explode on spindle arrays (using simple test data)
test_rows = [
    ("SC4001", 0, "N2", [{"start_sec": 5.2, "duration_sec": 1.3}]),
    ("SC4001", 1, "N2", [{"start_sec": 35.1, "duration_sec": 2.1},
                          {"start_sec": 45.0, "duration_sec": 0.9}]),
    ("SC4002", 0, "N3", []),  # empty array
]

df_nested = spark.createDataFrame(test_rows,
    "subject_id STRING, epoch_idx INT, sleep_stage STRING, spindles ARRAY<STRUCT<start_sec:FLOAT, duration_sec:FLOAT>>")

# explode: drops rows with empty arrays
df_nested.select(
    "subject_id", "epoch_idx", "sleep_stage",
    F.explode("spindles").alias("spindle")
).select(
    "subject_id", "epoch_idx", "sleep_stage",
    F.col("spindle.start_sec"),
    F.col("spindle.duration_sec")
).show()

# explode_outer: keeps rows with empty arrays (SC4002 appears)
print("With explode_outer (keeps empty rows):")
df_nested.select(
    "subject_id",
    F.explode_outer("spindles").alias("spindle")
).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Run Full Event Detection Pipeline (on Databricks)

# COMMAND ----------

# silver_df = spark.table(cfg.catalog.silver_epochs_fqn)
# n2_n3_df = silver_df.filter(F.col("sleep_stage").isin(["N2", "N3"]))
#
# # Detect spindles
# spindle_df = detect_spindles(n2_n3_df)
# spindle_df.write.format("delta").mode("overwrite").saveAsTable(cfg.catalog.silver_spindles_fqn)
#
# # Detect SOs
# so_df = detect_slow_oscillations(n2_n3_df)
# so_df.write.format("delta").mode("overwrite").saveAsTable(cfg.catalog.silver_so_fqn)
#
# # Spindle density summary
# density_df = compute_spindle_density(spindle_df)
# density_df.orderBy("spindle_density_per_min", ascending=False).show(20)

print("Day 5 complete. Key exam patterns: explode, posexplode, inline, StructType, ArrayType")
