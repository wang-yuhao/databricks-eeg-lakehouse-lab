# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4: Silver Preprocessing — EEG Cleaning with Pandas UDFs
# MAGIC
# MAGIC **Exam domain:** ELT with PySpark — Pandas UDFs (Arrow-based), transformation pipelines
# MAGIC **Research step:** Bandpass filter + artifact flagging scaffold; MNE plugs in here
# MAGIC
# MAGIC ## Learning objectives
# MAGIC - `@pandas_udf` vs Python UDF vs `mapInPandas` — know when to use each
# MAGIC - Arrow optimization: Pandas UDF transfers data as Apache Arrow batches (no row serialization)
# MAGIC - Schema enforcement: Silver table must match expected output schema

# COMMAND ----------

import sys, os, tempfile
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

try:
    _ = spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName("Day04-Silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
import numpy as np

tmp_dir = tempfile.mkdtemp()

# COMMAND ----------
# MAGIC %md ## 1. Create Synthetic Bronze Epoch Data
# MAGIC (In production: read from `eeg_lakehouse.bronze.raw_eeg_files` + load EDF via path)

# COMMAND ----------

import numpy as np
from pyspark.sql.types import *

# Simulate 5 subjects × 3 epochs each, each epoch = 30-sec at 100 Hz = 3000 samples
np.random.seed(42)
rows = []
for subj in ["SC4001", "SC4002", "SC4003", "SC4004", "SC4005"]:
    for epoch_idx in range(3):
        # Simulate 2-channel EEG: list of 3000 floats each
        ch1 = np.random.randn(3000).tolist()
        ch2 = np.random.randn(3000).tolist()
        rows.append((
            subj, f"E{epoch_idx}", epoch_idx,
            ch1, ch2, 100.0,        # sample_rate
            "N2" if epoch_idx < 2 else "N3",  # sleep_stage
            False                   # has_artifact (ground truth for testing)
        ))

epoch_schema = StructType([
    StructField("subject_id",    StringType(),   False),
    StructField("session_id",    StringType(),   False),
    StructField("epoch_index",   IntegerType(),  False),
    StructField("channel_fpzcz", ArrayType(DoubleType()), True),  # Fpz-Cz signal
    StructField("channel_pzoz",  ArrayType(DoubleType()), True),  # Pz-Oz signal
    StructField("sample_rate",   DoubleType(),   True),
    StructField("sleep_stage",   StringType(),   True),
    StructField("has_artifact",  BooleanType(),  True),
])

df_epochs_raw = spark.createDataFrame(rows, schema=epoch_schema)
print(f"Bronze epochs: {df_epochs_raw.count()} rows")
df_epochs_raw.show(5, truncate=40)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Pandas UDF for EEG Preprocessing
# MAGIC
# MAGIC `@pandas_udf` uses Apache Arrow for zero-copy data transfer between
# MAGIC JVM and Python. Compared to row-wise Python UDFs, this is ~10-100x faster
# MAGIC for numerical array processing.
# MAGIC
# MAGIC Exam key points:
# MAGIC - Must declare return type explicitly
# MAGIC - Input/output are `pd.Series` (scalar UDF) or `pd.DataFrame` (grouped map)
# MAGIC - `mapInPandas` is better for group-level operations (no aggregation)

# COMMAND ----------

from src.silver.preprocess_eeg import (
    compute_signal_stats,
    flag_artifact_epochs,
    preprocess_eeg_batch,
)

# Apply preprocessing
df_silver = preprocess_eeg_batch(df_epochs_raw)
print("Silver preprocessed epochs:")
df_silver.printSchema()
df_silver.show(5, truncate=50)

# COMMAND ----------
# MAGIC %md ## 3. Write Silver Delta Table

# COMMAND ----------

silver_path = os.path.join(tmp_dir, "silver_epochs")
(
    df_silver.write.format("delta")
    .partitionBy("subject_id")  # Partition by subject for efficient per-subject queries
    .mode("overwrite")
    .save(silver_path)
)
print(f"Silver table written to: {silver_path}")
df_s = spark.read.format("delta").load(silver_path)
df_s.groupBy("subject_id", "sleep_stage").count().show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Exam Concept: When to Use Which UDF Type
# MAGIC
# MAGIC | Type | Use Case | Performance | Exam tip |
# MAGIC |------|----------|-------------|----------|
# MAGIC | Python UDF | Simple scalar ops on small data | Slowest (row serialization) | Default choice — avoid for signal arrays |
# MAGIC | `@pandas_udf` (scalar) | Vectorized operations on columns | Fast (Arrow batches) | Best for column-level math |
# MAGIC | `@pandas_udf` (grouped_agg) | Aggregations returnin