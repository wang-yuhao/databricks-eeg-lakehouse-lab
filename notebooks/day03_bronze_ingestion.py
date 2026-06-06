# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3: Bronze Ingestion with Auto Loader Patterns
# MAGIC
# MAGIC **Exam domains:** Auto Loader, Delta basics, DESCRIBE HISTORY, schema enforcement
# MAGIC **Research step:** Idempotent EDF file registry in Bronze Delta table

# COMMAND ----------
# MAGIC %md ## 1. Setup

# COMMAND ----------

import sys, os, tempfile
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

try:
    _ = spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName("Day03-Bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import (
    load_raw_files_batch, transform_file_metadata, BRONZE_EEG_SCHEMA
)
from src.bronze.ingest_metadata import load_subject_metadata, write_metadata_bronze
from pyspark.sql import functions as F
from datetime import datetime

cfg = AppConfig()
tmp_dir = tempfile.mkdtemp()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Simulate File Ingestion (No actual EDF files needed)
# MAGIC Normally you'd call `create_bronze_table(spark, volume_path)` on Databricks.
# MAGIC Here we create a synthetic batch DataFrame matching the Auto Loader output.

# COMMAND ----------

# Simulate Auto Loader binaryFile output schema
sim_data = [
    (f"/data/SC{4000+i:04d}E0-PSG.edf", f"SC{4000+i:04d}E0-PSG.edf",
     2_400_000 + i * 1000, datetime(2026, 1, i+1, 20, 0, 0))
    for i in range(5)
] + [
    (f"/data/SC{4000+i:04d}EC-Hypnogram.edf", f"SC{4000+i:04d}EC-Hypnogram.edf",
     5000, datetime(2026, 1, i+1, 20, 0, 0))
    for i in range(5)
]

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
raw_schema = StructType([
    StructField("path",             StringType(),   False),
    StructField("name",             StringType(),   True),
    StructField("length",           LongType(),     True),
    StructField("modificationTime", TimestampType(),True),
])

df_raw_sim = spark.createDataFrame(sim_data, schema=raw_schema)
print("Simulated Auto Loader output:")
df_raw_sim.show()

# COMMAND ----------
# MAGIC %md ## 3. Apply Bronze Transform

# COMMAND ----------

df_bronze = transform_file_metadata(df_raw_sim)
print("Transformed Bronze table:")
df_bronze.show(truncate=60)
df_bronze.printSchema()

# Validate: no null subject_ids on PSG files
null_subjects = df_bronze.filter(
    (F.col("subject_id").isNull()) & (~F.col("is_hypnogram"))
).count()
print(f"\nNull subject_ids on PSG files: {null_subjects} (expect 0)")
assert null_subjects == 0, "Data quality check FAILED: null subject_ids found!"

# COMMAND ----------
# MAGIC %md ## 4. Write to Bronze Delta + Inspect

# COMMAND ----------

bronze_path = os.path.join(tmp_dir, "bronze_eeg")
(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)

df_b = spark.read.format("delta").load(bronze_path)
print(f"Bronze table row count: {df_b.count()}")
df_b.groupBy("is_hypnogram").count().show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. DESCRIBE HISTORY — Delta Audit Log
# MAGIC
# MAGIC **Exam:** `DESCRIBE HISTORY` returns all Delta operations.
# MAGIC Useful for time travel: `SELECT * FROM delta.\`path\` VERSION AS OF 0`

# COMMAND ----------

from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, bronze_path)
dt.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=100)

# Append 2 more subjects (simulating new files arriving)
more_data = [
    (f"/data/SC4005E0-PSG.edf", "SC4005E0-PSG.edf", 2_450_000, datetime(2026,1,6,20,0,0)),
    (f"/data/SC4006E0-PSG.edf", "SC4006E0-PSG.edf", 2_380_000, datetime(2026,1,7,20,0,0)),
]
df_new = spark.createDataFrame(more_data, schema=raw_schema)
df_new_bronze = transform_file_metadata(df_new)
df_new_bronze.write.format("delta").mode("append").save(bronze_path)

print("\nAfter appending 2 more subjects:")
dt.history().select("version", "timestamp", "operation").show()

# Time travel: read VERSION 0 (original state)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(bronze_path)
print(f"Version 0 row count: {df_v0.count()} (expect 10: 5 PSG + 5 Hypno)")
df_v1 = spark.read.format("delta").load(bronze_path)
print(f"Current row count: {df_v1.count()} (expect 12)")

# COMMAND ----------
# MAGIC %md ## 6. Exam Mini-Quiz
# MAGIC
# MAGIC Q1: What does `trigger(availableNow=True)` do in Auto Loader?
# MAGIC → Processes all files available at trigger time (multiple micro-batches), then stops.
# MAGIC    More efficient than `trigger(once=True)` which uses a single micro-batch.
# MAGIC
# MAGIC Q2: Where does Auto Loader track which files have been processed?
# MAGIC → In the `checkpointLocation`. It stores file notification state and offset progress.
# MAGIC    Delete checkpoint = restart from scratch (re-ingest all files).
# MAGIC
# MAGIC Q3: `overwriteSchema=True` vs `mergeSchema=True`?
# MAGIC → `overwriteSchema`: replaces existing schema (use for full refresh).
# MAGIC    `mergeSchema`: adds new columns to existing schema without dropping old ones.
print("Day 3 complete!")
