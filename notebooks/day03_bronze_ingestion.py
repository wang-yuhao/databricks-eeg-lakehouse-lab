# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3: Bronze Ingestion with Databricks Patterns
# MAGIC
# MAGIC **Learning objectives:**
# MAGIC - Run the Bronze ingestion pipeline (batch mode for local dev)
# MAGIC - Use `DESCRIBE HISTORY` to verify Delta audit log
# MAGIC - Run `DESCRIBE DETAIL` to see file statistics
# MAGIC - Write and run Bronze unit tests
# MAGIC
# MAGIC **Exam domains:** Delta Lake basics, Auto Loader, audit history
# MAGIC **Research:** Establishes the EDF file registry as the foundation for all downstream processing

# COMMAND ----------

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

try:
    _ = spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[*]")
             .appName("day03")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Write Synthetic Bronze Table (local Delta path)

# COMMAND ----------

from pyspark.sql import functions as F
import tempfile, os

# For local dev, write to a temp Delta path (on Databricks: use Unity Catalog table)
DELTA_PATH = "/tmp/eeg_bronze_edf"   # Change to UC table name on Databricks

synth_data = [
    ("SC4001", 0, "PSG",       "/data/SC4001E0-PSG.edf",       "SC4001E0-PSG.edf",       130_000_000, "sleep-edf"),
    ("SC4001", 1, "PSG",       "/data/SC4001E1-PSG.edf",       "SC4001E1-PSG.edf",       128_500_000, "sleep-edf"),
    ("SC4001", 0, "Hypnogram", "/data/SC4001EC-Hypnogram.edf", "SC4001EC-Hypnogram.edf",      1_200, "sleep-edf"),
    ("SC4002", 0, "PSG",       "/data/SC4002E0-PSG.edf",       "SC4002E0-PSG.edf",       131_000_000, "sleep-edf"),
    ("SC4002", 0, "Hypnogram", "/data/SC4002EC-Hypnogram.edf", "SC4002EC-Hypnogram.edf",      1_300, "sleep-edf"),
]
cols = ["subject_id", "night_index", "file_type", "file_path", "file_name",
        "file_size_bytes", "dataset_source"]

df = (
    spark.createDataFrame(synth_data, cols)
    .withColumn("ingestion_ts", F.current_timestamp())
)

# Write v1 of the Delta table
df.write.format("delta").mode("overwrite").save(DELTA_PATH)
print(f"Written {df.count()} rows to {DELTA_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read and Verify Bronze Table

# COMMAND ----------

df_bronze = spark.read.format("delta").load(DELTA_PATH)
df_bronze.show(truncate=False)
print(f"Row count: {df_bronze.count()}")
print(f"Subjects: {df_bronze.select('subject_id').distinct().count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Delta DESCRIBE HISTORY
# MAGIC
# MAGIC `DESCRIBE HISTORY` is a Delta-specific command that returns the audit log
# MAGIC of all operations on a table. Each write operation creates a new version.
# MAGIC Exam: know the columns (version, timestamp, operation, operationParameters).

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, DELTA_PATH)
history_df = dt.history()
history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Simulate a Second Ingestion (Append)
# MAGIC
# MAGIC Append new records to simulate Auto Loader picking up new files.

# COMMAND ----------

new_files = [
    ("SC4011", 0, "PSG", "/data/SC4011E0-PSG.edf", "SC4011E0-PSG.edf", 129_000_000, "sleep-edf"),
    ("SC4011", 0, "Hypnogram", "/data/SC4011EC-Hypnogram.edf", "SC4011EC-Hypnogram.edf", 1_100, "sleep-edf"),
]
df_new = (
    spark.createDataFrame(new_files, cols)
    .withColumn("ingestion_ts", F.current_timestamp())
)
df_new.write.format("delta").mode("append").save(DELTA_PATH)

# History now shows version 1 (append)
dt.history().select("version", "timestamp", "operation").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Time Travel — Read Previous Version
# MAGIC
# MAGIC **Exam:** Delta time travel uses `VERSION AS OF` or `TIMESTAMP AS OF`.
# MAGIC This is critical for audit, rollback, and reproducible ML experiments.

# COMMAND ----------

# Read original version (before append)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH)
print(f"Version 0 rows: {df_v0.count()}")   # Should be 5

df_v1 = spark.read.format("delta").load(DELTA_PATH)  # Latest
print(f"Latest rows: {df_v1.count()}")       # Should be 7

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. DESCRIBE DETAIL
# MAGIC
# MAGIC Returns storage metadata: number of files, size, partitioning, format.
# MAGIC Exam: Used to verify OPTIMIZE ran, check numFiles, sizeInBytes.

# COMMAND ----------

detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{DELTA_PATH}`")
detail_df.select("format", "numFiles", "sizeInBytes", "partitionColumns").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Bronze Data Quality Checks
# MAGIC
# MAGIC Simple assertions that would fail if ingestion had issues.
# MAGIC These same checks become DLT expectations in Day 7.

# COMMAND ----------

df_final = spark.read.format("delta").load(DELTA_PATH)
null_subjects = df_final.filter(F.col("subject_id").isNull()).count()
print(f"Null subject_id count: {null_subjects}")
assert null_subjects == 0, "Bronze invariant violated: subject_id must not be null"

unknown_types = df_final.filter(~F.col("file_type").isin(["PSG", "Hypnogram"])).count()
print(f"Unknown file_type count: {unknown_types}")

print("\n✅ All Bronze quality checks passed.")

# COMMAND ----------
print("Day 3 complete. Next: Day 4 - Silver preprocessing with Pandas UDFs")
