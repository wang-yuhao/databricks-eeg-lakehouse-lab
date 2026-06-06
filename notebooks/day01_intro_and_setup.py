# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1: Introduction and Setup
# MAGIC
# MAGIC **Goals for this notebook:**
# MAGIC 1. Verify the repo is correctly attached to Databricks Repos / Workspace
# MAGIC 2. Confirm Delta Lake is working (write a small test table)
# MAGIC 3. Explore the `PipelineConfig` object
# MAGIC 4. Discuss exam domain overview
# MAGIC
# MAGIC **Exam domains touched:** Lakehouse Platform, Unity Catalog (intro)
# MAGIC
# MAGIC **Research link:** Environment validation before any EEG data work

# COMMAND ----------

# MAGIC %md ## 1. Environment Check

# COMMAND ----------

import sys
print(f"Python: {sys.version}")

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print(f"Spark version: {spark.version}")
except Exception as e:
    print(f"Spark not available (local run): {e}")

try:
    import pyspark
    print(f"PySpark: {pyspark.__version__}")
except ImportError:
    print("PySpark not installed")

try:
    import delta
    print(f"Delta: {delta.__version__}")
except ImportError:
    print("Delta not installed")

try:
    import mne
    print(f"MNE: {mne.__version__}")
except ImportError:
    print("MNE not installed (install with: pip install mne)")

try:
    import yasa
    print(f"YASA: {yasa.__version__}")
except ImportError:
    print("YASA not installed (install with: pip install yasa)")

try:
    import ripser
    print(f"Ripser: installed")
except ImportError:
    print("Ripser not installed (install with: pip install ripser)")

# COMMAND ----------

# MAGIC %md ## 2. PipelineConfig
# MAGIC
# MAGIC `PipelineConfig` is the single source of truth for all paths, table names, and Spark settings.
# MAGIC Never hardcode catalog/schema names in pipeline code.

# COMMAND ----------

import sys, os
# Add repo root to path so we can import src.*
sys.path.insert(0, os.path.abspath("../"))

from src.utils.config import PipelineConfig, Env

cfg = PipelineConfig(env=Env.DEV)
print("=== PipelineConfig (DEV) ===")
print(f"  Bronze table 'eeg_raw':       {cfg.uc.bronze_table('eeg_raw')}")
print(f"  Silver table 'spindle_events': {cfg.uc.silver_table('spindle_events')}")
print(f"  Gold table 'eeg_gold_features': {cfg.uc.gold_table('eeg_gold_features')}")
print(f"  Raw EDF source:               {cfg.paths.raw_edf_source}")
print(f"  Checkpoint base:              {cfg.paths.checkpoint_base}")
print(f"  Shuffle partitions:           {cfg.spark.shuffle_partitions}")

# COMMAND ----------

# MAGIC %md ## 3. Delta Lake Smoke Test
# MAGIC
# MAGIC Write a tiny DataFrame to a Delta table and read it back.
# MAGIC Then demonstrate DESCRIBE HISTORY — a common exam question.

# COMMAND ----------

import tempfile, os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp
    import pyspark.sql.functions as F

    spark = SparkSession.builder.getOrCreate()

    tmp_path = "/tmp/day01_delta_smoke_test"

    # Write version 1
    (
        spark.range(5)
        .withColumn("subject_id", F.concat(F.lit("SC400"), F.col("id").cast("string")))
        .withColumn("created_at", current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .save(tmp_path)
    )
    print("✅ Delta write succeeded")

    # Read back
    df = spark.read.format("delta").load(tmp_path)
    df.show()

    # DESCRIBE HISTORY — exam favourite!
    # In Databricks SQL: DESCRIBE HISTORY delta.`/tmp/day01_delta_smoke_test`
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, tmp_path)
    dt.history().select("version", "timestamp", "operation").show()

except Exception as e:
    print(f"Spark/Delta not available in this environment: {e}")
    print("Run this notebook in Databricks for full functionality.")

# COMMAND ----------

# MAGIC %md ## 4. Exam Domain Map (Recap)
# MAGIC
# MAGIC | Domain | Weight | This Repo |
# MAGIC |--------|--------|----------|
# MAGIC | Lakehouse Platform | ~24% | `src/utils/config.py`, this notebook |
# MAGIC | ELT (Spark SQL/Python) | ~29% | `src/silver/`, `src/gold/` |
# MAGIC | Incremental Processing | ~22% | Auto Loader in `src/bronze/` |
# MAGIC | Production Pipelines (DLT) | ~16% | `@dlt.table` decorators — Day 7 |
# MAGIC | Data Governance (UC) | ~9% | `notebooks/day08_unity_catalog_setup.py` |
# MAGIC
# MAGIC **Next:** Day 2 — dataset interface + Bronze schema design.
