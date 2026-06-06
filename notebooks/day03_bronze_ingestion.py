# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3: Bronze Ingestion with Auto Loader Patterns
# MAGIC
# MAGIC **Learning objectives:**
# MAGIC - Run Auto Loader ingestion from a UC Volume
# MAGIC - Inspect Delta transaction history with `DESCRIBE HISTORY`
# MAGIC - Understand trigger types: `availableNow` vs `processingTime` vs `once`
# MAGIC - Verify Bronze table quality with assertions
# MAGIC
# MAGIC **Exam domains:** Auto Loader (Domain 3), Delta audit logging (Domain 1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import create_bronze_table, load_raw_files
from src.bronze.ingest_metadata import create_bronze_metadata_table

cfg = AppConfig()
print("Target Bronze table:", cfg.catalog.bronze_edf_fqn)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Create Unity Catalog structures (run once)

# COMMAND ----------

# Uncomment and run on a Databricks cluster with UC enabled:

# spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog.catalog}")
# spark.sql(f"USE CATALOG {cfg.catalog.catalog}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.bronze_schema}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.silver_schema}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.gold_schema}")
# spark.sql("""
#   CREATE VOLUME IF NOT EXISTS eeg_lakehouse.bronze.raw_edf
#   COMMENT 'Raw EDF files from PhysioNet Sleep-EDF Expanded'
# """)
# print("UC structures created.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Run Bronze Ingestion
# MAGIC
# MAGIC Auto Loader with `trigger(availableNow=True)` processes all pending files
# MAGIC in micro-batches, then terminates. This is the production pattern for
# MAGIC nightly batch EEG ingestion.

# COMMAND ----------

# Uncomment to run on Databricks:
# create_bronze_table(spark, cfg, trigger_once=True)
# print("Bronze ingestion complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Inspect Delta Transaction History
# MAGIC
# MAGIC `DESCRIBE HISTORY` is a core exam topic. Know what each operation means.

# COMMAND ----------

# spark.sql(f"""
#   DESCRIBE HISTORY {cfg.catalog.bronze_edf_fqn}
# """).select(
#     "version", "timestamp", "operation", "operationMetrics"
# ).show(10, truncate=False)

# Expected output:
# +-------+-------------------+----------------+------------------------------------------+
# |version|timestamp          |operation       |operationMetrics                          |
# +-------+-------------------+----------------+------------------------------------------+
# |1      |2026-06-01 10:30:00|STREAMING UPDATE|{numOutputRows: 400, numFiles: 2}         |
# |0      |2026-06-01 10:28:00|CREATE TABLE    |{}                                        |
# +-------+-------------------+----------------+------------------------------------------+

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Query Bronze Table

# COMMAND ----------

# spark.sql(f"""
#   SELECT
#     subject_id,
#     recording_type,
#     study_night,
#     is_hypnogram,
#     file_size_bytes,
#     ingestion_timestamp
#   FROM {cfg.catalog.bronze_edf_fqn}
#   ORDER BY subject_id, study_night
#   LIMIT 20
# """).show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Data Quality Check
# MAGIC
# MAGIC Before promoting to Silver, verify Bronze integrity.

# COMMAND ----------

# bronze_df = spark.table(cfg.catalog.bronze_edf_fqn)
#
# null_subjects = bronze_df.filter(F.col("subject_id").isNull()).count()
# total = bronze_df.count()
# psg_count = bronze_df.filter(~F.col("is_hypnogram")).count()
# hyp_count = bronze_df.filter(F.col("is_hypnogram")).count()
#
# print(f"Total files: {total}")
# print(f"PSG files: {psg_count}")
# print(f"Hypnogram files: {hyp_count}")
# print(f"Null subject IDs: {null_subjects}")
# assert null_subjects == 0, "FAIL: null subject_ids found in Bronze"
# print("PASS: All quality checks passed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Exam Quick Reference: Trigger Types
# MAGIC
# MAGIC | Trigger | Code | Use case |
# MAGIC |---------|------|----------|
# MAGIC | `availableNow` | `.trigger(availableNow=True)` | Process all pending, stop. Best for batch EEG |
# MAGIC | `once` | `.trigger(once=True)` | Legacy equivalent of availableNow (single micro-batch) |
# MAGIC | `processingTime` | `.trigger(processingTime='5 min')` | Continuous micro-batch |
# MAGIC | `continuous` | `.trigger(continuous='1 sec')` | Ultra-low latency, experimental |

print("Day 3 notebook ready. Uncomment cells to run on Databricks.")
