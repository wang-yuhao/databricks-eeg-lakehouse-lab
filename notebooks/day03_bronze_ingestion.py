# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# dependencies = [
#   "loguru",
# ]
# ///
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

# MAGIC %pip install loguru

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

# DBTITLE 1,Cell 6
# Uncomment and run on a Databricks cluster with UC enabled:

catalog_exists = spark.sql(f"SHOW CATALOGS LIKE '{cfg.catalog.catalog}'").count() > 0
assert catalog_exists, f"Catalog {cfg.catalog.catalog} does not exist. Create it with a managed location or use an existing catalog."
spark.sql(f"USE CATALOG {cfg.catalog.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.gold_schema}")
spark.sql("""
  CREATE VOLUME IF NOT EXISTS eeg_lakehouse.bronze.raw_edf
  COMMENT 'Raw EDF files from PhysioNet Sleep-EDF Expanded'
""")
print("UC structures created.")

# COMMAND ----------

# DBTITLE 1,Cell 7
# Step 3 — Download EDF sample directly into the UC Volume (no /tmp, no dbutils.fs.cp)

import os
import requests

# 1. Target Unity Catalog Volume path (as used in cfg.paths.volume_edf_dir) [cite:1]
volume_path = "/Volumes/eeg_lakehouse/bronze/raw_edf/"
print(f"Target UC Volume path: {volume_path}")

# 2. Correct PhysioNet base URL for cassette files [web:80]
base_url = "https://physionet.org/physiobank/database/sleep-edfx/sleep-cassette/"

# 3. Robust sample (3 subjects, 6 files)
sample_files = [
    "SC4001E0-PSG.edf", "SC4001EC-Hypnogram.edf",
    "SC4002E0-PSG.edf", "SC4002EC-Hypnogram.edf",
    "SC4041E0-PSG.edf", "SC4041EC-Hypnogram.edf",
]

print(f"Number of EDF files to download: {len(sample_files)}")
for f in sample_files:
    print(f"  - {f}")

# 4. Download each file directly into the UC Volume
for fname in sample_files:
    dest_path = os.path.join(volume_path, fname)
    if os.path.exists(dest_path):
        print(f"Skipping {fname}, already exists at {dest_path}")
        continue

    url = f"{base_url}{fname}"
    print(f"Downloading {url} -> {dest_path}")

    try:
        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()
        # Write directly to the Volume path
        with open(dest_path, "wb") as out:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
        print(f"  Done: {fname} ({os.path.getsize(dest_path):,} bytes)")
    except Exception as e:
        print(f"  ERROR downloading {fname}: {e}")

# 5. Verify files in UC Volume (via dbutils.fs.ls, which is allowed on Volumes) [web:35]
print("Verifying files in UC Volume...")
files_in_volume = dbutils.fs.ls(volume_path)
print(f"Files found in volume: {len(files_in_volume)}")
for f in files_in_volume:
    print(f"  {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# Step 3C — Download Sleep-EDFx metadata and index files to the UC Volume

import os
import requests

# 1. Target UC Volume path (matches cfg.paths.volume_edf_dir)
volume_path = "/Volumes/eeg_lakehouse/bronze/raw_edf/"
print(f"Target UC Volume path for metadata: {volume_path}")

# 2. Base URL for top-level Sleep-EDFx 1.0.0 files
base_url = "https://physionet.org/files/sleep-edfx/1.0.0/"

# 3. Files to download from the index you provided
metadata_files = [
    "RECORDS",
    "RECORDS-v1",
    "SC-subjects.xls",
    "ST-subjects.xls",
    "SHA256SUMS.txt",
]

print(f"Number of metadata/index files to download: {len(metadata_files)}")
for f in metadata_files:
    print(f"  - {f}")

# 4. Download each file directly into the UC Volume
for fname in metadata_files:
    dest_path = os.path.join(volume_path, fname)

    if os.path.exists(dest_path):
        print(f"Skipping {fname}, already exists at {dest_path}")
        continue

    url = f"{base_url}{fname}"
    print(f"Downloading {url} -> {dest_path}")

    try:
        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()

        # Write directly to the Volume path (driver file system view of /Volumes)
        with open(dest_path, "wb") as out:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)

        print(f"  Done: {fname} ({os.path.getsize(dest_path):,} bytes)")
    except Exception as e:
        print(f"  ERROR downloading {fname}: {e}")

# 5. Verify metadata files in the UC Volume via dbutils.fs.ls
print("Verifying metadata files in UC Volume...")
files_in_volume = dbutils.fs.ls(volume_path)
print(f"Files found in volume: {len(files_in_volume)}")
for f in files_in_volume:
    # Only show the files we expect from Step 3C
    if f.name in metadata_files:
        print(f"  {f.name} ({f.size:,} bytes)")

# COMMAND ----------

files = dbutils.fs.ls("/Volumes/eeg_lakehouse/bronze/raw_edf/")
print(f"Files found: {len(files)}")
for f in files[:5]:
    print(f"  {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run Bronze Ingestion
# MAGIC
# MAGIC Auto Loader with `trigger(availableNow=True)` processes all pending files
# MAGIC in micro-batches, then terminates. This is the production pattern for
# MAGIC nightly batch EEG ingestion.

# COMMAND ----------

# DBTITLE 1,Cell 10
# Uncomment to run on Databricks:
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog.catalog}.{cfg.catalog.bronze_schema}.`_schema`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog.catalog}.{cfg.catalog.bronze_schema}.`_checkpoints`")
create_bronze_table(spark, cfg, trigger_once=True)
print("Bronze ingestion complete.")

# COMMAND ----------

# MAGIC %sql SHOW VOLUMES

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect Delta Transaction History
# MAGIC
# MAGIC `DESCRIBE HISTORY` is a core exam topic. Know what each operation means.

# COMMAND ----------

spark.sql(f"""
  DESCRIBE HISTORY {cfg.catalog.bronze_edf_fqn}
""").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(10, truncate=False)

# Expected output:
# +-------+-------------------+----------------+------------------------------------------+
# |version|timestamp          |operation       |operationMetrics                          |
# +-------+-------------------+----------------+------------------------------------------+
# |1      |2026-06-01 10:30:00|STREAMING UPDATE|{numOutputRows: 400, numFiles: 2}         |
# |0      |2026-06-01 10:28:00|CREATE TABLE    |{}                                        |
# +-------+-------------------+----------------+------------------------------------------+

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   version,
# MAGIC   timestamp,
# MAGIC   userName,
# MAGIC   operation,
# MAGIC   operationMetrics.numDeletedRows AS rows_deleted
# MAGIC FROM (DESCRIBE HISTORY eeg_lakehouse.bronze.raw_eeg_files)
# MAGIC WHERE operation = 'STREAMING UPDATE'
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Query Bronze Table

# COMMAND ----------

spark.sql(f"""
  SELECT
    subject_id,
    recording_type,
    study_night,
    is_hypnogram,
    file_size_bytes,
    ingestion_timestamp
  FROM {cfg.catalog.bronze_edf_fqn}
  ORDER BY subject_id, study_night
  LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality Check
# MAGIC
# MAGIC Before promoting to Silver, verify Bronze integrity.

# COMMAND ----------

# DBTITLE 1,Cell 19
from pyspark.sql import functions as F

bronze_df = spark.table(cfg.catalog.bronze_edf_fqn)

null_subjects = bronze_df.filter(F.col("subject_id").isNull()).count()
total = bronze_df.count()
psg_count = bronze_df.filter(~F.col("is_hypnogram")).count()
hyp_count = bronze_df.filter(F.col("is_hypnogram")).count()

print(f"Total files: {total}")
print(f"PSG files: {psg_count}")
print(f"Hypnogram files: {hyp_count}")
print(f"Null subject IDs: {null_subjects}")
assert null_subjects == 0, "FAIL: null subject_ids found in Bronze"
print("PASS: All quality checks passed.")

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
# MAGIC
# MAGIC print("Day 3 notebook ready. Uncomment cells to run on Databricks.")

# COMMAND ----------

count_before = spark.table(cfg.catalog.bronze_edf_fqn).count()

create_bronze_table(spark, cfg, trigger_once=True)

count_after = spark.table(cfg.catalog.bronze_edf_fqn).count()

print(f"Row count before re-run : {count_before}")
print(f"Row count after re-run  : {count_after}")
assert count_before == count_after, (
    f"FAIL: Duplicate records created. Before={count_before}, After={count_after}"
)
print("PASS: Idempotency verified — no duplicates created.")

# COMMAND ----------

# DBTITLE 1,Cell 22
from src.bronze.ingest_metadata import create_bronze_metadata_table

# The sleep-edfx dataset downloaded in Step 3 includes SC-subjects.xls
# which wfdb converts to a CSV alongside the EDF files in the same directory.
# After Step 3's dbutils.fs.cp, all companion files land in volume_edf_dir.
metadata_csv_path = f"{cfg.paths.volume_edf_dir}/SC-subjects.xls"

# Pass csv_path as the required second positional argument (cfg is optional/third).
create_bronze_metadata_table(spark, metadata_csv_path, cfg, file_type="auto")

spark.table(cfg.catalog.bronze_metadata_fqn).show(5)

# COMMAND ----------


