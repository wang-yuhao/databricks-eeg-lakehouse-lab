# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# dependencies = [
#   "loguru",
# ]
# ///
# MAGIC %md
# MAGIC # Day 2: Dataset Interface & Bronze Schema Design
# MAGIC
# MAGIC **Learning objectives:**
# MAGIC - Understand the Sleep-EDF EDF file format and naming convention
# MAGIC - Design explicit Bronze schemas (exam: know why explicit > inferSchema)
# MAGIC - Preview Auto Loader options for incremental EDF ingestion
# MAGIC - Apply `extract_subject_id` logic and verify it works
# MAGIC
# MAGIC **Exam domains:** Auto Loader (Domain 3), Delta schema enforcement (Domain 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore Config

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install loguru

# COMMAND ----------

# DBTITLE 1,Cell 3
import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import (
    BRONZE_EDF_SCHEMA,
    extract_subject_id,
    extract_study_night,
    is_hypnogram_file,
)

cfg = AppConfig()
print("Catalog:", cfg.catalog.catalog)
print("Bronze EDF FQN:", cfg.catalog.bronze_edf_fqn)
print("Bronze Metadata FQN:", cfg.catalog.bronze_metadata_fqn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify filename parsing logic

# COMMAND ----------

test_files = [
    "SC4001E0-PSG.edf",
    "SC4001EC-Hypnogram.edf",
    "ST7011J0-PSG.edf",
    "ST7011JC-Hypnogram.edf",
    "SC4002E0-PSG.edf",
]

print(f"{'Filename':<30} {'subject_id':<12} {'night':<8} {'hypnogram'}")
print("-" * 65)
for f in test_files:
    sid = extract_subject_id(f)
    night = extract_study_night(f)
    hyp = is_hypnogram_file(f)
    print(f"{f:<30} {str(sid):<12} {str(night):<8} {hyp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect Bronze Delta schema

# COMMAND ----------

from pyspark.sql.types import StructType
import json

print("Bronze EDF test Schema:")
for field in BRONZE_EDF_SCHEMA.fields:
    nullable = "nullable" if field.nullable else "NOT NULL"
    print(f"  {field.name:<35} {str(field.dataType):<20} {nullable}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auto Loader options reference
# MAGIC
# MAGIC | Option | Value | Meaning |
# MAGIC |--------|-------|---------|
# MAGIC | `cloudFiles.format` | `binaryFile` | Read raw EDF bytes (not parsed CSV/JSON) |
# MAGIC | `cloudFiles.schemaLocation` | `/Volumes/.../schema` | Persist inferred schema between runs |
# MAGIC | `cloudFiles.useNotifications` | `false` | Directory listing (no cloud setup needed) |
# MAGIC | `cloudFiles.includeExistingFiles` | `true` | Process pre-existing files on first run |
# MAGIC | `pathGlobFilter` | `*.edf` | Only process EDF files |
# MAGIC
# MAGIC **COPY INTO vs Auto Loader:**
# MAGIC - **Auto Loader**: streaming, tracks new files automatically, scales to billions of files
# MAGIC - **COPY INTO**: idempotent batch, simpler, best for one-time or infrequent loads
# MAGIC - **Exam pitfall**: COPY INTO does NOT use cloudFiles format; Auto Loader does.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. (Optional) Create Bronze tables on Databricks
# MAGIC
# MAGIC Run these cells only when connected to a Databricks cluster with UC enabled.

# COMMAND ----------

# # Step 1: Create Unity Catalog structures
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog.catalog}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.catalog}.{cfg.catalog.bronze_schema}")
# spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog.catalog}.{cfg.catalog.bronze_schema}.raw_edf")

# COMMAND ----------

# # Step 2: Run Auto Loader ingestion (trigger once)
# from src.bronze.ingest_eeg_files import create_bronze_table
# create_bronze_table(spark, cfg, trigger_once=True)

# COMMAND ----------

# # Step 3: Inspect Bronze table
# spark.sql(f"DESCRIBE HISTORY {cfg.catalog.bronze_edf_fqn}").show(5, truncate=False)
# spark.sql(f"SELECT * FROM {cfg.catalog.bronze_edf_fqn} LIMIT 10").show(truncate=False)

print("Day 2 complete. Tomorrow: Auto Loader full run + Bronze tests.")

# COMMAND ----------


