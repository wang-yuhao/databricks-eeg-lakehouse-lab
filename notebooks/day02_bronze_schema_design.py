# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2: Dataset Interface & Bronze Schema Design
# MAGIC
# MAGIC **Exam domain:** Auto Loader, Delta schema enforcement, managed vs external tables
# MAGIC **Research step:** Define Bronze data contract for Sleep-EDF EDF files
# MAGIC
# MAGIC ## Learning objectives
# MAGIC - Understand Auto Loader option flow: `cloudFiles.format`, `schemaLocation`, `mergeSchema`
# MAGIC - Define explicit Bronze schemas (vs `inferSchema`)
# MAGIC - Understand FQN naming in Unity Catalog

# COMMAND ----------
# MAGIC %md ## 1. Load Config and Review Bronze Schema

# COMMAND ----------

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import BRONZE_EEG_SCHEMA

cfg = AppConfig()

print("=== Unity Catalog FQNs ===")
print(f"Bronze EDF table: {cfg.catalog.bronze_edf_fqn}")
print(f"Bronze metadata table: {cfg.catalog.bronze_metadata_fqn}")
print(f"Silver epochs: {cfg.catalog.silver_epochs_fqn}")
print(f"Gold features: {cfg.catalog.gold_features_fqn}")

print("\n=== Bronze EEG Schema ===")
for field in BRONZE_EEG_SCHEMA.fields:
    print(f"  {field.name:30s} {str(field.dataType):20s} nullable={field.nullable}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Simulate Bronze Ingestion Locally
# MAGIC
# MAGIC Since we may not have EDF files yet, we simulate the Bronze output
# MAGIC with a small DataFrame matching the exact Bronze schema.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

try:
    _ = spark
except NameError:
    spark = (
        SparkSession.builder
        .appName("Day02-Bronze-Schema")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

# Simulated Bronze rows (2 subjects, 2 files each: PSG + Hypnogram)
sample_data = [
    ("/Volumes/eeg_lakehouse/bronze/raw_edf/SC4001E0-PSG.edf",
     "SC4001E0-PSG.edf", 2_400_000, datetime(2026,1,1,20,0,0),
     "SC4001", "SC", "E0", False, datetime.now()),
    ("/Volumes/eeg_lakehouse/bronze/raw_edf/SC4001EC-Hypnogram.edf",
     "SC4001EC-Hypnogram.edf", 5000, datetime(2026,1,1,20,0,0),
     "SC4001", "SC", "EC", True, datetime.now()),
    ("/Volumes/eeg_lakehouse/bronze/raw_edf/SC4002E0-PSG.edf",
     "SC4002E0-PSG.edf", 2_350_000, datetime(2026,1,2,21,0,0),
     "SC4002", "SC", "E0", False, datetime.now()),
    ("/Volumes/eeg_lakehouse/bronze/raw_edf/SC4002EC-Hypnogram.edf",
     "SC4002EC-Hypnogram.edf", 4800, datetime(2026,1,2,21,0,0),
     "SC4002", "SC", "EC", True, datetime.now()),
]

from src.bronze.ingest_eeg_files import BRONZE_EEG_SCHEMA
df_bronze = spark.createDataFrame(sample_data, schema=BRONZE_EEG_SCHEMA)
df_bronze.show(truncate=50)
print(f"Schema:")
df_bronze.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Key Exam Concept: Auto Loader vs COPY INTO
# MAGIC
# MAGIC | Feature | Auto Loader | COPY INTO |
# MAGIC |---------|-------------|----------|
# MAGIC | Trigger | Streaming (micro-batch) | Batch (SQL command) |
# MAGIC | Scale | Billions of files | Thousands of files |
# MAGIC | Tracking | Checkpoint dir | Delta transaction log |
# MAGIC | Schema evolution | `mergeSchema`, `cloudFiles.schemaHints` | `COPY_OPTIONS (mergeSchema = 'true')` |
# MAGIC | Best for | Continuous pipelines | One-time / scheduled loads |
# MAGIC
# MAGIC **In this project:** Auto Loader for EDF files (incremental, may add new subjects).
# MAGIC COPY INTO would work for the metadata CSV (static, known schema).

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Write to Local Delta Table (dev only)

# COMMAND ----------

import tempfile, os

tmp_dir = tempfile.mkdtemp()
bronze_path = os.path.join(tmp_dir, "bronze_eeg")

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .save(bronze_path)
)

print(f"Wrote Bronze Delta table to: {bronze_path}")

# Read back and verify
df_verify = spark.read.format("delta").load(bronze_path)
df_verify.select("subject_id", "session_id", "is_hypnogram", "file_size_bytes").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. DESCRIBE HISTORY (Delta Audit Log)
# MAGIC
# MAGIC Key exam command — shows all Delta operations with version, timestamp, operation type.

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, bronze_path)
dt.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=80)

print("\nDay 2 complete! Next: Day 3 - Bronze ingestion with Auto Loader patterns")
