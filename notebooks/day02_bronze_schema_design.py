# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2: Dataset Interface & Bronze Schema Design
# MAGIC
# MAGIC **Learning objectives:**
# MAGIC - Understand the Sleep-EDF EDF file format and naming convention
# MAGIC - Design Bronze Delta table schemas with explicit types
# MAGIC - Practice file-name parsing with Python UDFs
# MAGIC - Understand Auto Loader `cloudFiles` key options
# MAGIC
# MAGIC **Exam domains:** Auto Loader, Delta schema enforcement, UDFs
# MAGIC **Research:** Defines the data contract for 197-subject EDF corpus

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import parse_edf_filename, BRONZE_EDF_SCHEMA

cfg = AppConfig()
print("Catalog:", cfg.catalog.catalog)
print("Bronze EDF FQN:", cfg.catalog.bronze_edf_fqn)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Test File Name Parsing
# MAGIC
# MAGIC **Exam note:** Python UDFs serialize data row-by-row (slower).
# MAGIC Prefer Pandas UDFs or native Spark functions when processing millions of rows.
# MAGIC For file-name parsing (one row per file), Python UDFs are fine.

# COMMAND ----------

test_files = [
    "SC4001E0-PSG.edf",
    "SC4001EC-Hypnogram.edf",
    "SC4002E0-PSG.edf",
    "ST7011J0-PSG.edf",
    "unknown_file.edf",
]

for f in test_files:
    result = parse_edf_filename(f)
    print(f"{f:35s} -> {result}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Inspect Bronze Schema
# MAGIC
# MAGIC The Bronze layer stores the **minimum viable metadata** about each file.
# MAGIC We do NOT store the EDF binary content in Delta — that lives in Volumes.
# MAGIC The Bronze table is a **file registry** (pointer + metadata).

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string
print("Bronze EDF Schema:")
for field in BRONZE_EDF_SCHEMA.fields:
    nullable = "nullable" if field.nullable else "NOT NULL"
    print(f"  {field.name:25s} {str(field.dataType):20s} {nullable}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Create a Synthetic Bronze DataFrame (local dev)
# MAGIC
# MAGIC Without actual EDF files, create a synthetic DataFrame matching the
# MAGIC Bronze schema. This lets us test downstream Silver transformations
# MAGIC before real data is available.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    _ = spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("day02")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())

# Synthetic records matching Bronze schema
synth_data = [
    ("SC4001", 0, "PSG",       "/data/SC4001E0-PSG.edf",       "SC4001E0-PSG.edf",       130_000_000, "sleep-edf"),
    ("SC4001", 1, "PSG",       "/data/SC4001E1-PSG.edf",       "SC4001E1-PSG.edf",       128_000_000, "sleep-edf"),
    ("SC4001", 0, "Hypnogram", "/data/SC4001EC-Hypnogram.edf", "SC4001EC-Hypnogram.edf",      1_200, "sleep-edf"),
    ("SC4002", 0, "PSG",       "/data/SC4002E0-PSG.edf",       "SC4002E0-PSG.edf",       131_000_000, "sleep-edf"),
    ("SC4002", 0, "Hypnogram", "/data/SC4002EC-Hypnogram.edf", "SC4002EC-Hypnogram.edf",      1_300, "sleep-edf"),
]

columns = ["subject_id", "night_index", "file_type", "file_path", "file_name",
           "file_size_bytes", "dataset_source"]
df_bronze = (
    spark.createDataFrame(synth_data, columns)
    .withColumn("ingestion_ts", F.current_timestamp())
)
df_bronze.printSchema()
df_bronze.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Auto Loader Configuration Reference
# MAGIC
# MAGIC ```python
# MAGIC # ✅ Production Auto Loader pattern (Databricks only)
# MAGIC df = (
# MAGIC     spark.readStream
# MAGIC     .format("cloudFiles")                    # Auto Loader format
# MAGIC     .option("cloudFiles.format", "binaryFile") # Inner format
# MAGIC     .option("pathGlobFilter", "*.edf")         # Filter by extension
# MAGIC     .option("cloudFiles.schemaLocation",       # Persist inferred schema
# MAGIC             "/Volumes/eeg_lakehouse/bronze/_schema")
# MAGIC     .load("/Volumes/eeg_lakehouse/bronze/raw_edf")
# MAGIC )
# MAGIC
# MAGIC # Write with checkpoint (exactly-once guarantee)
# MAGIC df.writeStream
# MAGIC   .format("delta")
# MAGIC   .outputMode("append")
# MAGIC   .option("checkpointLocation", "/Volumes/.../checkpoint")
# MAGIC   .trigger(availableNow=True)  # Process all pending, then stop
# MAGIC   .toTable("eeg_lakehouse.bronze.raw_eeg_files")
# MAGIC   .awaitTermination()
# MAGIC ```
# MAGIC
# MAGIC **Exam trap:** `trigger(once=True)` is deprecated in Spark 3.4+. Use `trigger(availableNow=True)`.

# COMMAND ----------
print("Day 2 complete. Next: Day 3 - Bronze ingestion with DESCRIBE HISTORY")
