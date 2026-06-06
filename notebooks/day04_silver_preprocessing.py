# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4: Silver Preprocessing — EEG Cleaning in PySpark
# MAGIC
# MAGIC **Learning objectives:**
# MAGIC - Apply `mapInPandas` for full-partition EDF preprocessing
# MAGIC - Understand Pandas UDF vs `mapInPandas` (exam key distinction)
# MAGIC - Write Silver epochs Delta table, partitioned by subject_id
# MAGIC - Inspect sigma/delta band power distributions
# MAGIC
# MAGIC **Exam domains:** Pandas UDFs (Domain 2), Delta partitioning (Domain 1)

# COMMAND ----------

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.silver.preprocess_eeg import preprocess_eeg, SILVER_EPOCH_SCHEMA
from pyspark.sql import functions as F

cfg = AppConfig()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Pandas UDF vs mapInPandas — Decision Guide
# MAGIC
# MAGIC | Method | Access pattern | Best for EEG use case |
# MAGIC |--------|---------------|----------------------|
# MAGIC | `@pandas_udf(return_type)` scalar | One column -> one column | Simple per-value transforms (e.g., log(sigma_power)) |
# MAGIC | `@pandas_udf` grouped map | Group of rows -> rows | Per-subject aggregations |
# MAGIC | `mapInPandas(func, schema)` | Full partition as pd.DataFrame | EDF file reading, TDA computation (needs full epoch as numpy) |
# MAGIC | `mapInArrow` | Full partition as Arrow RecordBatch | Same as mapInPandas but native Arrow |

# COMMAND ----------

# Example Pandas UDF for simple scalar transform:
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np

@pandas_udf("double")
def log_sigma_power(sigma_series: pd.Series) -> pd.Series:
    """Log-transform sigma power (stabilizes variance for ML)."""
    return sigma_series.apply(lambda x: float(np.log1p(x)) if x is not None and x > 0 else None)

# Demo: apply to a mock DataFrame
from pyspark.sql.types import StructType, StructField, FloatType, StringType
test_data = [("SC4001", 1.5), ("SC4001", 2.3), ("SC4002", 0.8)]
test_df = spark.createDataFrame(test_data, ["subject_id", "sigma_power"])
test_df.withColumn("log_sigma", log_sigma_power(F.col("sigma_power"))).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Run Silver Preprocessing (on Databricks)

# COMMAND ----------

# bronze_df = spark.table(cfg.catalog.bronze_edf_fqn)
# silver_epochs_df = preprocess_eeg(bronze_df, cfg)
#
# # Write Silver Delta table partitioned by subject
# (
#     silver_epochs_df.write
#     .format("delta")
#     .mode("overwrite")
#     .partitionBy("subject_id")
#     .saveAsTable(cfg.catalog.silver_epochs_fqn)
# )
# print(f"Silver epochs written to: {cfg.catalog.silver_epochs_fqn}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Inspect Silver Table

# COMMAND ----------

# silver_df = spark.table(cfg.catalog.silver_epochs_fqn)
# silver_df.printSchema()
# silver_df.groupBy("sleep_stage").count().orderBy("sleep_stage").show()
#
# # Band power distribution
# silver_df.select(
#     F.mean("sigma_power").alias("mean_sigma"),
#     F.stddev("sigma_power").alias("std_sigma"),
#     F.mean("delta_power").alias("mean_delta"),
#     F.percentile_approx("sigma_power", 0.5).alias("median_sigma")
# ).show()

print("Day 4 complete. Study note: mapInPandas processes entire partitions; Pandas UDF processes column vectors.")
