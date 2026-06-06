# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1: Intro, Goals & Repo Setup
# MAGIC
# MAGIC **Notebook purpose:** Verify your Databricks environment, explore the config module,
# MAGIC and confirm this repo is correctly attached as a Databricks Repo.
# MAGIC
# MAGIC **Exam domains touched:** Lakehouse platform, workspace setup, Unity Catalog basics
# MAGIC
# MAGIC **Research relevance:** Confirm MNE/YASA/Ripser are installable; verify PhysioNet
# MAGIC data access plan.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Environment Check

# COMMAND ----------

import sys
print(f"Python: {sys.version}")

try:
    import pyspark
    print(f"PySpark: {pyspark.__version__}")
except ImportError:
    print("PySpark not found — running locally? Install pyspark>=3.5.0")

try:
    from delta import configure_spark_with_delta_pip
    print("Delta Lake: available")
except ImportError:
    print("Delta: not found (OK on Databricks — pre-installed)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load Application Config

# COMMAND ----------

import sys
import os
# Add repo root to path when running in Databricks Repos
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig

cfg = AppConfig()
print(f"Environment: {cfg.env}")
print(f"EDF source path: {cfg.paths.edf_source_path}")
print(f"Bronze EDF table FQN: {cfg.catalog.bronze_edf_fqn}")
print(f"Gold features table FQN: {cfg.catalog.gold_features_fqn}")
print("\nSpark configuration:")
for k, v in cfg.spark.to_spark_conf().items():
    print(f"  {k} = {v}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Verify Spark Session

# COMMAND ----------

try:
    # On Databricks, `spark` is pre-injected
    print(f"Spark version: {spark.version}")
    print(f"Catalog: {spark.catalog.currentCatalog()}")
except NameError:
    # Local fallback
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("EEG-Lakehouse-Lab-Day01")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    print(f"Local Spark version: {spark.version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Optional: Install Research Libraries
# MAGIC
# MAGIC Run this cell on a Databricks cluster to install EEG/TDA libraries.
# MAGIC Skip this if libraries are already installed via cluster init script.

# COMMAND ----------

# %pip install mne yasa ripser giotto-tda persim xgboost shap
# dbutils.library.restartPython()  # Required after %pip install

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Architecture Recap
# MAGIC
# MAGIC ```
# MAGIC EDF Files (PhysioNet Sleep-EDF, N=197)
# MAGIC       │
# MAGIC       ▼  Auto Loader (cloudFiles)
# MAGIC  Bronze: eeg_lakehouse.bronze.raw_eeg_files
# MAGIC       │
# MAGIC       ▼  MNE preprocessing (Pandas UDF)
# MAGIC  Silver: eeg_lakehouse.silver.cleaned_epochs
# MAGIC       │
# MAGIC       ▼  YASA event detection (Pandas UDF)
# MAGIC  Silver: eeg_lakehouse.silver.spindle_events
# MAGIC       │  eeg_lakehouse.silver.slow_oscillation_events
# MAGIC       │  eeg_lakehouse.silver.pac_windows
# MAGIC       │
# MAGIC       ▼  TDA feature extraction (Ripser)
# MAGIC  Gold:   eeg_lakehouse.gold.tda_features
# MAGIC       │
# MAGIC       ▼  MLflow (RF / XGBoost / SHAP)
# MAGIC  Model Registry → Predict memory proxies
# MAGIC ```
# MAGIC
# MAGIC **Tomorrow (Day 2):** Define Bronze schemas and the Auto Loader ingestion skeleton.

# COMMAND ----------
print("Day 1 setup complete. See docs/daily-plan.md for Day 2 tasks.")
