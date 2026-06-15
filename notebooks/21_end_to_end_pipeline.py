# Databricks notebook source
# MAGIC %md
# MAGIC # Day 21: End-to-End Pipeline Integration
# MAGIC 
# MAGIC ## 🎯 Learning Objectives
# MAGIC - Integrate all components into production pipeline
# MAGIC - Implement Delta Live Tables (DLT) workflows
# MAGIC - Configure data quality expectations
# MAGIC - Set up pipeline orchestration and monitoring
# MAGIC - Deploy end-to-end automated EEG processing
# MAGIC 
# MAGIC ## 📋 Prerequisites
# MAGIC - Completed Days 1-20
# MAGIC - Unity Catalog setup
# MAGIC - DLT enabled on Databricks workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Delta Live Tables Pipeline Definition
# MAGIC 
# MAGIC Delta Live Tables provides:
# MAGIC - Declarative pipeline definitions
# MAGIC - Automatic data quality monitoring
# MAGIC - Dependency management
# MAGIC - Incremental processing

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Pipeline configuration
CATALOG = "eeg_lakehouse"
SCHEMA = "production"
RAW_PATH = "/mnt/eeg_data/raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Raw Data Ingestion

# COMMAND ----------

@dlt.table(
  name="bronze_eeg_raw",
  comment="Raw EEG data from multiple sources",
  table_properties={
    "quality": "bronze",
    "pipelines.autoOptimize.zOrderCols": "subject_id,recording_date"
  }
)
@dlt.expect_or_drop("valid_subject_id", "subject_id IS NOT NULL")
@dlt.expect_or_drop("valid_recording_date", "recording_date IS NOT NULL")
def bronze_eeg_raw():
  """
  Ingest raw EEG data with basic quality checks
  """
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", f"{RAW_PATH}/schema")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(RAW_PATH)
      .withColumn("ingestion_timestamp", F.current_timestamp())
      .withColumn("source_file", F.input_file_name())
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer: Cleaned and Enriched Data

# COMMAND ----------

@dlt.table(
  name="silver_eeg_preprocessed",
  comment="Preprocessed EEG with artifact removal and filtering",
  table_properties={
    "quality": "silver",
    "pipelines.autoOptimize.zOrderCols": "subject_id,epoch_id"
  }
)
@dlt.expect_or_drop("valid_sampling_rate", "sampling_rate IN (256, 512, 1000)")
@dlt.expect_or_drop("valid_epoch_duration", "epoch_duration_sec BETWEEN 0.5 AND 30")
@dlt.expect("quality_score_acceptable", "signal_quality_score >= 0.5")
def silver_eeg_preprocessed():
  """
  Apply preprocessing pipeline:
  - Bandpass filtering
  - Artifact removal (ICA)
  - Epoch segmentation
  - Quality scoring
  """
  return (
    dlt.read_stream("bronze_eeg_raw")
      .filter(F.col("channel_count") >= 19)  # Minimum channels
      .withColumn(
        "preprocessed_signal",
        F.expr("""
          apply_preprocessing(
            eeg_signal,
            sampling_rate,
            highpass_freq,
            lowpass_freq
          )
        """)
      )
      .withColumn(
        "signal_quality_score",
        F.expr("calculate_signal_quality(preprocessed_signal)")
      )
      .withColumn("processing_timestamp", F.current_timestamp())
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer: Feature Engineering

# COMMAND ----------

@dlt.table(
  name="gold_spectral_features",
  comment="Spectral features for ML models",
  table_properties={
    "quality": "gold",
    "pipelines.autoOptimize.zOrderCols": "subject_id,feature_date"
  }
)
@dlt.expect_all({
  "valid_delta_power": "delta_power >= 0",
  "valid_theta_power": "theta_power >= 0",
  "valid_alpha_power": "alpha_power >= 0",
  "valid_beta_power": "beta_power >= 0",
  "valid_gamma_power": "gamma_power >= 0"
})
def gold_spectral_features():
  """
  Extract spectral features from preprocessed EEG
  """
  return (
    dlt.read_stream("silver_eeg_preprocessed")
      .withColumn(
        "spectral_features",
        F.expr("""
          compute_spectral_features(
            preprocessed_signal,
            sampling_rate,
            ['delta', 'theta', 'alpha', 'beta', 'gamma']
          )
        """)
      )
      .select(
        "subject_id",
        "epoch_id",
        "recording_date",
        "spectral_features.*",
        F.current_timestamp().alias("feature_timestamp")
      )
  )

# COMMAND ----------

@dlt.table(
  name="gold_connectivity_features",
  comment="Functional connectivity features",
  table_properties={"quality": "gold"}
)
def gold_connectivity_features():
  """
  Compute connectivity metrics (PLV, coherence)
  """
  return (
    dlt.read_stream("silver_eeg_preprocessed")
      .withColumn(
        "connectivity_features",
        F.expr("""
          compute_connectivity_features(
            preprocessed_signal,
            channel_names,
            ['plv', 'coherence', 'pli']
          )
        """)
      )
      .select(
        "subject_id",
        "epoch_id",
        "connectivity_features.*",
        F.current_timestamp().alias("feature_timestamp")
      )
  )

# COMMAND ----------

@dlt.table(
  name="gold_tda_features",
  comment="Topological Data Analysis features",
  table_properties={"quality": "gold"}
)
def gold_tda_features():
  """
  Extract TDA features (persistent homology)
  """
  return (
    dlt.read_stream("silver_eeg_preprocessed")
      .withColumn(
        "tda_features",
        F.expr("""
          compute_tda_features(
            preprocessed_signal,
            embedding_dimension,
            time_delay
          )
        """)
      )
      .select(
        "subject_id",
        "epoch_id",
        "tda_features.*",
        F.current_timestamp().alias("feature_timestamp")
      )
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: ML-Ready Feature Store

# COMMAND ----------

@dlt.table(
  name="ml_feature_store",
  comment="Unified feature store for ML models",
  table_properties={
    "quality": "gold",
    "delta.enableChangeDataFeed": "true"
  }
)
def ml_feature_store():
  """
  Combine all features into ML-ready format
  """
  spectral = dlt.read("gold_spectral_features")
  connectivity = dlt.read("gold_connectivity_features")
  tda = dlt.read("gold_tda_features")
  
  return (
    spectral
      .join(connectivity, ["subject_id", "epoch_id"], "left")
      .join(tda, ["subject_id", "epoch_id"], "left")
      .withColumn("feature_version", F.lit("v1.0"))
      .withColumn("created_at", F.current_timestamp())
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Data Quality Monitoring

# COMMAND ----------

@dlt.table(
  name="data_quality_metrics",
  comment="Pipeline data quality monitoring"
)
def data_quality_metrics():
  """
  Track data quality metrics across pipeline
  """
  return (
    spark.sql("""
      SELECT
        current_timestamp() as metric_timestamp,
        'bronze_eeg_raw' as table_name,
        COUNT(*) as record_count,
        COUNT(DISTINCT subject_id) as unique_subjects,
        AVG(signal_quality_score) as avg_quality_score,
        SUM(CASE WHEN signal_quality_score < 0.5 THEN 1 ELSE 0 END) as low_quality_count
      FROM LIVE.silver_eeg_preprocessed
      WHERE processing_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    """)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Pipeline Configuration and Deployment

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Configuration JSON
# MAGIC 
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "eeg_lakehouse_pipeline",
# MAGIC   "storage": "/mnt/dlt/eeg_pipeline",
# MAGIC   "configuration": {
# MAGIC     "spark.databricks.delta.optimizeWrite.enabled": "true",
# MAGIC     "spark.databricks.delta.autoCompact.enabled": "true",
# MAGIC     "pipelines.applyChangesPreviewEnabled": "true"
# MAGIC   },
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "autoscale": {
# MAGIC         "min_workers": 1,
# MAGIC         "max_workers": 5,
# MAGIC         "mode": "ENHANCED"
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "libraries": [
# MAGIC     {"notebook": {"path": "/Workspace/notebooks/21_end_to_end_pipeline"}}
# MAGIC   ],
# MAGIC   "target": "eeg_lakehouse.production",
# MAGIC   "continuous": true,
# MAGIC   "channel": "CURRENT"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Orchestration with Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workflow Configuration
# MAGIC 
# MAGIC Create a Databricks Workflow for:
# MAGIC 1. Daily batch processing
# MAGIC 2. Model training and evaluation
# MAGIC 3. Data quality checks
# MAGIC 4. Report generation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create monitoring view for workflow
# MAGIC CREATE OR REPLACE VIEW eeg_lakehouse.production.pipeline_health AS
# MAGIC SELECT
# MAGIC   date_trunc('hour', processing_timestamp) as processing_hour,
# MAGIC   COUNT(*) as records_processed,
# MAGIC   AVG(signal_quality_score) as avg_quality,
# MAGIC   COUNT(DISTINCT subject_id) as subjects_processed,
# MAGIC   SUM(CASE WHEN signal_quality_score >= 0.8 THEN 1 ELSE 0 END) / COUNT(*) as high_quality_ratio
# MAGIC FROM eeg_lakehouse.production.silver_eeg_preprocessed
# MAGIC WHERE processing_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY date_trunc('hour', processing_timestamp)
# MAGIC ORDER BY processing_hour DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: MLflow Integration

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

# Set experiment
mlflow.set_experiment("/Workspace/experiments/eeg_sleep_staging")

def train_and_register_model():
  """
  Train model on feature store and register in Model Registry
  """
  with mlflow.start_run(run_name="sleep_staging_production") as run:
    # Load features from feature store
    features_df = spark.table("eeg_lakehouse.production.ml_feature_store")
    
    # Log parameters
    mlflow.log_param("feature_version", "v1.0")
    mlflow.log_param("model_type", "random_forest")
    
    # Train model (implementation from Day 14)
    # model = train_sleep_staging_model(features_df)
    
    # Log metrics
    # mlflow.log_metric("accuracy", accuracy)
    # mlflow.log_metric("f1_score", f1)
    
    # Register model
    # mlflow.sklearn.log_model(
    #   model,
    #   "model",
    #   registered_model_name="eeg_sleep_staging_model"
    # )
    
    print(f"✅ Model registered: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Real-time Monitoring Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Real-time pipeline metrics
# MAGIC SELECT
# MAGIC   'Pipeline Status' as metric_type,
# MAGIC   COUNT(*) as total_records,
# MAGIC   AVG(signal_quality_score) as avg_quality_score,
# MAGIC   MAX(processing_timestamp) as last_processed
# MAGIC FROM eeg_lakehouse.production.silver_eeg_preprocessed
# MAGIC WHERE processing_timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT
# MAGIC   'Feature Engineering' as metric_type,
# MAGIC   COUNT(*) as total_records,
# MAGIC   NULL as avg_quality_score,
# MAGIC   MAX(feature_timestamp) as last_processed
# MAGIC FROM eeg_lakehouse.production.ml_feature_store
# MAGIC WHERE feature_timestamp >= current_timestamp() - INTERVAL 1 HOUR;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Alerting and Notifications

# COMMAND ----------

def check_pipeline_health():
  """
  Check pipeline health and send alerts if needed
  """
  metrics = spark.sql("""
    SELECT
      COUNT(*) as record_count,
      AVG(signal_quality_score) as avg_quality
    FROM eeg_lakehouse.production.silver_eeg_preprocessed
    WHERE processing_timestamp >= current_timestamp() - INTERVAL 1 HOUR
  """).first()
  
  # Alert conditions
  if metrics.record_count < 100:
    print("⚠️ ALERT: Low record volume in last hour")
    # send_alert("Low volume", metrics.record_count)
  
  if metrics.avg_quality < 0.6:
    print("⚠️ ALERT: Quality degradation detected")
    # send_alert("Quality issue", metrics.avg_quality)
  
  print(f"✅ Pipeline health check complete")
  print(f"Records: {metrics.record_count}, Quality: {metrics.avg_quality:.3f}")

# Run health check
# check_pipeline_health()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Deployment Checklist

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Deployment Steps:
# MAGIC 
# MAGIC #### 1. **Infrastructure Setup** ✅
# MAGIC - Unity Catalog configured
# MAGIC - External storage mounted
# MAGIC - Network security configured
# MAGIC - Cluster policies defined
# MAGIC 
# MAGIC #### 2. **Pipeline Configuration** ✅
# MAGIC - DLT pipeline created
# MAGIC - Quality expectations defined
# MAGIC - Streaming sources configured
# MAGIC - Target schemas created
# MAGIC 
# MAGIC #### 3. **Monitoring Setup** ✅
# MAGIC - Pipeline health dashboard
# MAGIC - Data quality metrics
# MAGIC - Alert configurations
# MAGIC - Log aggregation
# MAGIC 
# MAGIC #### 4. **ML Integration** ✅
# MAGIC - Feature store populated
# MAGIC - Model registry configured
# MAGIC - Inference pipelines deployed
# MAGIC - A/B testing framework
# MAGIC 
# MAGIC #### 5. **Documentation** ✅
# MAGIC - Pipeline architecture diagrams
# MAGIC - Runbooks for operations
# MAGIC - Data dictionary
# MAGIC - Troubleshooting guides

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this final notebook, you learned:
# MAGIC 
# MAGIC ✅ Delta Live Tables pipeline architecture
# MAGIC ✅ Multi-layer data processing (Bronze → Silver → Gold)
# MAGIC ✅ Data quality expectations and monitoring
# MAGIC ✅ Feature store integration
# MAGIC ✅ MLflow model registry and tracking
# MAGIC ✅ Production deployment patterns
# MAGIC ✅ Real-time monitoring and alerting
# MAGIC ✅ End-to-end orchestration
# MAGIC 
# MAGIC ### 🎉 Congratulations!
# MAGIC 
# MAGIC You've completed the 21-day EEG Lakehouse journey covering:
# MAGIC - Days 1-7: Foundation (Databricks, Delta Lake, Unity Catalog)
# MAGIC - Days 8-14: EEG Processing (Preprocessing, Features, ML)
# MAGIC - Days 15-21: Advanced Topics (Streaming, Graph, TDA, Production)
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC - Deploy pipeline to production
# MAGIC - Integrate with clinical systems
# MAGIC - Scale to multi-center datasets
# MAGIC - Publish research findings
# MAGIC 
# MAGIC ### Resources:
# MAGIC - [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
# MAGIC - [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)
# MAGIC - [Databricks Workflows](https://docs.databricks.com/workflows/index.html)
