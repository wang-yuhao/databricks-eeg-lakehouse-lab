# Databricks notebook source
# MAGIC %md
# MAGIC # Day 17: Monitoring & Observability
# MAGIC 
# MAGIC ## Professional Certification - Advanced Data Engineering
# MAGIC 
# MAGIC ## MAGIC
# MAGIC 
# MAGIC ### **Learning Objectives:**
# MAGIC - Understand data observability and lineage in lakehouse architecture
# MAGIC - Master query profiling and performance monitoring techniques
# MAGIC - Leverage Spark UI and execution metrics for optimization
# MAGIC - Implement data quality monitoring and alerting
# MAGIC - Build monitoring dashboards for production pipelines
# MAGIC - Integrate with observability platforms (Datadog, Prometheus, Grafana)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Observability:****
# MAGIC 
# MAGIC Data observability provides visibility into:
# MAGIC - Data lineage (upstream/downstream dependencies)
# MAGIC - Data quality metrics and SLA compliance
# MAGIC - Pipeline execution health and performance
# MAGIC - Schema evolution and breaking changes
# MAGIC - Cost and resource utilization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Concepts:
# MAGIC 
# MAGIC 1. **Data Lineage**: Track data flow from source to consumption
# MAGIC 2. **Execution Metrics**: Monitor job performance, shuffle, and stage timings
# MAGIC 3. **Data Quality**: Measure completeness, accuracy, consistency, timeliness
# MAGIC 4. **Alerting**: Proactive notifications on pipeline failures or anomalies
# MAGIC 5. **Cost Monitoring**: Track compute and storage costs per workload

# COMMAND ----------

# Setup and imports
import json
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Data Lineage & Unity Catalog
# MAGIC 
# MAGIC Unity Catalog automatically tracks table lineage

# COMMAND ----------

# Query table lineage using Unity Catalog
# Note: Requires appropriate permissions

def get_table_lineage(catalog, schema, table):
    """
    Retrieve lineage information for a Unity Catalog table
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Get upstream dependencies (sources)
    upstream_query = f"""
    SELECT 
        source_table_full_name,
        source_column_name,
        entity_type
    FROM system.access.table_lineage
    WHERE target_table_full_name = '{full_table_name}'
    """
    
    # Get downstream dependencies (consumers)
    downstream_query = f"""
    SELECT 
        target_table_full_name,
        target_column_name,
        entity_type
    FROM system.access.table_lineage
    WHERE source_table_full_name = '{full_table_name}'
    """
    
    print(f"Lineage for {full_table_name}:")
    print("\nUpstream sources:")
    # spark.sql(upstream_query).display()
    
    print("\nDownstream consumers:")
    # spark.sql(downstream_query).display()
    
# Example usage
# get_table_lineage('eeg_lakehouse', 'bronze', 'eeg_raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Query Profiling & Spark UI
# MAGIC 
# MAGIC ### Analyzing Query Performance

# COMMAND ----------

# Performance profiling example
from pyspark.sql import SparkSession

def profile_query(query_name, df_operation):
    """
    Profile a DataFrame operation and log metrics
    """
    print(f"\n{'='*60}")
    print(f"Profiling: {query_name}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Execute the operation
    result = df_operation()
    
    # Force execution with count
    row_count = result.count()
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"Rows processed: {row_count:,}")
    print(f"Execution time: {elapsed:.2f} seconds")
    print(f"Throughput: {row_count/elapsed:,.0f} rows/sec")
    
    # Get query execution plan
    print("\nPhysical Plan:")
    result.explain(mode='cost')
    
    return result, elapsed

# COMMAND ----------

# Example: Profile a sample aggregation
def sample_aggregation():
    # Create sample EEG data
    sample_data = spark.range(0, 1000000).selectExpr(
        "id as sample_id",
        "cast(rand() * 100 as double) as eeg_value",
        "cast(rand() * 10 as int) as channel_id"
    )
    
    # Perform aggregation
    result = sample_data.groupBy("channel_id") \
        .agg(
            F.avg("eeg_value").alias("avg_value"),
            F.stddev("eeg_value").alias("std_value"),
            F.count("*").alias("sample_count")
        )
    
    return result

# Profile the query
# result_df, exec_time = profile_query("EEG Channel Aggregation", sample_aggregation)
# result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Execution Metrics & Performance Monitoring

# COMMAND ----------

# Monitor Spark execution metrics
def get_spark_metrics():
    """
    Retrieve current Spark application metrics
    """
    sc = spark.sparkContext
    
    # Get application info
    app_id = sc.applicationId
    app_name = sc.appName
    
    # Get executor metrics
    executor_info = sc._jsc.sc().getExecutorMemoryStatus()
    
    print(f"Application ID: {app_id}")
    print(f"Application Name: {app_name}")
    print(f"\nActive Executors: {len(executor_info)}")
    
    # Get stage metrics from recent jobs
    status_tracker = sc.statusTracker()
    active_jobs = status_tracker.getActiveJobIds()
    
    print(f"Active Jobs: {len(active_jobs)}")
    
    return {
        'app_id': app_id,
        'app_name': app_name,
        'executor_count': len(executor_info),
        'active_jobs': len(active_jobs)
    }

# metrics = get_spark_metrics()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitoring Data Quality Metrics

# COMMAND ----------

def compute_data_quality_metrics(df, table_name):
    """
    Compute comprehensive data quality metrics
    """
    total_rows = df.count()
    total_cols = len(df.columns)
    
    print(f"\nData Quality Report: {table_name}")
    print(f"{'='*60}")
    print(f"Total Rows: {total_rows:,}")
    print(f"Total Columns: {total_cols}")
    print(f"\nColumn-level Metrics:")
    
    quality_metrics = []
    
    for col_name in df.columns:
        # Count nulls
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = (null_count / total_rows) * 100
        
        # Count distinct values
        distinct_count = df.select(col_name).distinct().count()
        
        # Get data type
        col_type = dict(df.dtypes)[col_name]
        
        metric = {
            'column': col_name,
            'type': col_type,
            'null_count': null_count,
            'null_percentage': round(null_pct, 2),
            'distinct_count': distinct_count,
            'completeness': round(100 - null_pct, 2)
        }
        
        quality_metrics.append(metric)
        
        print(f"  {col_name:20} | Nulls: {null_pct:5.2f}% | Distinct: {distinct_count:8,} | Type: {col_type}")
    
    # Create metrics DataFrame
    metrics_df = spark.createDataFrame(quality_metrics)
    
    return metrics_df

# COMMAND ----------

# Example: Compute quality metrics for a sample dataset
sample_df = spark.range(0, 10000).selectExpr(
    "id",
    "case when rand() < 0.1 then null else cast(rand() * 100 as double) end as value1",
    "case when rand() < 0.05 then null else cast(rand() * 50 as int) end as value2",
    "case when rand() < 0.15 then null else concat('user_', cast(id % 100 as string)) end as user_id"
)

# quality_df = compute_data_quality_metrics(sample_df, "sample_dataset")
# quality_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Delta Table Monitoring

# COMMAND ----------

# Monitor Delta table statistics
def analyze_delta_table(catalog, schema, table):
    """
    Analyze Delta table characteristics and health
    """
    full_table = f"{catalog}.{schema}.{table}"
    
    print(f"\nDelta Table Analysis: {full_table}")
    print(f"{'='*60}")
    
    # Get table details
    detail_df = spark.sql(f"DESCRIBE DETAIL {full_table}")
    
    # Extract key metrics
    details = detail_df.collect()[0]
    
    print(f"Format: {details['format']}")
    print(f"Number of Files: {details['numFiles']:,}")
    print(f"Size in Bytes: {details['sizeInBytes']:,}")
    print(f"Size in MB: {details['sizeInBytes'] / (1024*1024):.2f}")
    
    # Get table history
    history_df = spark.sql(f"DESCRIBE HISTORY {full_table} LIMIT 10")
    
    print(f"\nRecent Operations:")
    history_df.select("version", "timestamp", "operation", "operationMetrics").show(10, truncate=False)
    
    # Check for small files (potential for optimization)
    if details['numFiles'] > 0:
        avg_file_size_mb = (details['sizeInBytes'] / details['numFiles']) / (1024*1024)
        print(f"\nAverage File Size: {avg_file_size_mb:.2f} MB")
        
        if avg_file_size_mb < 100:
            print("⚠️  Warning: Small file problem detected. Consider running OPTIMIZE.")
    
    return detail_df, history_df

# Example usage:
# detail, history = analyze_delta_table('eeg_lakehouse', 'bronze', 'eeg_raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Custom Monitoring Dashboard

# COMMAND ----------

# Create monitoring metrics table
def create_monitoring_table():
    """
    Create a table to store pipeline monitoring metrics
    """
    spark.sql("""
    CREATE TABLE IF NOT EXISTS eeg_lakehouse.monitoring.pipeline_metrics (
        metric_id STRING,
        pipeline_name STRING,
        execution_time TIMESTAMP,
        status STRING,
        rows_processed BIGINT,
        duration_seconds DOUBLE,
        error_message STRING,
        metadata MAP<STRING, STRING>
    )
    USING DELTA
    PARTITIONED BY (DATE(execution_time))
    """)
    
    print("✅ Monitoring table created/verified")

# create_monitoring_table()

# COMMAND ----------

# Log pipeline execution metrics
def log_pipeline_execution(pipeline_name, status, rows_processed, duration, error_msg=None, metadata=None):
    """
    Log pipeline execution metrics for monitoring
    """
    import uuid
    
    metric_data = [(
        str(uuid.uuid4()),
        pipeline_name,
        datetime.now(),
        status,  # 'SUCCESS', 'FAILED', 'RUNNING'
        rows_processed,
        duration,
        error_msg,
        metadata or {}
    )]
    
    metrics_df = spark.createDataFrame(metric_data, [
        "metric_id", "pipeline_name", "execution_time", "status",
        "rows_processed", "duration_seconds", "error_message", "metadata"
    ])
    
    # Append to monitoring table
    metrics_df.write.format("delta").mode("append") \
        .saveAsTable("eeg_lakehouse.monitoring.pipeline_metrics")
    
    print(f"✅ Logged execution for {pipeline_name}")

# Example usage:
# log_pipeline_execution(
#     pipeline_name="eeg_bronze_ingestion",
#     status="SUCCESS",
#     rows_processed=150000,
#     duration=45.2,
#     metadata={'source': 'PhysioNet', 'version': 'v2.1'}
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Alerting & Notifications

# COMMAND ----------

# Define data quality rules and alerts
def check_data_quality_rules(df, table_name):
    """
    Check data quality rules and raise alerts if violated
    """
    alerts = []
    
    total_rows = df.count()
    
    # Rule 1: Check for minimum row count
    min_rows_threshold = 1000
    if total_rows < min_rows_threshold:
        alerts.append({
            'severity': 'HIGH',
            'rule': 'MinimumRowCount',
            'message': f'Table {table_name} has only {total_rows} rows (threshold: {min_rows_threshold})'
        })
    
    # Rule 2: Check for high null percentage in critical columns
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = (null_count / total_rows) * 100
        
        if null_pct > 20:  # Alert if > 20% nulls
            alerts.append({
                'severity': 'MEDIUM',
                'rule': 'HighNullPercentage',
                'message': f'Column {col_name} has {null_pct:.2f}% null values'
            })
    
    # Rule 3: Check for duplicate records (if ID column exists)
    if 'id' in df.columns or 'patient_id' in df.columns:
        id_col = 'id' if 'id' in df.columns else 'patient_id'
        total_count = df.count()
        distinct_count = df.select(id_col).distinct().count()
        
        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            alerts.append({
                'severity': 'HIGH',
                'rule': 'DuplicateRecords',
                'message': f'Found {duplicate_count} duplicate records in {id_col}'
            })
    
    # Print alerts
    if alerts:
        print(f"\n⚠️  Data Quality Alerts for {table_name}:")
        print(f"{'='*60}")
        for alert in alerts:
            print(f"[{alert['severity']}] {alert['rule']}: {alert['message']}")
    else:
        print(f"\n✅ All data quality checks passed for {table_name}")
    
    return alerts

# Example
# alerts = check_data_quality_rules(sample_df, "sample_dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Integration with External Monitoring

# COMMAND ----------

# Send metrics to external monitoring system (example with HTTP)
def send_metrics_to_external_system(metrics_dict, endpoint_url=None):
    """
    Send metrics to external monitoring system like Datadog, Prometheus, or custom endpoint
    """
    import requests
    
    # Example payload structure (adjust based on your monitoring system)
    payload = {
        'timestamp': datetime.now().isoformat(),
        'metrics': metrics_dict,
        'tags': {
            'environment': 'production',
            'pipeline': 'eeg_lakehouse',
            'cluster_id': spark.sparkContext.applicationId
        }
    }
    
    print("📊 Metrics payload:")
    print(json.dumps(payload, indent=2))
    
    # Uncomment to actually send (requires endpoint configuration)
    # if endpoint_url:
    #     response = requests.post(endpoint_url, json=payload)
    #     print(f"Response status: {response.status_code}")
    
    return payload

# Example usage
metrics = {
    'pipeline_duration': 125.5,
    'rows_processed': 500000,
    'data_quality_score': 0.98,
    'cost_usd': 2.50
}

# payload = send_metrics_to_external_system(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Cost Monitoring

# COMMAND ----------

# Monitor cluster and job costs
def estimate_job_cost(duration_hours, dbu_rate=0.15):
    """
    Estimate job cost based on cluster usage
    Adjust DBU rate based on your Databricks pricing tier
    """
    # Get cluster info
    cluster_info = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("clusterId")
    
    # Simplified cost calculation
    # In reality, you'd query Databricks billing API or usage tables
    
    estimated_dbus = duration_hours * 2  # Assume 2 DBUs per hour (adjust based on cluster size)
    estimated_cost = estimated_dbus * dbu_rate
    
    print(f"Job Duration: {duration_hours:.2f} hours")
    print(f"Estimated DBUs: {estimated_dbus:.2f}")
    print(f"Estimated Cost: ${estimated_cost:.2f}")
    
    return estimated_cost

# Example
# cost = estimate_job_cost(duration_hours=0.5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Hands-On Exercises
# MAGIC 
# MAGIC ### Exercise 1: Create a monitoring dashboard
# MAGIC - Query pipeline_metrics table
# MAGIC - Aggregate success/failure rates by pipeline
# MAGIC - Calculate average execution times
# MAGIC - Identify performance trends
# MAGIC 
# MAGIC ### Exercise 2: Implement custom data quality checks
# MAGIC - Define quality rules for EEG data
# MAGIC - Check for outliers in EEG signal values
# MAGIC - Validate sampling rates and channel counts
# MAGIC - Generate quality reports
# MAGIC 
# MAGIC ### Exercise 3: Performance optimization
# MAGIC - Identify slow queries using Spark UI
# MAGIC - Analyze shuffle patterns
# MAGIC - Optimize partitioning strategy
# MAGIC - Measure improvement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this notebook, you learned:
# MAGIC 
# MAGIC ✅ Data observability and lineage tracking with Unity Catalog  
# MAGIC ✅ Query profiling and performance monitoring techniques  
# MAGIC ✅ Spark UI metrics and execution analysis  
# MAGIC ✅ Data quality monitoring and validation  
# MAGIC ✅ Custom alerting and notification systems  
# MAGIC ✅ Integration with external monitoring platforms  
# MAGIC ✅ Cost monitoring and optimization  
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC - Day 18: PhysioNet Dataset Integration
# MAGIC - Day 19: Security & Compliance Patterns
# MAGIC - Day 20: Advanced TDA Algorithms
# MAGIC - Day 21: End-to-End Pipeline Integration
