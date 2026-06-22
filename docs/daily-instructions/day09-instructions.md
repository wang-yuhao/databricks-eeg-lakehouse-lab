# Day 9: Monitoring, Logging, and Data Quality

## Objective
Implement comprehensive monitoring, logging, and data quality validation for the EEG lakehouse pipeline to ensure reliability, observability, and data integrity.

---

## Prerequisites

### Completed Tasks
- Delta Live Tables pipeline configured (Day 7)
- Bronze, Silver, and Gold tables created
- Unity Catalog setup (if using Premium)

### Required Knowledge
- Delta Lake expectations and constraints
- Databricks monitoring capabilities
- Data quality metrics and validation patterns

---

## Part 1: Enable Delta Live Tables Monitoring

### 1.1 Configure Event Logs

Delta Live Tables automatically captures pipeline events. Access these logs:

```python
# Read DLT event log
from pyspark.sql.functions import *

# Path to DLT event log (adjust based on your pipeline)
event_log_path = "/mnt/checkpoints/system/events"

event_df = spark.read.format("delta").load(event_log_path)

# Display recent events
display(
    event_df
    .select(
        "timestamp",
        "origin.update_id",
        "event_type",
        "level",
        "message",
        "details"
    )
    .orderBy(col("timestamp").desc())
    .limit(50)
)
```

### 1.2 Monitor Pipeline Metrics

```python
# Extract pipeline run metrics
pipeline_metrics = event_df.filter(
    col("event_type") == "update_progress"
).select(
    "timestamp",
    "details.update_id",
    "details.metrics.num_output_rows",
    "details.metrics.data_quality.dropped_records",
    "details.metrics.data_quality.expectations"
)

display(pipeline_metrics.orderBy("timestamp", ascending=False))
```

**Expected Output**: Metrics showing processed rows and data quality results

---

## Part 2: Implement Data Quality Checks

### 2.1 Add Delta Expectations to DLT Pipeline

Update your DLT pipeline notebook with expectations:

```python
import dlt
from pyspark.sql.functions import *

# Bronze table with expectations
@dlt.table(
    name="eeg_bronze",
    comment="Raw EEG data with quality checks"
)
@dlt.expect_or_drop("valid_timestamp", "record_timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL AND patient_id != ''")
@dlt.expect_or_fail("valid_file_format", "file_format IN ('edf', 'fif', 'set')")
def bronze_eeg_quality():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema/bronze")
        .load("/mnt/bronze/eeg_raw")
        .withColumn("record_timestamp", current_timestamp())
    )

# Silver table with advanced quality checks
@dlt.table(
    name="eeg_silver",
    comment="Cleaned EEG data with comprehensive quality validation"
)
@dlt.expect_or_drop("valid_channel_count", "num_channels > 0 AND num_channels <= 256")
@dlt.expect_or_drop("valid_sampling_rate", "sampling_rate >= 100 AND sampling_rate <= 10000")
@dlt.expect_or_drop("valid_duration", "duration_seconds > 0 AND duration_seconds <= 86400")
@dlt.expect("signal_quality_check", "signal_quality_score >= 0.5", on_violation="quarantine")
def silver_eeg_quality():
    return (
        dlt.read_stream("eeg_bronze")
        .select(
            "patient_id",
            "recording_id",
            "num_channels",
            "sampling_rate",
            "duration_seconds",
            "signal_quality_score",
            "processed_timestamp"
        )
    )
```

### 2.2 Create Data Quality Metrics Table

```python
@dlt.table(
    name="data_quality_metrics",
    comment="Aggregated data quality metrics"
)
def quality_metrics():
    return (
        dlt.read("eeg_silver")
        .groupBy(window("processed_timestamp", "1 hour"))
        .agg(
            count("*").alias("total_records"),
            avg("signal_quality_score").alias("avg_quality_score"),
            min("signal_quality_score").alias("min_quality_score"),
            max("signal_quality_score").alias("max_quality_score"),
            countDistinct("patient_id").alias("unique_patients"),
            sum(when(col("signal_quality_score") < 0.7, 1).otherwise(0)).alias("low_quality_count")
        )
    )
```

**Expected Result**: Quality metrics tracked at hourly intervals

---

## Part 3: Implement Custom Logging

### 3.1 Create Audit Log Table

```sql
CREATE TABLE IF NOT EXISTS eeg_lakehouse.audit_logs (
    log_id STRING,
    timestamp TIMESTAMP,
    pipeline_name STRING,
    table_name STRING,
    operation STRING,
    rows_processed BIGINT,
    rows_failed BIGINT,
    execution_time_seconds DOUBLE,
    status STRING,
    error_message STRING,
    metadata MAP<STRING, STRING>
)
USING DELTA
LOCATION '/mnt/gold/audit_logs'
COMMENT 'Audit trail for all pipeline operations';
```

### 3.2 Implement Logging Function

```python
from datetime import datetime
import uuid

def log_pipeline_execution(
    pipeline_name: str,
    table_name: str,
    operation: str,
    rows_processed: int,
    rows_failed: int = 0,
    execution_time: float = 0.0,
    status: str = "SUCCESS",
    error_message: str = None,
    metadata: dict = None
):
    """Log pipeline execution details to audit table"""
    
    log_entry = spark.createDataFrame([(
        str(uuid.uuid4()),
        datetime.now(),
        pipeline_name,
        table_name,
        operation,
        rows_processed,
        rows_failed,
        execution_time,
        status,
        error_message,
        metadata or {}
    )], schema="log_id STRING, timestamp TIMESTAMP, pipeline_name STRING, " +
               "table_name STRING, operation STRING, rows_processed BIGINT, " +
               "rows_failed BIGINT, execution_time_seconds DOUBLE, status STRING, " +
               "error_message STRING, metadata MAP<STRING, STRING>")
    
    log_entry.write.format("delta").mode("append").saveAsTable("eeg_lakehouse.audit_logs")

# Example usage
start_time = time.time()
try:
    # Your pipeline code
    result_df = spark.read.table("eeg_lakehouse.silver.eeg_silver")
    count = result_df.count()
    
    execution_time = time.time() - start_time
    log_pipeline_execution(
        pipeline_name="eeg_silver_processing",
        table_name="eeg_silver",
        operation="READ",
        rows_processed=count,
        execution_time=execution_time,
        metadata={"notebook": "day09_monitoring", "cluster_id": spark.conf.get("spark.databricks.clusterUsageTags.clusterId")}
    )
except Exception as e:
    execution_time = time.time() - start_time
    log_pipeline_execution(
        pipeline_name="eeg_silver_processing",
        table_name="eeg_silver",
        operation="READ",
        rows_processed=0,
        rows_failed=0,
        execution_time=execution_time,
        status="FAILED",
        error_message=str(e),
        metadata={"notebook": "day09_monitoring"}
    )
    raise
```

---

## Part 4: Monitor Table Operations

### 4.1 Query Delta Table History

```python
# View table operation history
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "eeg_lakehouse.silver.eeg_silver")
history_df = delta_table.history()

display(
    history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics",
        "userMetadata"
    ).orderBy("version", ascending=False)
)
```

### 4.2 Analyze Data Modification Patterns

```python
# Analyze write patterns
write_operations = history_df.filter(
    col("operation").isin(["WRITE", "MERGE", "UPDATE", "DELETE"])
).select(
    "timestamp",
    "operation",
    col("operationMetrics.numOutputRows").alias("rows_affected"),
    col("operationMetrics.numFiles").alias("files_written"),
    col("operationMetrics.executionTimeMs").alias("execution_time_ms")
)

display(write_operations.orderBy("timestamp", ascending=False))
```

**Expected Output**: Complete audit trail of all table modifications

---

## Part 5: Implement Data Quality Dashboard

### 5.1 Create Quality Metrics View

```sql
CREATE OR REPLACE VIEW eeg_lakehouse.gold.data_quality_dashboard AS
SELECT 
    date_trunc('day', timestamp) as date,
    COUNT(*) as total_records,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_records,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_records,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate,
    AVG(execution_time_seconds) as avg_execution_time,
    SUM(rows_processed) as total_rows_processed,
    SUM(rows_failed) as total_rows_failed
FROM eeg_lakehouse.audit_logs
GROUP BY date_trunc('day', timestamp)
ORDER BY date DESC;
```

### 5.2 Query Quality Dashboard

```python
# Display quality metrics
quality_dashboard = spark.sql("""
    SELECT 
        date,
        total_records,
        successful_records,
        failed_records,
        success_rate,
        avg_execution_time,
        total_rows_processed,
        total_rows_failed
    FROM eeg_lakehouse.gold.data_quality_dashboard
    WHERE date >= current_date() - INTERVAL 7 DAYS
""")

display(quality_dashboard)
```

---

## Part 6: Set Up Alerting

### 6.1 Create Quality Threshold Checks

```python
def check_quality_thresholds():
    """Check if data quality metrics meet thresholds"""
    
    # Define thresholds
    MIN_SUCCESS_RATE = 95.0
    MAX_FAILED_ROWS = 100
    MAX_EXECUTION_TIME = 300.0  # seconds
    
    # Query recent metrics
    recent_metrics = spark.sql("""
        SELECT 
            success_rate,
            total_rows_failed,
            avg_execution_time
        FROM eeg_lakehouse.gold.data_quality_dashboard
        WHERE date = current_date()
    """)
    
    if recent_metrics.count() > 0:
        metrics = recent_metrics.first()
        
        alerts = []
        
        if metrics.success_rate < MIN_SUCCESS_RATE:
            alerts.append(f"⚠️ Success rate ({metrics.success_rate}%) below threshold ({MIN_SUCCESS_RATE}%)")
        
        if metrics.total_rows_failed > MAX_FAILED_ROWS:
            alerts.append(f"⚠️ Failed rows ({metrics.total_rows_failed}) exceed threshold ({MAX_FAILED_ROWS})")
        
        if metrics.avg_execution_time > MAX_EXECUTION_TIME:
            alerts.append(f"⚠️ Avg execution time ({metrics.avg_execution_time}s) exceeds threshold ({MAX_EXECUTION_TIME}s)")
        
        if alerts:
            print("\n".join(alerts))
            # In production, send to notification service (email, Slack, PagerDuty, etc.)
            return False
        else:
            print("✅ All quality metrics within acceptable thresholds")
            return True
    else:
        print("⚠️ No metrics available for today")
        return False

# Run check
check_quality_thresholds()
```

### 6.2 Schedule Quality Checks

```python
# Use Databricks Jobs to schedule this notebook
# Configure in Databricks UI:
# - Job Name: "Daily Quality Check"
# - Schedule: "0 8 * * *" (8 AM daily)
# - Notebook: day09-instructions
# - Alerts: Configure email/Slack notifications on failure
```

---

## Part 7: Performance Monitoring

### 7.1 Monitor Query Performance

```python
# Analyze slow queries
slow_queries = spark.sql("""
    SELECT 
        table_name,
        operation,
        AVG(execution_time_seconds) as avg_time,
        MAX(execution_time_seconds) as max_time,
        COUNT(*) as execution_count
    FROM eeg_lakehouse.audit_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY table_name, operation
    HAVING AVG(execution_time_seconds) > 10
    ORDER BY avg_time DESC
""")

display(slow_queries)
```

### 7.2 Table Size and File Count Monitoring

```python
# Monitor table growth
def monitor_table_size(table_name: str):
    """Monitor table size and file count"""
    
    table_detail = spark.sql(f"DESCRIBE DETAIL {table_name}")
    
    display(
        table_detail.select(
            "name",
            "location",
            "sizeInBytes",
            "numFiles",
            col("sizeInBytes") / 1024 / 1024 / 1024).alias("sizeInGB"),
            "partitionColumns"
        )
    )
    
    # Check if optimization needed
    metrics = table_detail.first()
    if metrics.numFiles > 1000:
        print(f"⚠️ Table has {metrics.numFiles} files. Consider running OPTIMIZE.")
    
    return metrics

# Monitor key tables
for table in ["eeg_lakehouse.bronze.eeg_bronze", 
              "eeg_lakehouse.silver.eeg_silver",
              "eeg_lakehouse.gold.eeg_features"]:
    print(f"\n=== Monitoring {table} ===")
    monitor_table_size(table)
```

---

## Part 8: Enable Databricks SQL Analytics

### 8.1 Create SQL Dashboard

1. Navigate to **SQL** in Databricks workspace
2. Click **Create** → **Dashboard**
3. Name it "EEG Lakehouse Monitoring"
4. Add visualizations:

**Query 1: Daily Processing Volume**
```sql
SELECT 
    date(timestamp) as date,
    SUM(rows_processed) as total_rows
FROM eeg_lakehouse.audit_logs
WHERE timestamp >= current_date() - INTERVAL 30 DAYS
GROUP BY date(timestamp)
ORDER BY date DESC
```
*Visualization: Line chart*

**Query 2: Success Rate Trend**
```sql
SELECT 
    date,
    success_rate
FROM eeg_lakehouse.gold.data_quality_dashboard
WHERE date >= current_date() - INTERVAL 30 DAYS
ORDER BY date
```
*Visualization: Area chart*

**Query 3: Recent Failures**
```sql
SELECT 
    timestamp,
    pipeline_name,
    table_name,
    error_message
FROM eeg_lakehouse.audit_logs
WHERE status = 'FAILED'
    AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY timestamp DESC
LIMIT 20
```
*Visualization: Table*

### 8.2 Schedule Dashboard Refresh

1. Click **Schedule** in dashboard
2. Set refresh interval: **Every 1 hour**
3. Enable email delivery (optional)

---

## Part 9: Verification and Testing

### 9.1 Test Data Quality Expectations

```python
# Create test data with quality issues
test_data = spark.createDataFrame([
    ("P001", "R001", 64, 500, 3600, 0.95),  # Valid
    (None, "R002", 32, 250, 1800, 0.85),    # Invalid: null patient_id (should be dropped)
    ("P003", "R003", 512, 1000, 7200, 0.45), # Invalid: low quality score (quarantine)
    ("P004", "R004", 128, 5000, 600, 0.88),  # Valid
], schema="patient_id STRING, recording_id STRING, num_channels INT, " +
          "sampling_rate INT, duration_seconds INT, signal_quality_score DOUBLE")

# Write to bronze (will trigger DLT expectations)
test_data.write.format("delta").mode("append").save("/mnt/bronze/eeg_raw_test")

# Check quality metrics
print("\nVerifying expectations were applied...")
```

### 9.2 Verify Audit Logs

```python
# Check recent audit entries
recent_audits = spark.sql("""
    SELECT *
    FROM eeg_lakehouse.audit_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ORDER BY timestamp DESC
    LIMIT 10
""")

display(recent_audits)
```

**Expected Output**: Audit entries showing test operations

---

## Exercises

### Exercise 1: Custom Quality Metric
Implement a custom data quality metric that tracks:
- Percentage of records with complete metadata
- Average signal-to-noise ratio by hour
- Distribution of recording durations

### Exercise 2: Anomaly Detection
Create a query to detect anomalies:
- Sudden drops in processing volume (> 50% decrease)
- Spike in failure rates (> 10% failures)
- Unusual execution times (> 3 standard deviations)

### Exercise 3: Custom Alert System
Implement an alert function that:
- Sends Slack/email notification on quality threshold violations
- Includes relevant metrics and time range
- Provides actionable recommendations

---

## Best Practices

### Monitoring
- Set up automated daily quality checks
- Monitor pipeline execution times and optimize slow queries
- Track table growth and schedule regular OPTIMIZE operations
- Use descriptive log messages with relevant context

### Data Quality
- Implement expectations at each layer (bronze, silver, gold)
- Use `expect_or_drop` for critical validations
- Use `expect` with quarantine for warnings
- Document all quality rules and thresholds

### Logging
- Log all pipeline executions with metrics
- Include execution context (cluster ID, user, notebook)
- Retain logs for at least 90 days
- Implement log rotation for large tables

---

## Troubleshooting

### Issue: Expectations Not Applied
**Solution**: 
- Verify DLT pipeline configuration
- Check expectation syntax
- Review DLT event logs for errors

### Issue: Audit Table Growing Too Large
**Solution**:
```sql
-- Implement retention policy
DELETE FROM eeg_lakehouse.audit_logs
WHERE timestamp < current_timestamp() - INTERVAL 90 DAYS;

OPTIMIZE eeg_lakehouse.audit_logs;
VACUUM eeg_lakehouse.audit_logs RETAIN 168 HOURS;
```

### Issue: Dashboard Queries Slow
**Solution**:
- Create aggregated summary tables
- Add appropriate partitioning
- Use caching for frequently accessed queries

---

## Key Takeaways

1. ✅ Delta Live Tables provides built-in data quality expectations
2. ✅ Audit logging enables complete pipeline observability
3. ✅ Regular monitoring prevents data quality degradation
4. ✅ Automated alerts reduce time to detection for issues
5. ✅ Performance monitoring identifies optimization opportunities

---

## Next Steps

In **Day 10**, you will:
- Implement advanced machine learning feature engineering
- Build predictive models on EEG data
- Deploy ML models with MLflow
- Create automated retraining pipelines

---

**Day 9 Complete!** ✅

You now have comprehensive monitoring and data quality validation in place for your EEG lakehouse.
