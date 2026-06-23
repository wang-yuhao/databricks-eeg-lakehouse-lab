# Day 9: Monitoring, Audit Logging, and Data Quality Validation

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day09_monitoring.py` |
| **Exam domains** | Domain 1 — Databricks Lakehouse Platform; Domain 4 — Building Data Pipelines |
| **Time estimate** | 4–5 hours |
| **Prerequisite** | Days 7–8 completed; DLT pipeline has run at least once; Bronze, Silver, and Gold tables exist |

---

## Section 1: Environment Setup

Complete every step in this section before opening any notebook cell. A reader starting from a blank Databricks workspace must follow these steps in order.

### 1.1 Create a GitHub Personal Access Token

1. Sign in to [github.com](https://github.com).
2. Navigate to **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. Click **Generate new token (classic)**.
4. Set the following fields:

   | Field | Value |
   |---|---|
   | Note | `databricks-eeg-lab` |
   | Expiration | 90 days |
   | Scopes | `repo` (full), `workflow` |

5. Copy the token immediately and store it in a password manager. It will not be displayed again.

### 1.2 Configure Databricks Git Integration

1. Click your username in the top-right corner of the workspace and select **User Settings**.
2. Select the **Git integration** tab.
3. Set the following:

   | Field | Value |
   |---|---|
   | Git provider | GitHub |
   | Git provider username | Your GitHub username |
   | Personal access token | Token from step 1.1 |

4. Click **Save**.

### 1.3 Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos**.
2. Click **Add repo**.
3. Set the following:

   | Field | Value |
   |---|---|
   | Git repository URL | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab` |
   | Git provider | GitHub |
   | Repo name | `databricks-eeg-lakehouse-lab` |

4. Click **Create repo**.

### 1.4 Create a Cluster with Unity Catalog Enabled

1. In the left sidebar, click **Compute** → **Create compute**.
2. Apply the following configuration:

   | Setting | Value |
   |---|---|
   | Cluster name | `eeg-lab-day09` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS** |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled |

3. Click **Create compute** and wait for the cluster to reach the **Running** state.

### 1.5 Install Required Libraries

No additional third-party libraries are required for Day 9. All utilities used are part of the Databricks Runtime 14.3 LTS (`delta`, `pyspark`, `uuid`, `datetime`, `time`).

Confirm availability:

```python
# Cell 1: verify standard library availability
import uuid, datetime, time
from delta.tables import DeltaTable
from pyspark.sql import functions as F
print("All required modules imported successfully.")
```

### 1.6 Open and Attach the Notebook

1. In the left sidebar, click **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Click `day09_monitoring.py` to open it.
3. Click **Connect** in the top-right and select `eeg-lab-day09`.
4. Confirm the cluster name appears in the toolbar.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Read and query DLT event logs | Domain 4 — Delta Live Tables |
| Add `@dlt.expect` decorators to Bronze and Silver tables | Domain 4 — Data quality with DLT |
| Create and maintain a Delta audit log table | Domain 1 — Lakehouse Platform |
| Query `DESCRIBE HISTORY` to inspect table operations | Domain 1 — Delta Lake operations |
| Build a data quality dashboard view | Domain 1 — Databricks SQL |
| Implement threshold-based alerting logic | Domain 4 — Building Data Pipelines |
| Monitor table size and file count | Domain 1 — Delta Lake optimization |

---

## Section 3: Background

### DLT Data Quality — Expectation Modes

| Decorator | Violation action | Pipeline continues? | Exam relevance |
|---|---|---|---|
| `@dlt.expect(name, condition)` | Row kept; violation logged | Yes | Warn-only metrics |
| `@dlt.expect_or_drop(name, condition)` | Row removed from output | Yes | Filtering corrupt records |
| `@dlt.expect_or_fail(name, condition)` | Pipeline update fails | No | Enforcing critical constraints |

### Delta Table Operations Captured in History

| Operation | `DESCRIBE HISTORY` keyword | Typical use |
|---|---|---|
| Append rows | `WRITE` | Auto Loader ingestion, DLT Bronze |
| Merge rows | `MERGE` | SCD Type 1/2 upserts in Silver |
| Schema evolution | `ADD COLUMNS` | Column additions during pipeline updates |
| Compaction | `OPTIMIZE` | File consolidation for query performance |
| Retention cleanup | `VACUUM` | Removing files outside retention window |
| Time travel read | — (reader-side only) | `versionAsOf`, `timestampAsOf` |

---

## Section 4: Part 1 — DLT Event Log Monitoring

### Step 1 — Locate the DLT Event Log Path

The DLT event log is a Delta table stored at the storage location configured for the pipeline. Retrieve the path from the DLT pipeline settings:

1. Navigate to **Workflows** → **Delta Live Tables**.
2. Click the `eeg_lakehouse_pipeline` entry.
3. Under **Pipeline settings**, copy the value of **Storage location**.

The event log is located at: `<storage_location>/system/events`.

Set this path as a variable for all subsequent cells:

```python
# Cell 2: set event log path
DLT_STORAGE_LOCATION = "/mnt/dlt/eeg_lakehouse_pipeline"  # replace with actual storage location
EVENT_LOG_PATH = f"{DLT_STORAGE_LOCATION}/system/events"

print(f"Event log path: {EVENT_LOG_PATH}")
```

---

### Step 2 — Read the DLT Event Log

```python
# Cell 3: read DLT event log
from pyspark.sql import functions as F

event_df = (
    spark.read.format("delta")
    .load(EVENT_LOG_PATH)
)

print(f"Event log schema:")
event_df.printSchema()

display(
    event_df
    .select(
        F.col("timestamp"),
        F.col("origin.update_id").alias("update_id"),
        F.col("event_type"),
        F.col("level"),
        F.col("message"),
    )
    .orderBy(F.col("timestamp").desc())
    .limit(50)
)
```

---

### Step 3 — Extract Pipeline Run Metrics

```python
# Cell 4: extract per-update quality metrics
pipeline_metrics = (
    event_df
    .filter(F.col("event_type") == "update_progress")
    .select(
        F.col("timestamp"),
        F.col("details.update_id").alias("update_id"),
        F.col("details.metrics.num_output_rows").alias("output_rows"),
        F.col("details.metrics.data_quality.dropped_records").alias("dropped_records"),
    )
    .orderBy(F.col("timestamp").desc())
)

display(pipeline_metrics)
```

Expected output: one row per pipeline update, showing how many rows were produced and how many were dropped by expectations.

---

## Section 5: Part 2 — DLT Data Quality Expectations

### Step 4 — Add Expectations to the DLT Pipeline Notebook

Open `notebooks/day07_dlt_pipeline.py` and update the Bronze and Silver table definitions as shown below. These cells replace the existing table definitions for `eeg_bronze` and `eeg_silver`.

```python
# DLT notebook cell: Bronze table with expectations
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="eeg_bronze",
    comment="Raw EEG recordings ingested from PhysioNet via Auto Loader with entry-level quality gates.",
)
@dlt.expect_or_drop("valid_record_timestamp", "record_timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL AND patient_id != ''")
@dlt.expect_or_fail("valid_file_format", "file_format IN ('edf', 'fif', 'set')")
def bronze_eeg():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema/bronze")
        .load("/mnt/bronze/eeg_raw")
        .withColumn("record_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
    )
```

```python
# DLT notebook cell: Silver table with advanced expectations
@dlt.table(
    name="eeg_silver",
    comment="Cleaned and validated EEG recordings with per-channel quality scoring.",
)
@dlt.expect_or_drop("valid_channel_count",   "num_channels > 0 AND num_channels <= 256")
@dlt.expect_or_drop("valid_sampling_rate",   "sampling_rate >= 100 AND sampling_rate <= 10000")
@dlt.expect_or_drop("valid_duration",        "duration_seconds > 0 AND duration_seconds <= 86400")
@dlt.expect("acceptable_signal_quality",     "signal_quality_score >= 0.5")
def silver_eeg():
    return (
        dlt.read_stream("eeg_bronze")
        .select(
            "patient_id",
            "recording_id",
            "num_channels",
            "sampling_rate",
            "duration_seconds",
            "signal_quality_score",
            "record_timestamp",
            F.current_timestamp().alias("processed_timestamp"),
        )
    )
```

```python
# DLT notebook cell: hourly quality metrics aggregate
@dlt.table(
    name="eeg_quality_metrics",
    comment="Hourly aggregated data quality metrics for Silver EEG recordings.",
)
def quality_metrics():
    return (
        dlt.read("eeg_silver")
        .groupBy(F.window("processed_timestamp", "1 hour"))
        .agg(
            F.count("*").alias("total_records"),
            F.avg("signal_quality_score").alias("avg_quality_score"),
            F.min("signal_quality_score").alias("min_quality_score"),
            F.max("signal_quality_score").alias("max_quality_score"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.sum(
                F.when(F.col("signal_quality_score") < 0.7, 1).otherwise(0)
            ).alias("low_quality_count"),
        )
    )
```

---

## Section 6: Part 3 — Custom Audit Log Table

### Step 5 — Create the Audit Log Table Schema

```sql
-- Cell 5: create audit log table
CREATE TABLE IF NOT EXISTS eeg_lakehouse.monitoring.audit_logs (
    log_id              STRING    COMMENT 'UUID for each log entry',
    log_timestamp       TIMESTAMP COMMENT 'Wall-clock time of the operation',
    pipeline_name       STRING    COMMENT 'Logical name of the pipeline or notebook',
    table_name          STRING    COMMENT 'Fully qualified table name (catalog.schema.table)',
    operation           STRING    COMMENT 'Operation type: READ, WRITE, MERGE, OPTIMIZE, VACUUM',
    rows_processed      BIGINT    COMMENT 'Number of rows successfully processed',
    rows_failed         BIGINT    COMMENT 'Number of rows that failed validation',
    execution_time_secs DOUBLE    COMMENT 'Wall-clock execution time in seconds',
    status              STRING    COMMENT 'Outcome: SUCCESS or FAILED',
    error_message       STRING    COMMENT 'Exception message if status is FAILED, NULL otherwise',
    metadata            MAP<STRING, STRING> COMMENT 'Arbitrary key-value pairs for run context'
)
USING DELTA
PARTITIONED BY (DATE(log_timestamp))
LOCATION 'abfss://eeg-lakehouse@<storage-account>.dfs.core.windows.net/monitoring/audit_logs'
COMMENT 'Audit trail for all EEG lakehouse pipeline operations. Retained for 90 days.';
```

> Replace `<storage-account>` with the Azure Data Lake Storage account name configured in your Unity Catalog external location. On AWS, use an `s3://` URI.

---

### Step 6 — Implement the Logging Utility Function

```python
# Cell 6: audit logging utility
import uuid
import time
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, DoubleType, MapType,
)

AUDIT_TABLE = "eeg_lakehouse.monitoring.audit_logs"

AUDIT_SCHEMA = StructType([
    StructField("log_id",              StringType(),    nullable=False),
    StructField("log_timestamp",       TimestampType(), nullable=False),
    StructField("pipeline_name",       StringType(),    nullable=False),
    StructField("table_name",          StringType(),    nullable=False),
    StructField("operation",           StringType(),    nullable=False),
    StructField("rows_processed",      LongType(),      nullable=False),
    StructField("rows_failed",         LongType(),      nullable=False),
    StructField("execution_time_secs", DoubleType(),    nullable=False),
    StructField("status",              StringType(),    nullable=False),
    StructField("error_message",       StringType(),    nullable=True),
    StructField("metadata",            MapType(StringType(), StringType()), nullable=True),
])


def log_pipeline_execution(
    pipeline_name: str,
    table_name: str,
    operation: str,
    rows_processed: int,
    rows_failed: int = 0,
    execution_time: float = 0.0,
    status: str = "SUCCESS",
    error_message: str = None,
    metadata: dict = None,
) -> None:
    """Append one row to the audit log Delta table."""
    cluster_id = spark.conf.get(
        "spark.databricks.clusterUsageTags.clusterId", "unknown"
    )
    context_metadata = {"cluster_id": cluster_id}
    if metadata:
        context_metadata.update(metadata)

    row = [(
        str(uuid.uuid4()),
        datetime.utcnow(),
        pipeline_name,
        table_name,
        operation,
        int(rows_processed),
        int(rows_failed),
        float(execution_time),
        status,
        error_message,
        context_metadata,
    )]

    (
        spark.createDataFrame(row, schema=AUDIT_SCHEMA)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )


print(f"log_pipeline_execution() registered. Target table: {AUDIT_TABLE}")
```

---

### Step 7 — Instrument a Pipeline Stage with Audit Logging

```python
# Cell 7: example instrumented read with audit logging
start_ts = time.time()

try:
    result_df = spark.read.table("eeg_lakehouse.silver.eeg_silver")
    row_count = result_df.count()
    elapsed = time.time() - start_ts

    log_pipeline_execution(
        pipeline_name="eeg_silver_read",
        table_name="eeg_lakehouse.silver.eeg_silver",
        operation="READ",
        rows_processed=row_count,
        execution_time=elapsed,
        metadata={"notebook": "day09_monitoring"},
    )
    print(f"Read {row_count} rows in {elapsed:.2f}s — logged to audit table.")

except Exception as exc:
    elapsed = time.time() - start_ts
    log_pipeline_execution(
        pipeline_name="eeg_silver_read",
        table_name="eeg_lakehouse.silver.eeg_silver",
        operation="READ",
        rows_processed=0,
        rows_failed=0,
        execution_time=elapsed,
        status="FAILED",
        error_message=str(exc),
        metadata={"notebook": "day09_monitoring"},
    )
    raise
```

---

## Section 7: Part 4 — Delta Table History Analysis

### Step 8 — Inspect Table History with `DESCRIBE HISTORY`

```python
# Cell 8: retrieve full history of Silver table
from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, "eeg_lakehouse.silver.eeg_silver")
history_df = silver_table.history()

display(
    history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics",
        "userMetadata",
    ).orderBy(F.col("version").desc())
)
```

---

### Step 9 — Analyze Write Operation Patterns

```python
# Cell 9: filter write operations and extract key metrics
write_ops = (
    history_df
    .filter(F.col("operation").isin(["WRITE", "MERGE", "UPDATE", "DELETE"]))
    .select(
        F.col("version"),
        F.col("timestamp"),
        F.col("operation"),
        F.col("operationMetrics.numOutputRows").cast("long").alias("rows_written"),
        F.col("operationMetrics.numFiles").cast("long").alias("files_written"),
        F.col("operationMetrics.executionTimeMs").cast("long").alias("execution_time_ms"),
    )
    .orderBy(F.col("timestamp").desc())
)

display(write_ops)
```

Expected output: a complete write audit trail showing version numbers, timestamps, row counts, and file counts for every destructive or additive operation.

---

## Section 8: Part 5 — Data Quality Dashboard

### Step 10 — Create the Quality Summary View

```sql
-- Cell 10: daily quality summary view
CREATE OR REPLACE VIEW eeg_lakehouse.monitoring.daily_quality_summary AS
SELECT
    DATE_TRUNC('day', log_timestamp)                                                     AS log_date,
    COUNT(*)                                                                             AS total_operations,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)                                 AS successful_ops,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END)                                 AS failed_ops,
    ROUND(
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                                                    AS success_rate_pct,
    ROUND(AVG(execution_time_secs), 2)                                                   AS avg_execution_time_secs,
    SUM(rows_processed)                                                                  AS total_rows_processed,
    SUM(rows_failed)                                                                     AS total_rows_failed
FROM eeg_lakehouse.monitoring.audit_logs
GROUP BY DATE_TRUNC('day', log_timestamp)
ORDER BY log_date DESC;
```

```sql
-- Cell 11: query last 7 days from summary view
SELECT *
FROM eeg_lakehouse.monitoring.daily_quality_summary
WHERE log_date >= CURRENT_DATE() - INTERVAL 7 DAYS;
```

---

## Section 9: Part 6 — Threshold-Based Alerting

### Step 11 — Implement Quality Threshold Checks

```python
# Cell 12: quality threshold alerting function
MIN_SUCCESS_RATE_PCT = 95.0
MAX_FAILED_ROWS       = 100
MAX_AVG_EXEC_TIME_SEC = 300.0


def check_quality_thresholds() -> bool:
    """
    Query today's quality summary and raise a printed alert for each violated threshold.
    Returns True if all thresholds pass, False otherwise.
    """
    today_metrics = spark.sql("""
        SELECT
            success_rate_pct,
            total_rows_failed,
            avg_execution_time_secs
        FROM eeg_lakehouse.monitoring.daily_quality_summary
        WHERE log_date = CURRENT_DATE()
    """)

    if today_metrics.count() == 0:
        print("WARNING: No metrics available for today. No pipeline operations logged yet.")
        return False

    m = today_metrics.first()
    alerts = []

    if m.success_rate_pct < MIN_SUCCESS_RATE_PCT:
        alerts.append(
            f"SUCCESS RATE {m.success_rate_pct:.1f}% is below the threshold of {MIN_SUCCESS_RATE_PCT}%."
        )
    if m.total_rows_failed > MAX_FAILED_ROWS:
        alerts.append(
            f"FAILED ROWS {m.total_rows_failed} exceed the threshold of {MAX_FAILED_ROWS}."
        )
    if m.avg_execution_time_secs > MAX_AVG_EXEC_TIME_SEC:
        alerts.append(
            f"AVG EXECUTION TIME {m.avg_execution_time_secs:.1f}s exceeds the threshold of {MAX_AVG_EXEC_TIME_SEC}s."
        )

    if alerts:
        print("DATA QUALITY ALERT — The following thresholds were violated:")
        for alert in alerts:
            print(f"  [ALERT] {alert}")
        # In production, replace print statements with calls to a notification service
        # (e.g., requests.post to a Slack webhook or Azure Logic Apps endpoint).
        return False

    print("All quality metrics are within acceptable thresholds.")
    return True


check_quality_thresholds()
```

---

## Section 10: Part 7 — Performance and Table Size Monitoring

### Step 12 — Monitor Table Size and File Count

```python
# Cell 13: table size and file count monitoring
def monitor_table_size(fully_qualified_table_name: str) -> None:
    """
    Print size and file count for a Delta table.
    Prints an OPTIMIZE recommendation when file count exceeds 1 000.
    """
    detail_df = spark.sql(f"DESCRIBE DETAIL {fully_qualified_table_name}")
    row = detail_df.first()

    size_gb = row.sizeInBytes / (1024 ** 3)
    print(f"Table  : {fully_qualified_table_name}")
    print(f"  Size : {size_gb:.3f} GB  ({row.sizeInBytes:,} bytes)")
    print(f"  Files: {row.numFiles}")
    print(f"  Partitions: {row.partitionColumns}")

    if row.numFiles > 1000:
        print(
            f"  RECOMMENDATION: {row.numFiles} files detected. "
            f"Run OPTIMIZE {fully_qualified_table_name} ZORDER BY (patient_id) to compact."
        )
    print()


for tbl in [
    "eeg_lakehouse.bronze.eeg_bronze",
    "eeg_lakehouse.silver.eeg_silver",
    "eeg_lakehouse.gold.subject_features",
]:
    monitor_table_size(tbl)
```

---

### Step 13 — Apply Retention Policy to Audit Logs

```sql
-- Cell 14: delete audit log entries older than 90 days
DELETE FROM eeg_lakehouse.monitoring.audit_logs
WHERE log_timestamp < CURRENT_TIMESTAMP() - INTERVAL 90 DAYS;

OPTIMIZE eeg_lakehouse.monitoring.audit_logs;

VACUUM eeg_lakehouse.monitoring.audit_logs RETAIN 168 HOURS;
```

> **Exam note**: `VACUUM` permanently removes files from storage. The default retention period is 7 days (168 hours). Setting `RETAIN 0 HOURS` with `spark.databricks.delta.retentionDurationCheck.enabled = false` is required for immediate cleanup but voids time-travel capability for all versions within the retention window.

---

## Section 11: Part 8 — Databricks SQL Monitoring Dashboard

### Step 14 — Create a SQL Analytics Dashboard

1. Navigate to **SQL** → **Dashboards** → **Create dashboard**.
2. Name the dashboard `EEG Lakehouse — Operations Monitor`.
3. Add the following queries as individual widgets:

**Widget 1 — Daily Processing Volume (line chart)**

```sql
SELECT
    log_date,
    total_rows_processed
FROM eeg_lakehouse.monitoring.daily_quality_summary
WHERE log_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY log_date;
```

**Widget 2 — Success Rate Trend (area chart)**

```sql
SELECT
    log_date,
    success_rate_pct
FROM eeg_lakehouse.monitoring.daily_quality_summary
WHERE log_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY log_date;
```

**Widget 3 — Recent Failures (table)**

```sql
SELECT
    log_timestamp,
    pipeline_name,
    table_name,
    error_message
FROM eeg_lakehouse.monitoring.audit_logs
WHERE status = 'FAILED'
  AND log_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY log_timestamp DESC
LIMIT 20;
```

4. Click **Schedule** and set the refresh interval to **Every 1 hour**.

---

## Section 12: Exam Reference Tables

### Delta Lake Commands — Maintenance Operations

| Command | Purpose | Exam consideration |
|---|---|---|
| `OPTIMIZE <table>` | Compacts small files into larger ones | Improves read performance; does not delete data |
| `OPTIMIZE <table> ZORDER BY (col)` | Compacts and co-locates data by column | Accelerates range and equality filters on `col` |
| `VACUUM <table> RETAIN N HOURS` | Deletes data files outside the retention window | Cannot time-travel past the retention point after vacuum |
| `DESCRIBE HISTORY <table>` | Returns version history and operation metadata | Used for audit trails and time-travel planning |
| `RESTORE TABLE <table> TO VERSION AS OF N` | Reverts table to a prior version | Requires version to be within the retention window |

### Data Quality — Monitoring Architecture Pattern

| Layer | Quality mechanism | Storage location |
|---|---|---|
| Bronze | `expect_or_drop` on NULL keys, `expect_or_fail` on invalid format | DLT event log |
| Silver | `expect_or_drop` on range violations, `expect` (warn) on score | DLT event log + `eeg_quality_metrics` |
| Gold | SQL assertions in Gold notebook cells | `audit_logs` Delta table |
| Dashboard | `daily_quality_summary` view | Databricks SQL |

### Certified Professional Exam Domain Mapping — Day 9 Topics

| Topic | Professional exam domain |
|---|---|
| DLT event log queries | Domain 4: Delta Live Tables |
| `@dlt.expect*` decorators | Domain 4: Data quality with DLT |
| `DESCRIBE HISTORY` | Domain 1: Delta Lake operations |
| `OPTIMIZE` and `VACUUM` | Domain 1: Delta Lake optimization |
| Audit Delta table design | Domain 1: Lakehouse Platform |
| Databricks SQL dashboard | Domain 1: Databricks SQL |

---

## Section 13: Self-Check Questions

1. What is the difference between `@dlt.expect_or_drop` and `@dlt.expect_or_fail`?
2. How do you locate the DLT event log path for a pipeline?
3. What `DESCRIBE HISTORY` operation keyword indicates that Auto Loader appended new files to a Bronze table?
4. Why is a `MAP<STRING, STRING>` column a good choice for audit log metadata?
5. What happens if you run `VACUUM` with a retention window shorter than the default 7 days without disabling the retention check?
6. How should threshold alerts be delivered in a production environment?

**Reference answers:**

1. `expect_or_drop` removes only the rows that violate the condition and allows the pipeline update to continue. `expect_or_fail` immediately fails the entire pipeline update the moment any row violates the condition.
2. Navigate to **Workflows** → **Delta Live Tables**, open the pipeline, and copy the **Storage location** from the pipeline settings. The event log is at `<storage_location>/system/events`.
3. The `WRITE` keyword in the `operation` column of `DESCRIBE HISTORY` output corresponds to an append from Auto Loader or a direct `df.write.mode("append")` call.
4. `MAP<STRING, STRING>` allows arbitrary key-value pairs to be stored without requiring schema changes when new context fields (e.g., a new orchestration system ID) need to be added to log entries.
5. Databricks raises an `AnalysisException` stating that the retention period is too short. The check must be disabled explicitly with `spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")` before the `VACUUM` command.
6. In production, replace `print()` statements with calls to a notification endpoint: a Slack incoming webhook via `requests.post`, an Azure Logic Apps HTTP trigger, or a PagerDuty Events API call, wrapped in a Databricks Job task with an email notification on task failure.

---

## Section 14: Day 9 Summary

| Artifact | Tool | Medallion layer | Exam domain |
|---|---|---|---|
| DLT event log reader | PySpark Delta read | — | Domain 4 |
| Bronze expectations | `@dlt.expect_or_drop`, `@dlt.expect_or_fail` | Bronze | Domain 4 |
| Silver expectations | `@dlt.expect_or_drop`, `@dlt.expect` | Silver | Domain 4 |
| Hourly quality metrics table | DLT `@dlt.table` aggregate | Silver | Domain 4 |
| Audit log Delta table | Delta append via PySpark | Monitoring | Domain 1 |
| `DESCRIBE HISTORY` analysis | DeltaTable Python API | All layers | Domain 1 |
| Daily quality summary view | Databricks SQL | Monitoring | Domain 1 |
| Threshold alerting function | Python | Monitoring | Domain 4 |
| Table size monitoring | `DESCRIBE DETAIL` | All layers | Domain 1 |
| Operations dashboard | Databricks SQL | Monitoring | Domain 1 |

**Next**: Day 10 covers end-to-end machine learning on EEG features using the Databricks Feature Store, MLflow experiment tracking, the Model Registry, batch inference, and automated retraining pipelines.
