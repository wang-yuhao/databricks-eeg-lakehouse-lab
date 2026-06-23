# Day 14: Delta Lake Performance Optimisation — Z-Ordering, Liquid Clustering, and Data Skipping

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day14_delta_performance.py` |
| **Exam domains** | Domain 1 — Databricks Lakehouse Platform; Domain 2 — ELT with Apache Spark |
| **Time estimate** | 4–5 hours |
| **Prerequisite** | Days 1–13 completed; Silver and Gold tables contain at least 1 000 rows; Unity Catalog enabled |

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

5. Copy the token immediately and store it in a password manager.

### 1.2 Configure Databricks Git Integration

1. Click your username (top-right) → **User Settings** → **Git integration**.
2. Enter the following:

   | Field | Value |
   |---|---|
   | Git provider | GitHub |
   | Git provider username | Your GitHub username |
   | Personal access token | Token from step 1.1 |

3. Click **Save**.

### 1.3 Clone the Repository into Databricks Repos

1. Left sidebar → **Repos** → **Add repo**.
2. Enter the following:

   | Field | Value |
   |---|---|
   | Git repository URL | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab` |
   | Git provider | GitHub |
   | Repo name | `databricks-eeg-lakehouse-lab` |

3. Click **Create repo**.

### 1.4 Create a Cluster with Unity Catalog Enabled

1. Left sidebar → **Compute** → **Create compute**.
2. Apply the following configuration:

   | Setting | Value |
   |---|---|
   | Cluster name | `eeg-lab-day14` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS** |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled (Single user access mode) |

3. Click **Create compute** and wait for the cluster to reach the **Running** state.

### 1.5 Install Required Libraries

All required packages (`delta`, `pyspark`) are included in DBR 14.3 LTS.

```python
# Cell 1: verify Delta and PySpark imports
from delta.tables import DeltaTable
from pyspark.sql import functions as F
print(f"Delta version  : {spark.conf.get('spark.databricks.delta.preview.enabled', 'enabled')}")
print(f"Spark version  : {spark.version}")
print("Imports verified.")
```

### 1.6 Open and Attach the Notebook

1. Left sidebar → **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Open `day14_delta_performance.py`.
3. Click **Connect** → select `eeg-lab-day14`.
4. Confirm the cluster name in the toolbar.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Measure file count and table size before and after `OPTIMIZE` | Domain 1 — Delta Lake operations |
| Apply `OPTIMIZE ... ZORDER BY` to accelerate filter queries | Domain 1 — Delta Lake optimization |
| Configure and enable liquid clustering | Domain 1 — Liquid clustering |
| Explain data skipping and how Delta min/max statistics enable it | Domain 1 — Data skipping |
| Apply `VACUUM` with correct retention settings | Domain 1 — Delta Lake operations |
| Benchmark query performance before and after optimization | Domain 2 — Spark query performance |
| Read Delta table properties and understand their effect | Domain 1 — Table properties |

---

## Section 3: Background

### Delta Lake Optimization Techniques

| Technique | SQL command | Effect | Best for |
|---|---|---|---|
| File compaction | `OPTIMIZE <table>` | Merges many small files into fewer large files | Tables with many small-file writes (streaming, Auto Loader) |
| Z-Ordering | `OPTIMIZE <table> ZORDER BY (col1, col2)` | Co-locates related data within files for efficient data skipping | Range and equality filters on high-cardinality columns |
| Liquid clustering | `CLUSTER BY (col)` at table creation | Automatic, incremental re-clustering without full rewrites | Dynamic query patterns; replaces static partitioning |
| Auto Optimize | Table properties `optimizeWrite` + `autoCompact` | Automatically compacts during writes | Streaming and DLT pipelines |

### Data Skipping

Delta Lake records the minimum and maximum value of each column in each data file. When a query contains a filter predicate (e.g., `WHERE patient_id = 'P0042'`), the Delta engine compares the filter value against the per-file min/max statistics and skips files that cannot contain matching rows. Z-Ordering increases the effectiveness of data skipping by ensuring that similar values of the Z-Ordered columns are stored in the same files.

### Liquid Clustering vs. Partitioning

| Dimension | Static partitioning | Liquid clustering |
|---|---|---|
| Column cardinality | Low (date, region) | High (patient_id, recording_id) |
| Flexibility | Fixed at table creation | Clustering columns can be changed without rewriting the table |
| Small-file risk | High for high-cardinality columns | Managed automatically by the Databricks runtime |
| Incremental optimization | Requires `OPTIMIZE` manually | Optimized incrementally during writes |

---

## Section 4: Part 1 — Baseline Measurement

### Step 1 — Measure Table Statistics Before Optimization

```python
# Cell 2: capture baseline file count, size, and query time for Silver table
import time

SILVER_TABLE = "eeg_lakehouse.silver.eeg_silver_advanced"

def measure_table(table_name: str, label: str = "") -> dict:
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    return {
        "label":      label,
        "num_files":  detail.numFiles,
        "size_mb":    round(detail.sizeInBytes / 1_048_576, 2),
        "partitions": detail.partitionColumns,
    }


def benchmark_query(table_name: str, label: str = "") -> dict:
    start = time.time()
    count = spark.sql(f"""
        SELECT patient_id, COUNT(*) AS rec_count
        FROM {table_name}
        WHERE patient_id = 'P0042'
          AND signal_quality >= 0.8
        GROUP BY patient_id
    """).count()
    elapsed = round(time.time() - start, 3)
    return {"label": label, "query_time_secs": elapsed, "rows": count}


baseline_stats = measure_table(SILVER_TABLE, "Before OPTIMIZE")
baseline_perf  = benchmark_query(SILVER_TABLE, "Before OPTIMIZE")

print("=== Baseline ===")
for k, v in baseline_stats.items():
    print(f"  {k:<20}: {v}")
print(f"  {'query_time_secs':<20}: {baseline_perf['query_time_secs']}")
```

---

### Step 2 — Inspect Delta File Distribution

```python
# Cell 3: inspect file sizes in the Silver table
file_df = spark.sql(f"""
    SELECT
        size / 1048576.0 AS size_mb,
        stats
    FROM
        (DESCRIBE DETAIL {SILVER_TABLE})
""")

# Use the Delta log files API for detailed file listing
silver_delta = DeltaTable.forName(spark, SILVER_TABLE)
detail_df    = silver_delta.detail()

print(f"Number of files  : {detail_df.first().numFiles}")
print(f"Table size (MB)  : {detail_df.first().sizeInBytes / 1_048_576:.2f}")
```

---

## Section 5: Part 2 — OPTIMIZE and Z-Ordering

### Step 3 — Run OPTIMIZE with Z-Ordering

```sql
-- Cell 4: compact files and Z-Order by the most-filtered columns
OPTIMIZE eeg_lakehouse.silver.eeg_silver_advanced
    ZORDER BY (patient_id, signal_quality);
```

Expected output: a JSON summary showing `numFilesAdded`, `numFilesRemoved`, and `filesAdded.totalSize`.

---

### Step 4 — Measure Statistics After Optimization

```python
# Cell 5: compare file count, size, and query time after OPTIMIZE
optimized_stats = measure_table(SILVER_TABLE, "After OPTIMIZE")
optimized_perf  = benchmark_query(SILVER_TABLE, "After OPTIMIZE")

print("=== After OPTIMIZE ZORDER BY ===")
for k, v in optimized_stats.items():
    print(f"  {k:<20}: {v}")
print(f"  {'query_time_secs':<20}: {optimized_perf['query_time_secs']}")

print("\n=== Improvement ===")
print(f"  File count reduction  : {baseline_stats['num_files']} → {optimized_stats['num_files']}")
print(f"  Query time improvement: {baseline_perf['query_time_secs']}s → {optimized_perf['query_time_secs']}s")
```

---

### Step 5 — Inspect the OPTIMIZE History

```python
# Cell 6: view OPTIMIZE in DESCRIBE HISTORY
history_df = spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}")
display(
    history_df
    .filter(F.col("operation") == "OPTIMIZE")
    .select("version", "timestamp", "operation", "operationMetrics")
    .orderBy(F.col("version").desc())
    .limit(5)
)
```

---

## Section 6: Part 3 — Liquid Clustering

### Step 6 — Create a Table with Liquid Clustering

```python
# Cell 7: create a new Silver table with liquid clustering (DBR 13.3 LTS+)
spark.sql("""
    CREATE TABLE IF NOT EXISTS eeg_lakehouse.silver.eeg_silver_clustered
    CLUSTER BY (patient_id, ingestion_date)
    COMMENT 'Silver EEG table using liquid clustering on patient_id and ingestion_date.'
    AS
    SELECT *
    FROM eeg_lakehouse.silver.eeg_silver_advanced
""")

print("Liquid-clustered table created.")
print(spark.sql("DESCRIBE EXTENDED eeg_lakehouse.silver.eeg_silver_clustered").filter(
    F.col("col_name") == "Clustering Information"
).first())
```

---

### Step 7 — Trigger Incremental Clustering

```sql
-- Cell 8: run OPTIMIZE to apply incremental clustering
-- For liquid-clustered tables, OPTIMIZE applies clustering without requiring a ZORDER BY clause
OPTIMIZE eeg_lakehouse.silver.eeg_silver_clustered;
```

### Step 8 — Verify Clustering Effectiveness

```python
# Cell 9: benchmark the clustered table vs. the Z-Ordered table
clustered_perf = benchmark_query(
    "eeg_lakehouse.silver.eeg_silver_clustered",
    "Liquid Clustered",
)

print(f"Z-Ordered query time  : {optimized_perf['query_time_secs']}s")
print(f"Liquid clustered time : {clustered_perf['query_time_secs']}s")
```

---

## Section 7: Part 4 — Auto Optimize Table Properties

### Step 9 — Enable Auto Optimize on the Silver Table

```sql
-- Cell 10: enable auto-optimize properties for streaming write performance
ALTER TABLE eeg_lakehouse.silver.eeg_silver_advanced
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    );

DESCRIBE EXTENDED eeg_lakehouse.silver.eeg_silver_advanced;
```

---

### Step 10 — Verify Table Properties

```python
# Cell 11: read table properties programmatically
props = (
    spark.sql("SHOW TBLPROPERTIES eeg_lakehouse.silver.eeg_silver_advanced")
    .filter(F.col("key").isin([
        "delta.autoOptimize.optimizeWrite",
        "delta.autoOptimize.autoCompact",
    ]))
)
display(props)
```

---

## Section 8: Part 5 — VACUUM and Retention Management

### Step 11 — Preview Files to Be Deleted

```python
# Cell 12: dry-run VACUUM to see which files would be removed (no data is deleted)
spark.conf.set("spark.databricks.delta.vacuum.logging.enabled", "true")

vacuum_preview = spark.sql(
    f"VACUUM {SILVER_TABLE} RETAIN 168 HOURS DRY RUN"
)
display(vacuum_preview)
```

### Step 12 — Execute VACUUM

```python
# Cell 13: run VACUUM with 7-day (168-hour) retention
# This permanently removes files that are no longer referenced by the current table version
# and are older than the retention window. Time travel beyond 168 hours will not be possible.
spark.sql(f"VACUUM {SILVER_TABLE} RETAIN 168 HOURS")
print("VACUUM completed. Files outside the 168-hour retention window have been deleted.")
```

> **Exam note**: The default retention period is 7 days (168 hours). Running `VACUUM` with a shorter retention window requires setting `spark.databricks.delta.retentionDurationCheck.enabled` to `false`. Never run `VACUUM RETAIN 0 HOURS` in production unless time travel is deliberately disabled.

---

## Section 9: Part 6 — Statistics and Data Skipping

### Step 13 — Compute Column Statistics

```sql
-- Cell 14: compute and update table statistics for the query optimizer
ANALYZE TABLE eeg_lakehouse.silver.eeg_silver_advanced COMPUTE STATISTICS FOR ALL COLUMNS;
```

### Step 14 — Inspect Min/Max Statistics from the Delta Log

```python
# Cell 15: read per-file min/max statistics directly from the Delta transaction log
import json

SILVER_LOCATION = spark.sql(
    f"DESCRIBE DETAIL {SILVER_TABLE}"
).first().location

log_df = (
    spark.read.json(f"{SILVER_LOCATION}/_delta_log/*.json")
    .filter(F.col("add").isNotNull())
    .select(
        F.col("add.path").alias("file_path"),
        F.col("add.stats").alias("stats_json"),
    )
)

stats_df = log_df.withColumn("stats", F.from_json(
    F.col("stats_json"),
    "struct<numRecords:long, minValues:map<string,string>, maxValues:map<string,string>>",
)).select(
    F.col("file_path"),
    F.col("stats.numRecords").alias("num_records"),
    F.col("stats.minValues")["patient_id"].alias("patient_id_min"),
    F.col("stats.maxValues")["patient_id"].alias("patient_id_max"),
    F.col("stats.minValues")["signal_quality"].alias("signal_quality_min"),
    F.col("stats.maxValues")["signal_quality"].alias("signal_quality_max"),
)

display(stats_df.orderBy("file_path").limit(10))
```

---

## Section 10: Part 7 — Full Performance Comparison Report

### Step 15 — Generate a Comparison Summary

```python
# Cell 16: print a formatted comparison report
report_rows = [
    {"Configuration": "Unoptimized",     **baseline_stats,   **baseline_perf},
    {"Configuration": "OPTIMIZE ZORDER", **optimized_stats,  **optimized_perf},
    {"Configuration": "Liquid Clustered",**measure_table("eeg_lakehouse.silver.eeg_silver_clustered", "Liquid"), **clustered_perf},
]

print(f"{'Configuration':<22} {'Files':>8} {'Size MB':>10} {'Query (s)':>12}")
print("-" * 55)
for r in report_rows:
    print(
        f"{r['Configuration']:<22} "
        f"{r['num_files']:>8} "
        f"{r['size_mb']:>10.2f} "
        f"{r['query_time_secs']:>12.3f}"
    )
```

---

## Section 11: Exam Reference Tables

### Delta Lake Optimization Command Reference

| Command | Purpose | Side effect |
|---|---|---|
| `OPTIMIZE <table>` | Compact small files into target file size (~1 GB) | Adds new compacted files; old files marked for deletion |
| `OPTIMIZE <table> ZORDER BY (cols)` | Compact and co-locate data by column values | Rewrites files; increases data skipping effectiveness |
| `VACUUM <table> RETAIN N HOURS` | Delete files outside retention window | Permanently removes old file versions; disables time travel beyond N hours |
| `ANALYZE TABLE ... FOR ALL COLUMNS` | Compute column statistics for the query optimizer | Improves cost-based optimizer plan selection |
| `DESCRIBE DETAIL <table>` | Return metadata: file count, size, partition columns | Useful for pre/post optimization comparison |
| `RESTORE TABLE ... TO VERSION AS OF N` | Revert table to a prior version | Requires the version to be within the VACUUM retention window |

### Table Property Reference

| Property | Value | Effect |
|---|---|---|
| `delta.autoOptimize.optimizeWrite` | `true` | Coalesces small shuffle partitions into optimal files during writes |
| `delta.autoOptimize.autoCompact` | `true` | Runs a background compaction after write operations |
| `delta.logRetentionDuration` | `interval 30 days` | How long the Delta transaction log is retained |
| `delta.deletedFileRetentionDuration` | `interval 7 days` | Minimum age of deleted files before VACUUM can remove them |
| `pipelines.reset.allowed` | `true` | Allows DLT full refresh on the table |

### Certified Professional Exam Domain Mapping — Day 14 Topics

| Topic | Professional exam domain |
|---|---|
| `OPTIMIZE` and file compaction | Domain 1 — Delta Lake optimization |
| `ZORDER BY` and data skipping | Domain 1 — Delta Lake optimization |
| Liquid clustering | Domain 1 — Delta Lake optimization |
| `VACUUM` and retention management | Domain 1 — Delta Lake operations |
| `DESCRIBE DETAIL` and `DESCRIBE HISTORY` | Domain 1 — Delta Lake operations |
| Spark query plan analysis | Domain 2 — ELT with Apache Spark |

---

## Section 12: Self-Check Questions

Answer each question before proceeding to Day 15.

1. What is the difference between `OPTIMIZE` and `OPTIMIZE ZORDER BY`?
2. Why does Z-Ordering improve query performance for filter predicates?
3. Under what circumstances should liquid clustering be preferred over static partitioning?
4. What happens to the time-travel capability of a table after running `VACUUM RETAIN 24 HOURS`?
5. What does `delta.autoOptimize.optimizeWrite` do, and when is it most beneficial?
6. How do Delta min/max statistics enable data skipping?

**Reference answers:**

1. `OPTIMIZE` alone performs file compaction only: many small files are merged into fewer large files, reducing the overhead of opening many files during a scan. `OPTIMIZE ZORDER BY` additionally sorts data within the compacted files by the specified columns, so that rows with similar values of those columns are physically co-located, enabling effective data skipping.
2. Z-Ordering ensures that rows with similar values of the Z-Ordered column(s) are stored in the same data files. Because Delta records the min and max value of each column per file, a filter predicate such as `WHERE patient_id = 'P0042'` can skip all files whose `patient_id` min-max range does not include `'P0042'`, dramatically reducing I/O.
3. Liquid clustering is preferred when the query access pattern is varied or not known at table creation time, when the clustering column has high cardinality (which would create too many partitions with static partitioning), or when the clustering requirements change over time and the table cannot be recreated.
4. After `VACUUM RETAIN 24 HOURS`, all Delta table versions older than 24 hours are permanently removed. Time travel queries such as `SELECT * FROM table VERSION AS OF 5` will fail with an error if version 5 corresponds to a commit older than 24 hours.
5. `optimizeWrite` coalesces the output shuffle partitions of a write operation before flushing them to storage. This prevents many very small files from being created during high-throughput streaming or DLT writes, where Spark may produce hundreds of shuffle partitions.
6. Delta records the minimum and maximum value of each column in each Parquet file in the transaction log. When the query engine evaluates a filter predicate, it compares the filter value against the per-file min-max statistics and marks any file whose range cannot contain matching rows as “skipped”. Skipped files are not opened or read, reducing I/O proportionally to the fraction of files that are skipped.

---

## Section 13: Day 14 Summary

| Artifact | Tool | Layer | Exam domain |
|---|---|---|---|
| Baseline file-count measurement | `DESCRIBE DETAIL` | Silver | Domain 1 |
| `OPTIMIZE ZORDER BY (patient_id, signal_quality)` | Delta SQL | Silver | Domain 1 |
| Liquid-clustered Silver table | `CLUSTER BY` at table creation | Silver | Domain 1 |
| Auto Optimize table properties | `ALTER TABLE SET TBLPROPERTIES` | Silver | Domain 1 |
| `VACUUM RETAIN 168 HOURS` | Delta SQL | Silver | Domain 1 |
| Column statistics computation | `ANALYZE TABLE` | Silver | Domain 2 |
| Delta log min/max statistics reader | PySpark JSON read | Silver | Domain 1 |
| Before/after performance comparison | Python timing | Silver | Domain 2 |

**Next**: Day 15 covers Apache Spark performance tuning — partitioning strategies, broadcast joins, shuffle optimisation, and the Spark UI for query plan analysis.
