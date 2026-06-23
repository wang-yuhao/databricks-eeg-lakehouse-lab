# Day 15: Apache Spark Performance Tuning — Partitioning, Broadcast Joins, Shuffle Optimisation, and Spark UI

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day15_spark_performance.py` |
| **Exam domains** | Domain 2 — ELT with Apache Spark; Domain 1 — Databricks Lakehouse Platform |
| **Time estimate** | 4–5 hours |
| **Prerequisite** | Days 1–14 completed; Silver table `eeg_lakehouse.silver.eeg_silver_advanced` contains at least 10 000 rows; Unity Catalog enabled |

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
   | Cluster name | `eeg-lab-day15` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS** |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled (Single user access mode) |

3. Click **Create compute** and wait for the cluster to reach the **Running** state.

### 1.5 Install Required Libraries

All required packages are included in DBR 14.3 LTS.

```python
# Cell 1: verify imports and Spark configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from delta.tables import DeltaTable
import time

print(f"Spark version  : {spark.version}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
print("Imports verified.")
```

### 1.6 Open and Attach the Notebook

1. Left sidebar → **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Open `day15_spark_performance.py`.
3. Click **Connect** → select `eeg-lab-day15`.
4. Confirm the cluster name in the toolbar.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Explain Spark's shuffle mechanism and its performance cost | Domain 2 — Spark execution model |
| Set `spark.sql.shuffle.partitions` to match dataset size | Domain 2 — Spark configuration |
| Trigger and observe a broadcast join to eliminate shuffle | Domain 2 — Spark joins |
| Read and interpret the Spark UI DAG, stages, and tasks | Domain 2 — Spark UI |
| Compare sort-merge join vs. broadcast join via query plans | Domain 2 — Spark joins |
| Apply repartition and coalesce correctly | Domain 2 — Partitioning |
| Cache a DataFrame and measure reuse benefit | Domain 2 — Spark caching |

---

## Section 3: Background

### Spark Execution Model

| Concept | Description |
|---|---|
| Job | Triggered by an action (e.g., `count()`, `write()`). A job consists of one or more stages. |
| Stage | A set of tasks that can be computed without a shuffle. Stages are separated by shuffle boundaries. |
| Task | The smallest unit of work. One task processes one partition. |
| Shuffle | Exchange of data across executors to satisfy a `groupBy`, `join`, or `orderBy`. Shuffles write to disk and are the primary source of Spark latency. |
| Partition | A logical division of data. The number of shuffle output partitions is controlled by `spark.sql.shuffle.partitions`. |

### Partition Sizing Reference

| Dataset size | Recommended `spark.sql.shuffle.partitions` |
|---|---|
| < 1 GB | 8–32 |
| 1–10 GB | 100–200 |
| 10–100 GB | 200–800 |
| > 100 GB | 800–2 000 |

### Join Strategy Selection

| Strategy | SQL hint | When to use | Shuffle cost |
|---|---|---|---|
| Sort-merge join | (default) | Large table × large table | High — both sides shuffle |
| Broadcast join | `BROADCAST(table)` | Large table × small table (< 10 MB by default) | None — small table broadcast to all executors |
| Shuffle-hash join | `SHUFFLE_HASH(table)` | Medium tables; no sort guarantee needed | Medium |
| Broadcast threshold | `spark.sql.autoBroadcastJoinThreshold` | Set bytes; -1 to disable auto-broadcast | Configurable |

---

## Section 4: Part 1 — Shuffle Partition Tuning

### Step 1 — Observe Default Shuffle Partition Count

```python
# Cell 2: inspect current shuffle partition configuration
default_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Default spark.sql.shuffle.partitions: {default_shuffle_partitions}")

# Perform a shuffle-inducing aggregation and measure duration
SILVER_TABLE = "eeg_lakehouse.silver.eeg_silver_advanced"

start = time.time()
spark.sql(f"""
    SELECT patient_id, channel_label, COUNT(*) AS record_count, AVG(amplitude_uv) AS avg_amplitude
    FROM {SILVER_TABLE}
    GROUP BY patient_id, channel_label
    ORDER BY avg_amplitude DESC
""").count()
elapsed_default = round(time.time() - start, 3)
print(f"Aggregation time with {default_shuffle_partitions} shuffle partitions: {elapsed_default}s")
```

### Step 2 — Tune Shuffle Partitions for the EEG Dataset

```python
# Cell 3: set shuffle partitions appropriate to the Silver table size
spark.conf.set("spark.sql.shuffle.partitions", "32")

start = time.time()
result_df = spark.sql(f"""
    SELECT patient_id, channel_label, COUNT(*) AS record_count, AVG(amplitude_uv) AS avg_amplitude
    FROM {SILVER_TABLE}
    GROUP BY patient_id, channel_label
    ORDER BY avg_amplitude DESC
""")
result_df.count()
elapsed_tuned = round(time.time() - start, 3)
print(f"Aggregation time with 32 shuffle partitions: {elapsed_tuned}s")
print(f"Improvement: {elapsed_default}s → {elapsed_tuned}s ({round((elapsed_default - elapsed_tuned) / elapsed_default * 100, 1)}% faster)")
```

### Step 3 — Inspect Output Partition Distribution

```python
# Cell 4: inspect per-partition record counts after aggregation (no cache, fresh plan)
spark.conf.set("spark.sql.shuffle.partitions", "32")

agg_df = spark.sql(f"""
    SELECT patient_id, channel_label, COUNT(*) AS record_count
    FROM {SILVER_TABLE}
    GROUP BY patient_id, channel_label
""")

partition_counts = agg_df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
print(f"Number of output partitions: {len(partition_counts)}")
print(f"Max records in one partition: {max(partition_counts)}")
print(f"Min records in one partition: {min(partition_counts)}")
print(f"Avg records per partition:    {sum(partition_counts) / len(partition_counts):.0f}")
```

---

## Section 5: Part 2 — Broadcast Joins

### Step 4 — Create a Small Lookup Table for Broadcast

```python
# Cell 5: create a patient metadata lookup table small enough for broadcast
spark.sql("""
    CREATE TABLE IF NOT EXISTS eeg_lakehouse.silver.patient_metadata (
        patient_id      STRING NOT NULL,
        age_years       INT,
        diagnosis_group STRING,
        recording_site  STRING
    )
    USING DELTA
    COMMENT 'Small patient metadata table used to demonstrate broadcast join.'
""")

# Populate with representative sample data derived from Silver table patients
spark.sql(f"""
    INSERT OVERWRITE eeg_lakehouse.silver.patient_metadata
    SELECT DISTINCT
        patient_id,
        CAST(RAND() * 40 + 20 AS INT)                        AS age_years,
        CASE WHEN RAND() > 0.5 THEN 'Epilepsy' ELSE 'Normal' END AS diagnosis_group,
        'Site-A'                                             AS recording_site
    FROM {SILVER_TABLE}
""")

meta_count = spark.sql("SELECT COUNT(*) AS cnt FROM eeg_lakehouse.silver.patient_metadata").first().cnt
print(f"Patient metadata rows: {meta_count}")
print(f"Broadcast threshold (bytes): {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
```

### Step 5 — Compare Sort-Merge Join vs. Broadcast Join

```python
# Cell 6: sort-merge join (default — disable auto-broadcast to force SMJ)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

start = time.time()
smj_df = spark.sql(f"""
    SELECT s.patient_id, s.channel_label, s.amplitude_uv, m.diagnosis_group
    FROM {SILVER_TABLE} s
    JOIN eeg_lakehouse.silver.patient_metadata m ON s.patient_id = m.patient_id
""")
smj_df.count()
elapsed_smj = round(time.time() - start, 3)
print(f"Sort-merge join time: {elapsed_smj}s")

# Reset to auto-broadcast and run broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

start = time.time()
bcast_df = spark.sql(f"""
    SELECT /*+ BROADCAST(m) */
        s.patient_id, s.channel_label, s.amplitude_uv, m.diagnosis_group
    FROM {SILVER_TABLE} s
    JOIN eeg_lakehouse.silver.patient_metadata m ON s.patient_id = m.patient_id
""")
bcast_df.count()
elapsed_bcast = round(time.time() - start, 3)
print(f"Broadcast join time : {elapsed_bcast}s")
print(f"Improvement         : {elapsed_smj}s → {elapsed_bcast}s")
```

### Step 6 — Verify Join Strategy in the Physical Plan

```python
# Cell 7: confirm BroadcastHashJoin appears in the physical plan
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

plan_df = spark.sql(f"""
    SELECT /*+ BROADCAST(m) */
        s.patient_id, s.channel_label, m.diagnosis_group
    FROM {SILVER_TABLE} s
    JOIN eeg_lakehouse.silver.patient_metadata m ON s.patient_id = m.patient_id
""")

plan_str = plan_df.explain(mode="formatted")
print(plan_str)
# Expected output contains: BroadcastHashJoin
```

---

## Section 6: Part 3 — Repartition vs. Coalesce

### Step 7 — Understand Repartition Behaviour

```python
# Cell 8: compare repartition (full shuffle) vs. coalesce (no shuffle)
silver_df = spark.table(SILVER_TABLE)

print(f"Original partitions: {silver_df.rdd.getNumPartitions()}")

# repartition: full shuffle, increases OR decreases partition count
repartitioned_df = silver_df.repartition(64)
print(f"After repartition(64): {repartitioned_df.rdd.getNumPartitions()}")

# coalesce: no shuffle, only reduces partition count by merging existing partitions
coalesced_df = silver_df.coalesce(8)
print(f"After coalesce(8): {coalesced_df.rdd.getNumPartitions()}")
```

### Step 8 — Repartition by Column for Write Optimisation

```python
# Cell 9: repartition by patient_id before writing to ensure data locality per patient
output_path = "dbfs:/user/hive/warehouse/eeg_lakehouse/silver/eeg_silver_repartitioned"

(
    silver_df
    .repartition(32, F.col("patient_id"))
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(output_path)
)

written_df = spark.read.format("delta").load(output_path)
print(f"Written partitions: {written_df.rdd.getNumPartitions()}")
print(f"Row count: {written_df.count()}")
```

---

## Section 7: Part 4 — Caching

### Step 9 — Cache a DataFrame and Measure Reuse Benefit

```python
# Cell 10: cache the Silver DataFrame and compare first vs. second scan
silver_df = spark.table(SILVER_TABLE)

# First scan — uncached
start = time.time()
silver_df.agg(F.count("*"), F.avg("amplitude_uv")).collect()
elapsed_cold = round(time.time() - start, 3)
print(f"Cold scan (no cache): {elapsed_cold}s")

# Cache the DataFrame
silver_df.cache()
silver_df.count()  # materialize cache

# Second scan — from cache
start = time.time()
silver_df.agg(F.count("*"), F.avg("amplitude_uv")).collect()
elapsed_cached = round(time.time() - start, 3)
print(f"Cached scan         : {elapsed_cached}s")
print(f"Cache speedup       : {elapsed_cold / max(elapsed_cached, 0.001):.1f}×")

# Always unpersist when done to release memory
silver_df.unpersist()
print("Cache released.")
```

---

## Section 8: Part 5 — Spark UI Navigation

### Step 10 — Open the Spark UI

1. In the Databricks cluster page, click the cluster name `eeg-lab-day15`.
2. Click **Spark UI** (opens in a new tab).
3. Navigate to the **Jobs** tab to find the jobs triggered by prior cells.

### Step 11 — Inspect a Shuffle Stage

```python
# Cell 11: trigger a clearly labelled job for Spark UI inspection
spark.sparkContext.setJobDescription("Day15-ShuffleInspect-GroupBy-patient_id")

spark.conf.set("spark.sql.shuffle.partitions", "32")
inspect_df = spark.sql(f"""
    SELECT patient_id, COUNT(*) AS n, AVG(amplitude_uv) AS avg_amp
    FROM {SILVER_TABLE}
    GROUP BY patient_id
""")
inspect_df.show(5)
```

> In the Spark UI **Stages** tab, locate the stage named **Exchange** (the shuffle). Note the **Shuffle Read** and **Shuffle Write** sizes. Reducing shuffle partitions from 200 to 32 should reduce the number of tasks in this stage proportionally.

### Step 12 — Read the Query Plan from Spark UI SQL Tab

```python
# Cell 12: trigger a query with a broadcast join for SQL tab inspection
spark.sparkContext.setJobDescription("Day15-BroadcastJoin-Inspect")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
spark.sql(f"""
    SELECT /*+ BROADCAST(m) */
        s.patient_id, COUNT(*) AS n
    FROM {SILVER_TABLE} s
    JOIN eeg_lakehouse.silver.patient_metadata m ON s.patient_id = m.patient_id
    GROUP BY s.patient_id
""").show(5)
```

> In the Spark UI **SQL / DataFrame** tab, find the query for `Day15-BroadcastJoin-Inspect`. Click it and inspect the visual DAG. Verify that `BroadcastHashJoin` appears rather than `SortMergeJoin`.

---

## Section 9: Exam Reference Tables

### Spark Configuration Reference — Day 15

| Configuration key | Default | Recommended for EEG lab | Effect |
|---|---|---|---|
| `spark.sql.shuffle.partitions` | `200` | `32` | Number of partitions created by shuffle operations |
| `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10 MB) | `10485760` | Tables smaller than this byte size are auto-broadcast |
| `spark.sql.adaptive.enabled` | `true` (DBR 14.3) | `true` | Enables Adaptive Query Execution (AQE) |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | AQE coalesces empty or small shuffle partitions at runtime |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | AQE detects and mitigates data skew in joins |

### DataFrame Operations That Trigger Shuffles

| Operation | Shuffle triggered | Notes |
|---|---|---|
| `groupBy().agg()` | Yes | Output partitions = `spark.sql.shuffle.partitions` |
| `orderBy()` | Yes | Global sort; use `sortWithinPartitions` for local sort |
| `join()` (sort-merge) | Yes (both sides) | Replaced by broadcast join when one side is small |
| `repartition(n)` | Yes | Always triggers a full shuffle |
| `coalesce(n)` | No | Merges partitions without shuffle; can only reduce |
| `distinct()` | Yes | Equivalent to `groupBy` on all columns |

### Certified Professional Exam Domain Mapping — Day 15 Topics

| Topic | Professional exam domain |
|---|---|
| `spark.sql.shuffle.partitions` tuning | Domain 2 — ELT with Apache Spark |
| Broadcast join hint and threshold | Domain 2 — ELT with Apache Spark |
| Sort-merge join vs. broadcast join | Domain 2 — ELT with Apache Spark |
| `repartition` vs. `coalesce` | Domain 2 — ELT with Apache Spark |
| DataFrame caching and `unpersist` | Domain 2 — ELT with Apache Spark |
| Spark UI: jobs, stages, tasks, DAG | Domain 2 — ELT with Apache Spark |
| Adaptive Query Execution (AQE) | Domain 2 — ELT with Apache Spark |

---

## Section 10: Self-Check Questions

Answer each question before proceeding to Day 16.

1. Why does reducing `spark.sql.shuffle.partitions` from 200 to 32 improve performance on a small dataset?
2. What is the difference between `repartition(n)` and `coalesce(n)`, and when should each be used?
3. Under what condition does Spark automatically apply a broadcast join without a hint?
4. What does Adaptive Query Execution do at runtime to address data skew?
5. When should you avoid caching a DataFrame?
6. In the Spark UI, what is the significance of a stage with a high **Shuffle Write** size?

**Reference answers:**

1. With 200 shuffle partitions and a small dataset, most of the 200 tasks have no data to process and spend their time on scheduling overhead. Reducing to 32 aligns the partition count with the actual data volume, eliminating empty task overhead and improving locality.
2. `repartition(n)` triggers a full shuffle and can increase or decrease the number of partitions. It is used when a specific partition count or distribution by a column is required. `coalesce(n)` merges existing partitions without a shuffle and can only reduce the partition count; it is used before writing to avoid many small output files.
3. Spark automatically applies a broadcast join when the estimated size of one side of the join is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). The size estimate is derived from table statistics, so `ANALYZE TABLE` must be run for accurate estimates.
4. AQE with `spark.sql.adaptive.skewJoin.enabled = true` detects partitions that are significantly larger than the median partition size. It splits those skewed partitions into smaller sub-partitions and replicates the non-skewed side to join against each sub-partition, distributing the work more evenly.
5. Avoid caching when: (a) the DataFrame is used only once and the data fits a single scan, (b) the cluster memory is too small to hold the cached data and spillage to disk would be slower than re-reading, or (c) the underlying data changes frequently and a stale cache would produce incorrect results.
6. A high **Shuffle Write** size in a stage indicates that a large amount of data is being serialised and written to disk for consumption by the downstream stage. This is a direct signal that the shuffle is expensive. Actions include increasing `spark.sql.shuffle.partitions`, enabling AQE, or eliminating the shuffle by broadcasting a small table.

---

## Section 11: Day 15 Summary

| Artifact | Tool | Exam domain |
|---|---|---|
| Shuffle partition tuning (`spark.sql.shuffle.partitions = 32`) | Spark configuration | Domain 2 |
| Broadcast join via `BROADCAST(m)` hint | Spark SQL | Domain 2 |
| Sort-merge join vs. broadcast join comparison | `explain(mode="formatted")` | Domain 2 |
| `repartition(32, patient_id)` before Delta write | DataFrame API | Domain 2 |
| DataFrame caching and `unpersist()` | DataFrame API | Domain 2 |
| Spark UI job description tagging | `setJobDescription` | Domain 2 |
| AQE configuration review | Spark configuration | Domain 2 |

**Next**: Day 16 covers Delta Live Tables advanced pipelines — event-driven triggers, expectations, table constraints, and pipeline monitoring.
