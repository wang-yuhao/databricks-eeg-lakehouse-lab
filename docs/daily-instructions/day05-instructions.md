# Day 5: Delta Lake Internals — Time Travel, OPTIMIZE, ZORDER, VACUUM

**Notebook**: `notebooks/day05_delta_internals.py`
**Exam domains**: Delta Lake (Domain 2 — Delta with Spark SQL & Python)
**Time estimate**: 2–3 hours
**Prerequisite**: Day 4 completed; Silver table `eeg_lakehouse.silver.cleaned_epochs` exists

---

## Environment Setup

Complete every sub-section below before executing any notebook cell. A reader starting from a blank Databricks workspace must follow these steps in order.

### 1. Create a GitHub Personal Access Token (PAT)

1. Navigate to [https://github.com/settings/tokens](https://github.com/settings/tokens) and sign in.
2. Click **Generate new token (classic)**.
3. Set **Note** to `databricks-eeg-lab`.
4. Set **Expiration** to `90 days`.
5. Select the following scopes: `repo` (full), `workflow`.
6. Click **Generate token** and copy the token value immediately — it will not be shown again.

### 2. Configure Databricks Git Integration

1. In your Databricks workspace, click your username in the top-right corner and select **User Settings**.
2. Click the **Git Integration** tab.
3. Set **Git provider** to `GitHub`.
4. Paste your GitHub PAT into the **Token** field.
5. Enter your GitHub username in the **Username** field.
6. Click **Save**.

### 3. Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos**.
2. Click **Add Repo**.
3. Enter the repository URL: `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git`.
4. Leave **Branch** as `main`.
5. Click **Create Repo**. The repository tree will appear under your user folder.

### 4. Create a Unity Catalog–Enabled Cluster

1. In the left sidebar, click **Compute**.
2. Click **Create compute**.
3. Configure the cluster using the reference table below.

| Parameter | Value |
|---|---|
| Cluster name | `eeg-lab-cluster` |
| Cluster mode | Single node |
| Databricks Runtime | **14.3 LTS** (Scala 2.12, Spark 3.5) |
| Node type | `Standard_DS3_v2` (Azure) or equivalent |
| Terminate after | 60 minutes of inactivity |
| Unity Catalog | Enabled (set **Access mode** to **Single user**) |

4. Expand **Advanced options** > **Spark** and add the following configuration:

```
spark.databricks.delta.retentionDurationCheck.enabled false
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
```

5. Click **Create compute** and wait until the cluster status shows **Running**.

### 5. Install Required Libraries

1. Select your cluster from the Compute list and click the **Libraries** tab.
2. Click **Install new**.
3. Install the following libraries in order.

| Library source | Coordinates / Package name |
|---|---|
| PyPI | `mne==1.7.0` |
| PyPI | `scipy==1.13.0` |
| PyPI | `numpy==1.26.4` |

4. Wait for each library to show the status **Installed** before proceeding.

### 6. Open and Attach the Notebook

1. In the left sidebar, click **Repos** and navigate to `wang-yuhao/databricks-eeg-lakehouse-lab/notebooks/`.
2. Click `day05_delta_internals.py` to open it.
3. In the notebook toolbar, click the cluster dropdown and select `eeg-lab-cluster`.
4. Confirm the cluster indicator turns green before running any cell.

---

## Objectives

- Understand the structure of the Delta Lake transaction log (`_delta_log`).
- Perform time travel queries by version number and by timestamp.
- Use `OPTIMIZE` to compact small Parquet files.
- Apply `ZORDER` for column-level data co-location.
- Execute `VACUUM` to remove obsolete file versions.
- Inspect and interpret the full Delta table commit history.

---

## Background

Delta Lake stores data as a set of Parquet files alongside a JSON-based transaction log. Every write, update, or schema change appends a new entry to this log. The runtime reconstructs the table state for any historical version by replaying the log from version 0.

| Concept | Description | Exam relevance |
|---|---|---|
| `_delta_log/` | JSON commit log; one file per transaction | Time travel; audit trail |
| Time travel by version | `option("versionAsOf", N)` | Rollback; A/B testing |
| Time travel by timestamp | `option("timestampAsOf", "YYYY-MM-DD HH:MM:SS")` | Point-in-time recovery |
| `OPTIMIZE` | Merges small files into ~1 GB target files | Reduces read overhead |
| `ZORDER BY` | Co-locates rows sharing the same column values | Accelerates predicate filters |
| `VACUUM` | Deletes Parquet files older than the retention threshold | Reduces storage cost |

> **Exam tip**: `OPTIMIZE` does **not** delete old files. You must run `VACUUM` after `OPTIMIZE` to reclaim storage. `ZORDER` is effective only on high-cardinality columns such as `subject_id`; applying it to low-cardinality columns such as `is_artifact` provides no benefit.

---

## Step-by-Step Instructions

### Step 1 — Define the Silver table path

Run the following cell to establish the storage path used throughout this notebook.

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F
import json, datetime

SILVER_PATH = "dbfs:/eeg_lakehouse/silver/cleaned_epochs"
SILVER_TABLE = "eeg_lakehouse.silver.cleaned_epochs"

print(f"Silver path : {SILVER_PATH}")
print(f"Silver table: {SILVER_TABLE}")
```

**Expected output**:

```
Silver path : dbfs:/eeg_lakehouse/silver/cleaned_epochs
Silver table: eeg_lakehouse.silver.cleaned_epochs
```

---

### Step 2 — List the storage root

```python
display(dbutils.fs.ls(SILVER_PATH))
```

**Expected output** (columns: `path`, `name`, `size`):

```
dbfs:/eeg_lakehouse/silver/cleaned_epochs/_delta_log/   _delta_log/   0
dbfs:/eeg_lakehouse/silver/cleaned_epochs/subject_id=001/   subject_id=001/   0
dbfs:/eeg_lakehouse/silver/cleaned_epochs/subject_id=002/   subject_id=002/   0
```

The `_delta_log/` directory is the transaction log. Partition directories (`subject_id=001/`, etc.) contain the actual Parquet data files.

---

### Step 3 — Inspect the transaction log

```python
display(dbutils.fs.ls(f"{SILVER_PATH}/_delta_log/"))
```

**Expected output**:

```
dbfs:/eeg_lakehouse/silver/cleaned_epochs/_delta_log/00000000000000000000.json
dbfs:/eeg_lakehouse/silver/cleaned_epochs/_delta_log/00000000000000000001.json
```

Each `.json` file corresponds to one committed transaction. File `00000000000000000000.json` is the initial table creation (version 0).

---

### Step 4 — Read a raw commit entry

```python
log_file = f"{SILVER_PATH}/_delta_log/00000000000000000000.json"
content  = dbutils.fs.head(log_file)

for line in content.split("\n")[:5]:
    if line.strip():
        print(json.dumps(json.loads(line), indent=2))
```

**Expected output (abridged)**:

```json
{
  "add": {
    "path": "subject_id=001/part-00000-abc123.snappy.parquet",
    "size": 1345678,
    "partitionValues": { "subject_id": "001" },
    "modificationTime": 1718000000000,
    "dataChange": true,
    "stats": "{\"numRecords\":500,\"minValues\":{\"epoch_idx\":0},\"maxValues\":{\"epoch_idx\":499}}"
  }
}
```

| JSON field | Meaning |
|---|---|
| `add.path` | Parquet file added in this commit |
| `add.partitionValues` | Partition key values for the file |
| `add.stats` | Min/max column statistics used for data skipping |
| `add.dataChange` | `true` if the commit adds or removes rows |

---

### Step 5 — Time travel by version number

```python
df_v0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(SILVER_PATH)
)

print(f"Version 0 — record count: {df_v0.count()}")
df_v0.select("subject_id", "epoch_idx", "sleep_stage").show(5, truncate=False)
```

**Expected output**:

```
Version 0 — record count: 15000
+----------+---------+-----------+
|subject_id|epoch_idx|sleep_stage|
+----------+---------+-----------+
|001       |0        |N2         |
|001       |1        |N2         |
|001       |2        |N3         |
|001       |3        |REM        |
|002       |0        |Wake       |
+----------+---------+-----------+
```

---

### Step 6 — Time travel by timestamp

```python
one_hour_ago = (
    datetime.datetime.now() - datetime.timedelta(hours=1)
).strftime("%Y-%m-%d %H:%M:%S")

try:
    df_ts = (
        spark.read
        .format("delta")
        .option("timestampAsOf", one_hour_ago)
        .load(SILVER_PATH)
    )
    print(f"Record count as of {one_hour_ago}: {df_ts.count()}")
except Exception as exc:
    print(f"Time travel unavailable for timestamp {one_hour_ago}: {exc}")
    print("This is expected when the table was created less than one hour ago.")
```

---

### Step 7 — Inspect commit history

```python
delta_table = DeltaTable.forPath(spark, SILVER_PATH)

(
    delta_table.history()
    .select("version", "timestamp", "operation", "operationParameters")
    .show(10, truncate=False)
)
```

**Expected output**:

```
+-------+-------------------+---------+----------------------------------------+
|version|timestamp          |operation|operationParameters                     |
+-------+-------------------+---------+----------------------------------------+
|1      |2026-06-22 00:30:00|WRITE    |{mode -> Overwrite, partitionBy -> [...]}|
|0      |2026-06-21 23:00:00|WRITE    |{mode -> Overwrite, partitionBy -> [...]}|
+-------+-------------------+---------+----------------------------------------+
```

---

### Step 8 — Profile file sizes before OPTIMIZE

```python
def count_parquet_files(path: str) -> dict:
    """Return file count and average size in MB for Parquet files under path."""
    all_files = dbutils.fs.ls(path)
    parquet_files = [f for f in all_files if f.path.endswith(".parquet")]

    if not parquet_files:
        # Data is in partition sub-directories — inspect first partition
        sub_dirs = [f for f in all_files if f.name.startswith("subject_id=")]
        if sub_dirs:
            parquet_files = [
                f for f in dbutils.fs.ls(sub_dirs[0].path)
                if f.path.endswith(".parquet")
            ]

    if not parquet_files:
        return {"count": 0, "avg_mb": 0.0}

    avg_mb = sum(f.size for f in parquet_files) / len(parquet_files) / 1024 / 1024
    return {"count": len(parquet_files), "avg_mb": round(avg_mb, 2)}

stats_before = count_parquet_files(SILVER_PATH)
print(f"Before OPTIMIZE — files: {stats_before['count']}, avg size: {stats_before['avg_mb']} MB")
```

---

### Step 9 — Run OPTIMIZE (compaction)

```python
delta_table.optimize().executeCompaction()
print("OPTIMIZE (compaction) complete.")
```

`OPTIMIZE` merges Parquet files below the 1 GB target size. Old file versions are retained in storage until `VACUUM` removes them.

---

### Step 10 — Apply ZORDER on subject_id

```python
delta_table.optimize().executeZOrderBy("subject_id")
print("OPTIMIZE with ZORDER on subject_id complete.")
```

| Column | Cardinality | Suitable for ZORDER? |
|---|---|---|
| `subject_id` | High (e.g., 200 unique values) | Yes — accelerates `WHERE subject_id = '001'` |
| `sleep_stage` | Low (5 unique values) | Marginal benefit |
| `is_artifact` | Very low (2 values) | No — no data skipping benefit |

---

### Step 11 — Profile file sizes after OPTIMIZE

```python
stats_after = count_parquet_files(SILVER_PATH)
print(f"After OPTIMIZE  — files: {stats_after['count']}, avg size: {stats_after['avg_mb']} MB")
print("Old file versions still exist. Run VACUUM to remove them.")
```

---

### Step 12 — VACUUM dry run

```python
# Disable the 7-day retention safety check for demonstration purposes only.
# Never disable this check in a production environment.
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

delta_table.vacuum(retentionHours=0, dryRun=True)
print("Dry run complete — no files were deleted.")
```

The dry run lists files that would be removed without deleting them. This is the correct way to preview the impact of `VACUUM`.

---

### Step 13 — Execute VACUUM

```python
delta_table.vacuum(retentionHours=0)
print("VACUUM complete. Parquet files for superseded versions have been removed.")
```

> **Warning**: Running `VACUUM` with `retentionHours=0` in production breaks time travel for all versions except the latest. Always use `retentionHours=168` (7 days) or higher in production workloads.

---

### Step 14 — Verify time travel failure after VACUUM

```python
try:
    df_check = (
        spark.read
        .format("delta")
        .option("versionAsOf", 0)
        .load(SILVER_PATH)
    )
    print(f"Version 0 accessible — record count: {df_check.count()}")
except Exception as exc:
    print("Expected FileNotFoundException: VACUUM removed the Parquet files for version 0.")
    print(str(exc)[:200])
```

---

### Step 15 — Inspect final commit history

```python
(
    delta_table.history()
    .select("version", "timestamp", "operation")
    .show(10, truncate=False)
)
```

**Expected output**:

```
+-------+-------------------+---------+
|version|timestamp          |operation|
+-------+-------------------+---------+
|4      |2026-06-22 01:05:00|VACUUM   |
|3      |2026-06-22 01:02:00|OPTIMIZE |
|2      |2026-06-22 00:58:00|OPTIMIZE |
|1      |2026-06-22 00:30:00|WRITE    |
|0      |2026-06-21 23:00:00|WRITE    |
+-------+-------------------+---------+
```

Each `OPTIMIZE` and `VACUUM` execution creates a new version entry in the transaction log. The log entry itself persists; only the underlying Parquet data files are deleted by `VACUUM`.

---

## Exam Reflection Questions

Answer the following questions without referring to the notebook. These topics appear directly on the Databricks Data Engineer Professional exam.

1. What is stored in the `_delta_log/` directory, and why does it enable time travel?
2. Write the PySpark expression to read a Delta table at version 3.
3. Does `OPTIMIZE` delete old Parquet files? What command must follow it to reclaim storage?
4. Give one column where `ZORDER` is highly effective and one where it is not. Explain why.
5. What is the default `VACUUM` retention period, and what is the risk of setting it to zero?
6. After running `VACUUM retentionHours=0`, will `DeltaTable.history()` still show version 0?

**Reference answers**:

1. The `_delta_log/` directory contains one JSON file per committed transaction. Each file records which Parquet files were added or removed. The runtime replays these entries to reconstruct any historical snapshot, enabling time travel.
2. `spark.read.format("delta").option("versionAsOf", 3).load(path)`
3. `OPTIMIZE` does **not** delete old files. Run `VACUUM` after `OPTIMIZE` to remove superseded Parquet files.
4. High-cardinality: `subject_id` (200 unique subjects) — effective, because Spark can skip entire file groups. Low-cardinality: `is_artifact` (True/False only) — ineffective, because nearly every file contains both values.
5. Default retention is 168 hours (7 days). Setting it to zero permanently removes Parquet files for all versions except the current one, making time travel impossible for those versions.
6. Yes. `DeltaTable.history()` reads the transaction log JSON files, which `VACUUM` does not delete. Reading the data at version 0 will fail with `FileNotFoundException`, but the history entry remains.

---

## Day 5 Operation Reference

| Operation | Command | Side effect |
|---|---|---|
| List storage root | `dbutils.fs.ls(SILVER_PATH)` | Read-only |
| Read commit log | `dbutils.fs.head(".../_delta_log/00...json")` | Read-only |
| Time travel by version | `.option("versionAsOf", N)` | Read-only |
| Time travel by timestamp | `.option("timestampAsOf", "YYYY-MM-DD HH:MM:SS")` | Read-only |
| View history | `DeltaTable.history()` | Read-only |
| Compact small files | `DeltaTable.optimize().executeCompaction()` | Writes new files; adds version |
| Co-locate by column | `DeltaTable.optimize().executeZOrderBy("col")` | Writes new files; adds version |
| Remove old files | `DeltaTable.vacuum(retentionHours=168)` | Deletes Parquet; adds version |

**Next**: Day 6 builds the Gold layer by computing per-subject sleep features and writing aggregated Delta tables for downstream machine learning.
