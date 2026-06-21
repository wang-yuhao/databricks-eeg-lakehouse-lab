# Day 5: Delta Lake Internals — Time Travel, OPTIMIZE, ZORDER, VACUUM

**Notebook**: `notebooks/day05_delta_internals.py`
**Source modules**: N/A (Delta Lake operations only)
**Exam domains**: Delta Lake (Domain 2 — Delta with Spark SQL & Python), Delta Live Tables (Domain 1)
**Time estimate**: 2–3 hours
**Prerequisite**: Day 4 completed, Silver table exists

---

## Objectives

- Understand Delta Lake's ACID transaction log (`_delta_log`)
- Perform time travel queries to inspect historical data
- Use OPTIMIZE to compact small files
- Apply ZORDER for co-location of frequently queried columns
- Run VACUUM to delete old file versions
- Inspect Delta table history and metadata

---

## Background

**Why Delta Lake internals matter:**

Delta Lake stores data as Parquet files + a transaction log. Understanding the internals helps you:

1. **Debug pipeline issues** (check commit history, rollback)
2. **Optimize read performance** (file compaction, data skipping)
3. **Manage storage costs** (VACUUM old versions)
4. **Pass the Databricks exam** (OPTIMIZE, ZORDER, time travel are heavily tested)

**Key concepts:**

| Concept | Description | Use case |
|---|---|---|
| `_delta_log/` | JSON commit log (one file per transaction) | Audit trail, time travel |
| Time travel | Query table as of version N or timestamp T | Rollback, A/B testing |
| OPTIMIZE | Merge small files into 1GB files | Improve read speed |
| ZORDER | Co-locate data by column values | Speed up filters on `subject_id`, `epoch_idx` |
| VACUUM | Delete Parquet files older than retention period | Reduce storage costs |

**Exam tip:**

- OPTIMIZE **does not** delete old files — you must run VACUUM after OPTIMIZE
- ZORDER works best on high-cardinality columns (`subject_id`), not low-cardinality (`is_artifact`)

---

## Step-by-Step Instructions

### Step 1 — Open the notebook

1. Go to Databricks Workspace
2. Open `notebooks/day05_delta_internals.py`
3. Attach to your cluster

---

### Step 2 — Run Cell 1: Inspect Delta table location

```python
# Display Silver table location
silver_path = "dbfs:/eeg_lakehouse/silver/cleaned_epochs"
display(dbutils.fs.ls(silver_path))
```

**Expected output:**

```
path | name | size
-----|------|-----
dbfs:/eeg_lakehouse/silver/cleaned_epochs/_delta_log/ | _delta_log/ | 0
dbfs:/eeg_lakehouse/silver/cleaned_epochs/subject_id=001/ | subject_id=001/ | 0
dbfs:/eeg_lakehouse/silver/cleaned_epochs/subject_id=002/ | subject_id=002/ | 0
...
```

**Key observation:**

- `_delta_log/` stores transaction metadata (JSON files)
- Data partitioned by `subject_id` (from Day 4)

---

### Step 3 — Run Cell 2: Inspect transaction log

```python
# List Delta log files
display(dbutils.fs.ls(f"{silver_path}/_delta_log/"))
```

**Expected output:**

```
path | name
-----|-----
.../_delta_log/00000000000000000000.json | 00000000000000000000.json
.../_delta_log/00000000000000000001.json | 00000000000000000001.json
```

**Explanation:**

- Each `.json` file = one transaction (commit)
- File `00000000000000000000.json` = version 0 (initial write)
- File `00000000000000000001.json` = version 1 (if you re-ran Day 4 with `mode='overwrite'`)

---

### Step 4 — Run Cell 3: Read transaction log

```python
# Read the first commit
import json

log_file = f"{silver_path}/_delta_log/00000000000000000000.json"
content = dbutils.fs.head(log_file)
for line in content.split("\n")[:5]:  # Show first 5 lines
    if line.strip():
        print(json.dumps(json.loads(line), indent=2))
```

**Expected output (example):**

```json
{
  "add": {
    "path": "subject_id=001/part-00000-xxx.snappy.parquet",
    "size": 1234567,
    "partitionValues": {"subject_id": "001"},
    "modificationTime": 1718000000000,
    "dataChange": true,
    "stats": "{...}"
  }
}
```

**Key fields:**

- `add`: File added in this commit
- `path`: Parquet file path
- `partitionValues`: Partition key (`subject_id=001`)
- `stats`: Min/max stats for data skipping

---

### Step 5 — Run Cell 4: Time travel — Query version 0

```python
from pyspark.sql import functions as F

# Read Silver table at version 0
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)

print(f"Version 0 record count: {df_v0.count()}")
df_v0.select("subject_id", "epoch_idx", "sleep_stage").show(5)
```

**Expected output:**

```
Version 0 record count: 15000
+----------+---------+-----------+
|subject_id|epoch_idx|sleep_stage|
+----------+---------+-----------+
|001       |0        |N2         |
|001       |1        |N2         |
...
```

**Why this works:**

- Delta Lake keeps old Parquet files until VACUUM
- `option("versionAsOf", 0)` reads the transaction log at version 0

---

### Step 6 — Run Cell 5: Time travel — Query by timestamp

```python
import datetime

# Query table as of 1 hour ago
one_hour_ago = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

try:
    df_ts = spark.read.format("delta").option("timestampAsOf", one_hour_ago).load(silver_path)
    print(f"Record count 1 hour ago: {df_ts.count()}")
except Exception as e:
    print(f"Time travel failed: {e}")
    print("(This is expected if the table was created less than 1 hour ago)")
```

**Expected output:**

- If table is older than 1 hour → shows record count
- If table is new → error (expected)

---

### Step 7 — Run Cell 6: Inspect table history

```python
# Show Delta table history
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, silver_path)
delta_table.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
```

**Expected output:**

```
+-------+-------------------+---------+-------------------------------+
|version|timestamp          |operation|operationParameters            |
+-------+-------------------+---------+-------------------------------+
|1      |2026-06-22 00:30:00|WRITE    |{mode: Overwrite, ...}         |
|0      |2026-06-21 23:00:00|WRITE    |{mode: Overwrite, ...}         |
+-------+-------------------+---------+-------------------------------+
```

**Use case:**

- Audit who wrote data and when
- Rollback to a previous version

---

### Step 8 — Run Cell 7: Check file sizes before OPTIMIZE

```python
# Count number of data files
import re

files = dbutils.fs.ls(silver_path)
parquet_files = [f for f in files if f.path.endswith(".parquet")]

if parquet_files:
    total_size = sum([f.size for f in parquet_files])
    print(f"Total files: {len(parquet_files)}")
    print(f"Average file size: {total_size / len(parquet_files) / 1024 / 1024:.2f} MB")
else:
    # Files are in partition directories
    print("Files stored in partition directories — check subject_id=001/")
    partition_files = dbutils.fs.ls(f"{silver_path}/subject_id=001/")
    parquet_files = [f for f in partition_files if ".parquet" in f.path]
    if parquet_files:
        avg_size = sum([f.size for f in parquet_files]) / len(parquet_files) / 1024 / 1024
        print(f"Files in subject_id=001: {len(parquet_files)}, Avg size: {avg_size:.2f} MB")
```

**Expected output:**

```
Files in subject_id=001: 8, Avg size: 12.5 MB
```

**Problem:**

- Small files (< 128 MB) cause read overhead
- Spark reads many small files instead of one large file

---

### Step 9 — Run Cell 8: Run OPTIMIZE

```python
# Compact small files into 1GB files
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, silver_path)
delta_table.optimize().executeCompaction()

print("✓ OPTIMIZE complete")
```

**Expected output:**

```
✓ OPTIMIZE complete
```

**What happened:**

- Delta Lake merged small Parquet files into larger files (~1GB target)
- Old files still exist (for time travel)

---

### Step 10 — Run Cell 9: Apply ZORDER

```python
# Apply ZORDER on subject_id (improves filters like WHERE subject_id = '001')
delta_table.optimize().executeZOrderBy("subject_id")

print("✓ ZORDER complete")
```

**Expected output:**

```
✓ ZORDER complete
```

**Why ZORDER `subject_id`?**

- `subject_id` is used in filters (e.g., `WHERE subject_id IN ('001', '002')`)
- ZORDER co-locates rows with same `subject_id` → faster data skipping

**Exam tip:**

- ZORDER works best on **high-cardinality** columns (many unique values)
- Don't ZORDER on `is_artifact` (only 2 values: True/False)

---

### Step 11 — Run Cell 10: Check file sizes after OPTIMIZE

```python
# Re-check file sizes
partition_files = dbutils.fs.ls(f"{silver_path}/subject_id=001/")
parquet_files = [f for f in partition_files if ".parquet" in f.path]
avg_size = sum([f.size for f in parquet_files]) / len(parquet_files) / 1024 / 1024

print(f"Files after OPTIMIZE: {len(parquet_files)}, Avg size: {avg_size:.2f} MB")
```

**Expected output:**

```
Files after OPTIMIZE: 16, Avg size: 8.2 MB
```

**Observation:**

- More files (old + new), but VACUUM will delete old files

---

### Step 12 — Run Cell 11: Run VACUUM (dry run)

```python
# VACUUM dry run (show files to delete, but don't delete them)
delta_table.vacuum(retentionHours=0, dryRun=True)
```

**Expected error:**

```
IllegalArgumentException: retention period must be >= 168 hours (7 days)
```

**Why?**

- Default retention = 7 days (safety against accidental deletion)
- To override, set `spark.databricks.delta.retentionDurationCheck.enabled = false`

---

### Step 13 — Run Cell 12: Disable retention check and VACUUM

```python
# Disable retention check (ONLY for testing — never do this in production)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# VACUUM (delete files older than 0 hours)
delta_table.vacuum(retentionHours=0)

print("✓ VACUUM complete")
```

**Expected output:**

```
✓ VACUUM complete
```

**⚠️ WARNING:**

- VACUUM deletes old Parquet files → time travel will fail for deleted versions
- In production, use `retentionHours=168` (7 days) or higher

---

### Step 14 — Run Cell 13: Verify time travel fails after VACUUM

```python
# Try to read version 0 (should fail if VACUUM deleted it)
try:
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
    print(f"Version 0 still accessible: {df_v0.count()} records")
except Exception as e:
    print(f"✓ Expected error: {str(e)[:100]}...")
    print("(VACUUM deleted old Parquet files)")
```

**Expected output:**

```
✓ Expected error: FileNotFoundException: File dbfs:/eeg_lakehouse/silver/cleaned_epochs/subject_id=001/part-0000...
(VACUUM deleted old Parquet files)
```

---

### Step 15 — Run Cell 14: Inspect final table history

```python
# Show updated history
delta_table.history().select("version", "timestamp", "operation").show(10, truncate=False)
```

**Expected output:**

```
+-------+-------------------+---------+
|version|timestamp          |operation|
+-------+-------------------+---------+
|4      |2026-06-22 01:00:00|VACUUM   |
|3      |2026-06-22 00:55:00|OPTIMIZE |
|2      |2026-06-22 00:50:00|OPTIMIZE |
|1      |2026-06-22 00:30:00|WRITE    |
|0      |2026-06-21 23:00:00|WRITE    |
+-------+-------------------+---------+
```

**Observation:**

- Each OPTIMIZE and VACUUM creates a new version
- Transaction log still exists, but Parquet files for version 0 are deleted

---

## Self-Check: Answer Exam Reflection Questions

1. What is stored in the `_delta_log/` directory?
2. How do you query a Delta table as of version 3?
3. What does OPTIMIZE do, and does it delete old files?
4. When should you use ZORDER? Give an example of a good column and a bad column.
5. What is the default VACUUM retention period, and why?
6. After running VACUUM with 0 hours retention, can you still time travel to version 0?

**Answers:**

1. Transaction metadata (JSON files, one per commit). Each file contains `add`/`remove` actions for Parquet files.
2. `spark.read.format("delta").option("versionAsOf", 3).load(path)`
3. OPTIMIZE merges small files into larger files (~1GB). It does **not** delete old files — you must run VACUUM after OPTIMIZE.
4. Use ZORDER on high-cardinality columns that appear in WHERE clauses. Good: `subject_id` (200 unique values). Bad: `is_artifact` (2 values).
5. 7 days (168 hours). This prevents accidental deletion of files needed for time travel or concurrent readers.
6. No. VACUUM deletes the Parquet files referenced by version 0. The transaction log entry still exists, but reading it will fail with FileNotFoundException.

---

## Day 5 Summary

| What was built | Command |
|---|---|
| Inspected Delta transaction log | `dbutils.fs.ls(".../_delta_log/")` |
| Time travel by version | `option("versionAsOf", 0)` |
| Time travel by timestamp | `option("timestampAsOf", "2026-06-22 00:00:00")` |
| Compacted small files | `DeltaTable.optimize().executeCompaction()` |
| Applied ZORDER on `subject_id` | `DeltaTable.optimize().executeZOrderBy("subject_id")` |
| Deleted old file versions | `DeltaTable.vacuum(retentionHours=168)` |
| Inspected commit history | `DeltaTable.history()` |

**Next**: Day 6 explores Gold layer aggregations and feature engineering for TDA (Topological Data Analysis).
