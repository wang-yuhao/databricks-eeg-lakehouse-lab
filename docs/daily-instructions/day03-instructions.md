# Day 3: Bronze Ingestion with Auto Loader Patterns

**Notebook**: `notebooks/day03_bronze_ingestion.py`  
**Source modules**: `src/bronze/ingest_eeg_files.py`, `src/bronze/ingest_metadata.py`  
**Exam domains**: Auto Loader (Domain 3 — Incremental Processing), Delta audit logging (Domain 1 — Lakehouse Platform)  
**Time estimate**: 2–3 hours  
**Prerequisite**: Day 2 completed; Unity Catalog structures created; EDF files uploaded to UC Volume

---

## Objectives

- Run full Bronze ingestion via Auto Loader with `trigger(availableNow=True)`
- Inspect Delta transaction history using `DESCRIBE HISTORY`
- Understand trigger types: `availableNow` vs `once` vs `processingTime`
- Run data quality checks with assertions (null counts, file type validation)
- Verify idempotency by re-running ingestion
- Ingest Bronze metadata table from subject demographics

---

## Background

Day 3 transitions from schema design (Day 2) to actual execution. The Bronze ingestion uses Auto Loader's `cloudFiles` format with the `trigger(availableNow=True)` pattern, which processes all pending files in micro-batches and then stops automatically — this is the recommended pattern for batch-style incremental loads in production.

Auto Loader tracks processed files via a checkpoint directory. If ingestion fails mid-run and you restart the notebook, Auto Loader resumes from the checkpoint — **exactly-once semantics** are guaranteed.

---

## Step-by-Step Instructions

### Step 1 — Open the notebook

1. In Databricks Workspace, navigate to **Repos** > your repo > `notebooks/`
2. Open `day03_bronze_ingestion.py`
3. Attach to a Unity Catalog-enabled cluster (same as Day 2)
4. Confirm `mne`, `pyedflib`, and `scipy` are installed

---

### Step 2 — Run Cell 1: Setup imports and AppConfig

```python
import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import create_bronze_table, load_raw_files
from src.bronze.ingest_metadata import create_bronze_metadata_table

cfg = AppConfig()
print("Target Bronze table:", cfg.catalog.bronze_edf_fqn)
print("Volume path:", cfg.paths.volume_edf_dir)
print("Checkpoint path:", cfg.paths.autoloader_checkpoint)
```

**What to verify:**
- Bronze FQN = `eeg_lakehouse.bronze.raw_eeg_files`
- Volume path = `/Volumes/eeg_lakehouse/bronze/raw_edf`
- Checkpoint exists or will be auto-created on first run

---

### Step 3 — Run Cell 2: Create Unity Catalog structures (run once)

Uncomment and run these DDL statements **once** on a UC-enabled cluster:

```python
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog.catalog}")
spark.sql(f"USE CATALOG {cfg.catalog.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.gold_schema}")

# Create UC Volume for raw EDF files
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS eeg_lakehouse.bronze.raw_edf
    COMMENT 'Raw EDF files from PhysioNet Sleep-EDF Expanded'
""")
print("UC structures created.")
```

**Why these are idempotent:** `IF NOT EXISTS` ensures running this cell multiple times is safe. The Unity Catalog hierarchy is:
- **Metastore** (top-level, one per region/account)
  - **Catalog**: `eeg_lakehouse`
    - **Schema**: `bronze`, `silver`, `gold`
      - **Tables**: `raw_eeg_files`, `eeg_metadata`, etc.
      - **Volumes**: `raw_edf` (unstructured file storage)

**Exam note:** Only account admins or catalog owners can create catalogs. Schemas and volumes can be created by users with appropriate `USE CATALOG` and `CREATE` permissions.

---

### Step 4 — Upload EDF files to the UC Volume

Before running ingestion, raw EDF files must exist in `/Volumes/eeg_lakehouse/bronze/raw_edf/`.

**Option A — Download from PhysioNet via CLI (recommended):**

Run this in a Databricks notebook cell or init script:

```python
import os
import subprocess

# Download PhysioNet Sleep-EDF Expanded dataset (197 subjects)
download_dir = "/tmp/sleep-edf/"
os.makedirs(download_dir, exist_ok=True)

# Use wfdb library to download
subprocess.run(["pip", "install", "wfdb"], check=True)

import wfdb
wfdb.dl_database('sleep-edfx', dl_dir=download_dir)

# Copy to UC Volume
volume_path = "/Volumes/eeg_lakehouse/bronze/raw_edf/"
dbutils.fs.cp(f"file:{download_dir}", volume_path, recurse=True)

print(f"EDF files uploaded to {volume_path}")
```

**Option B — Upload sample files for testing:**

For a quick dry run, upload just 2–4 subjects (8 files: 4 PSG + 4 Hypnogram):

```python
# Example: Upload one subject's files
sample_files = [
    "SC4001E0-PSG.edf",
    "SC4001EC-Hypnogram.edf",
    "SC4002E0-PSG.edf",
    "SC4002EC-Hypnogram.edf",
]

for file in sample_files:
    dbutils.fs.cp(
        f"file:/tmp/sleep-edf/{file}",
        f"/Volumes/eeg_lakehouse/bronze/raw_edf/{file}"
    )
```

**Verify files are in the Volume:**

```python
dbutils.fs.ls("/Volumes/eeg_lakehouse/bronze/raw_edf/")[:5]
```

You should see `.edf` files listed.

---

### Step 5 — Run Cell 3: Execute Bronze ingestion with Auto Loader

```python
from src.bronze.ingest_eeg_files import create_bronze_table

# trigger_once=True uses .trigger(availableNow=True)
create_bronze_table(spark, cfg, trigger_once=True)
```

**What happens internally** (from `src/bronze/ingest_eeg_files.py`):

1. **`load_raw_files()` sets up the streaming source:**
   ```python
   spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "binaryFile")
       .option("cloudFiles.schemaLocation", cfg.paths.autoloader_schema_location)
       .option("cloudFiles.useNotifications", "false")  # directory listing
       .option("cloudFiles.includeExistingFiles", "true")
       .option("pathGlobFilter", "*.edf")
       .load(cfg.paths.volume_edf_dir)
   ```

2. **UDF enrichment:**
   - `_extract_subject_id_udf` parses `subject_id` from filename
   - `_extract_study_night_udf` extracts study night (0 or 1)
   - `is_hypnogram_file` classifies PSG vs Hypnogram

3. **Add audit columns:**
   - `ingestion_timestamp = F.current_timestamp()`
   - `dataset_source = "physionet-sleep-edf"`

4. **Write stream to Delta table:**
   ```python
   writer = (
       df.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", cfg.paths.autoloader_checkpoint)
         .trigger(availableNow=True)  # Batch-style processing
         .toTable(cfg.catalog.bronze_edf_fqn)
   )
   writer.awaitTermination()  # Blocks until all files processed
   ```

**Expected output:**
```
Processing files: 100%|██████████| 8/8 [00:15<00:00,  1.92s/file]
Ingestion complete. 8 files written to eeg_lakehouse.bronze.raw_eeg_files.
```

**How long does this take?**
- 8 files (2 subjects): ~15–30 seconds
- 394 files (197 subjects): ~5–10 minutes (depends on cluster size)

---

### Step 6 — Run Cell 4: Inspect Delta transaction history

Every write to a Delta table creates a new transaction version. Use `DESCRIBE HISTORY` to see the audit log:

```sql
%sql
DESCRIBE HISTORY eeg_lakehouse.bronze.raw_eeg_files
```

**Or in Python:**
```python
spark.sql(f"DESCRIBE HISTORY {cfg.catalog.bronze_edf_fqn}").show(truncate=False)
```

**Expected columns:**
| Column | Example Value |
|---|---|
| `version` | `0` (first version), `1`, `2`, ... |
| `timestamp` | `2026-06-22 00:15:32.123` |
| `operation` | `STREAMING UPDATE` (from Auto Loader) |
| `operationMetrics` | `{"numAddedFiles":"8", "numOutputRows":"8"}` |
| `userName` | `yuhao.wang@databricks.com` |
| `notebook` | `{"notebookId":"123456"}` |

**Exam questions to understand:**
1. **What is version 0?** The initial empty table or first write.
2. **What is `STREAMING UPDATE`?** Auto Loader writes via structured streaming.
3. **How do you query version 0?** `SELECT * FROM table VERSION AS OF 0`
4. **How do you roll back to version 1?** `RESTORE TABLE table TO VERSION AS OF 1`

**Common exam pattern:** "How would you audit who deleted rows from this table?" Answer: `DESCRIBE HISTORY table` shows `operation = DELETE` with `userName` and `timestamp`.

---

### Step 7 — Run Cell 5: Understand trigger type differences

Review this reference table (do NOT run any code here — just understand the concepts):

| Trigger Type | Syntax | Behavior | Use Case |
|---|---|---|---|
| `availableNow` | `.trigger(availableNow=True)` | Processes all pending files in micro-batches, then stops | **Best for batch pipelines** — recommended for production |
| `once` | `.trigger(once=True)` | Legacy equivalent of `availableNow` | Deprecated but still works |
| `processingTime` | `.trigger(processingTime='5 min')` | Runs indefinitely, processing new files every 5 minutes | Continuous near-real-time pipelines |
| `continuous` | `.trigger(continuous='1 sec')` | Ultra-low latency, limited sink support | Kafka ingestion with <1s latency |

**Exam tip:** For EDF file ingestion (batch delivery), `availableNow=True` is the correct choice. For real-time streaming (e.g., IoT sensors), use `processingTime`.

**Why `availableNow` > `once`?**
- `availableNow` processes files in optimized micro-batches (better parallelism)
- `once` processes everything as a single batch (may timeout on large datasets)

---

### Step 8 — Run Cell 6: Data quality checks with assertions

```python
import pyspark.sql.functions as F

bronze_df = spark.table(cfg.catalog.bronze_edf_fqn)

# Check 1: No null subject_ids (regex parsing must work)
null_subjects = bronze_df.filter(F.col("subject_id").isNull()).count()
assert null_subjects == 0, f"FAIL: {null_subjects} null subject_ids found"

# Check 2: Both file types are present
psg_count = bronze_df.filter(~F.col("is_hypnogram")).count()
hyp_count = bronze_df.filter(F.col("is_hypnogram")).count()
assert psg_count > 0, "FAIL: No PSG files found"
assert hyp_count > 0, "FAIL: No Hypnogram files found"

# Check 3: PSG and Hypnogram counts match (paired data)
assert psg_count == hyp_count, \
    f"FAIL: PSG count ({psg_count}) != Hypnogram count ({hyp_count})"

# Check 4: All files have expected dataset_source tag
wrong_source = bronze_df.filter(
    F.col("dataset_source") != "physionet-sleep-edf"
).count()
assert wrong_source == 0, f"FAIL: {wrong_source} files with wrong dataset_source"

print("✓ PASS: All quality checks passed.")
print(f"  PSG files: {psg_count}, Hypnogram files: {hyp_count}")

# Display summary by file type
bronze_df.groupBy("recording_type", "is_hypnogram").count().show()
```

**Expected output:**
```
✓ PASS: All quality checks passed.
  PSG files: 4, Hypnogram files: 4

+--------------+-------------+-----+
|recording_type|is_hypnogram|count|
+--------------+-------------+-----+
|SC            |false        |4    |
|SC            |true         |4    |
+--------------+-------------+-----+
```

**Why assertions are critical for production:** These checks ensure data integrity before promoting to Silver. In a real pipeline, failed assertions would trigger alerts via Delta Live Tables expectations or Great Expectations.

---

### Step 9 — Re-run ingestion to verify idempotency

Re-run **Step 5** (the `create_bronze_table()` call) a second time:

```python
create_bronze_table(spark, cfg, trigger_once=True)
```

Then check the row count:

```python
count_before = 8  # From first run
count_after = spark.table(cfg.catalog.bronze_edf_fqn).count()

print(f"Row count before: {count_before}")
print(f"Row count after: {count_after}")
assert count_before == count_after, "FAIL: Duplicate records created!"
print("✓ PASS: Idempotency verified — no duplicates.")
```

**Why this works:**
Auto Loader tracks processed files via the checkpoint directory (`cfg.paths.autoloader_checkpoint`). The checkpoint stores a list of `(file_path, modification_time)` tuples. On the second run:
1. Auto Loader scans the Volume and finds 8 files
2. Checks the checkpoint — all 8 files already processed
3. No new files to ingest — stream terminates immediately
4. No new rows written to the Bronze table

**Exam question:** "What happens if you delete the checkpoint directory and re-run ingestion?" Answer: Auto Loader re-ingests all files, creating duplicates. This is why checkpoint management is critical in production.

---

### Step 10 — Run Cell 7: Ingest Bronze metadata table

The `eeg_lakehouse.bronze.eeg_metadata` table stores subject-level demographics (age, sex, medication status) parsed from PhysioNet annotations.

```python
from src.bronze.ingest_metadata import create_bronze_metadata_table

create_bronze_metadata_table(spark, cfg)
```

**What this does** (from `src/bronze/ingest_metadata.py`):
1. Parses hypnogram `.edf` files to extract subject metadata
2. Creates a Delta table with schema: `subject_id`, `age`, `sex`, `medication`, `dataset_source`
3. Writes to `eeg_lakehouse.bronze.eeg_metadata`

**Verify the metadata table:**

```python
spark.table("eeg_lakehouse.bronze.eeg_metadata").show(5)
```

**Expected output:**
```
+----------+---+------+-----------+---------------------+
|subject_id|age|sex   |medication |dataset_source       |
+----------+---+------+-----------+---------------------+
|SC4001    |40 |M     |None       |physionet-sleep-edf  |
|SC4002    |40 |F     |None       |physionet-sleep-edf  |
+----------+---+------+-----------+---------------------+
```

This metadata will be joined with Gold features in later days.

---

### Step 11 — Self-check: Answer exam reflection questions

1. What is the difference between `availableNow` and `processingTime` triggers?
2. Where is the Auto Loader checkpoint stored, and what happens if you delete it?
3. How do you verify that Bronze ingestion is idempotent?
4. What does `DESCRIBE HISTORY` return, and how is it different from `DESCRIBE DETAIL`?
5. Why is the Bronze table a metadata registry (not raw signal bytes)?
6. What Delta feature enables exactly-once semantics in Auto Loader?

**Answers:**
1. `availableNow`: Batch processing — processes all pending files and stops. `processingTime`: Continuous — runs indefinitely, triggering every N minutes.
2. Stored at `cfg.paths.autoloader_checkpoint`. Deleting it causes Auto Loader to re-ingest all files (duplicates).
3. Re-run ingestion and assert row count unchanged. Auto Loader checkpoint ensures files are not re-processed.
4. `DESCRIBE HISTORY`: Audit log (versions, operations, user, timestamp). `DESCRIBE DETAIL`: Physical metadata (file count, size, partitions).
5. EDF parsing requires MNE-Python (Pandas UDFs) — Bronze stores metadata only; actual signal parsing is deferred to Silver.
6. Auto Loader checkpoint + Delta transaction log = exactly-once guarantees.

---

## Day 3 Summary

| What was built | Source |
|---|---|
| Unity Catalog structure (catalog, schemas, volume) | SQL DDL |
| PhysioNet EDF file upload to UC Volume | `dbutils.fs.cp` or `wfdb` library |
| Bronze table ingestion via Auto Loader | `src/bronze/ingest_eeg_files.py` |
| Delta transaction history inspection | `DESCRIBE HISTORY` SQL command |
| Data quality checks (null counts, file type validation) | Spark DataFrame assertions |
| Idempotency verification (re-run ingestion) | Auto Loader checkpoint |
| Bronze metadata table (subject demographics) | `src/bronze/ingest_metadata.py` |

**Next**: Day 4 builds the Silver layer with signal preprocessing (bandpass filtering, artifact removal) using Pandas UDFs and MNE-Python.
