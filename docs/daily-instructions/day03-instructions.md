# Day 3: Bronze Ingestion with Auto Loader Patterns

**Notebook**: `notebooks/day03_bronze_ingestion.py`  
**Source modules**: `src/bronze/ingest_eeg_files.py`, `src/bronze/ingest_metadata.py`  
**Exam domains**: Auto Loader (Domain 3 — Incremental Processing), Delta audit logging (Domain 1 — Lakehouse Platform)  
**Time estimate**: 2–3 hours  
**Prerequisite**: Day 2 completed; Databricks workspace with Unity Catalog enabled and the repository cloned into Databricks Repos

---

## Objectives

- Execute full Bronze ingestion via Auto Loader with `trigger(availableNow=True)`
- Inspect Delta transaction history using `DESCRIBE HISTORY`
- Understand and compare trigger types: `availableNow`, `once`, and `processingTime`
- Run assertion-based data quality checks (null counts, file-type validation)
- Verify idempotency by re-running ingestion and asserting an unchanged row count
- Ingest the Bronze subject-metadata table from PhysioNet annotations

---

## Background

Day 3 transitions from schema design (Day 2) to actual execution. The Bronze ingestion uses Auto Loader’s `cloudFiles` format with the `trigger(availableNow=True)` pattern, which processes all pending files in micro-batches and then stops automatically. This is the recommended pattern for batch-style incremental loads in production.

Auto Loader tracks processed files via a checkpoint directory. If ingestion fails mid-run and you restart the notebook, Auto Loader resumes from the checkpoint — **exactly-once semantics are guaranteed**.

---

## Environment Setup

> **Complete all steps in this section before proceeding to any notebook instructions.** Every day’s instructions include this section so that the guide is fully self-contained, regardless of which day you are starting from.

### Step A — Prerequisites Check

Confirm all of the following before continuing:

1. A Databricks workspace is provisioned (Azure Databricks, AWS Databricks, or GCP Databricks).
2. Unity Catalog is enabled. Verify by navigating to **Catalog** in the left sidebar — a catalog hierarchy should be visible.
3. You have workspace admin or catalog owner permissions (required to create catalogs and volumes).
4. A GitHub account exists and is accessible in the browser.

---

### Step B — Generate a GitHub Personal Access Token (PAT)

If GitHub is already connected to Databricks via a saved PAT or OAuth from a previous session, skip to Step C.

1. Open [https://github.com/settings/tokens](https://github.com/settings/tokens) in a browser.
2. Click **Generate new token (classic)**.
3. Set **Note** to `databricks-repos-access`.
4. Set **Expiration** to at least 90 days.
5. Under **Select scopes**, enable `repo` (full repository access).
6. Click **Generate token**. Copy the token immediately — it will not be shown again.

---

### Step C — Configure Databricks Git Integration

1. In the Databricks workspace, click your **username** in the top-right corner.
2. Select **User Settings**.
3. Navigate to the **Linked Accounts** tab (also labelled **Git Integration** in some Databricks versions; for DBR 14.3+, navigate to **Settings > Developer Settings > Git credentials**).
4. Set **Git provider** to `GitHub`.
5. Paste the PAT from Step B into the **Token** field.
6. Click **Save**. A green confirmation banner should appear.

---

### Step D — Clone the Repository into Databricks Repos

1. In the left sidebar, click **Workspace**.
2. Expand **Repos**, then expand your user folder (e.g., `Repos / your-email@example.com /`).
3. Click **Add Repo** (the `+` icon at the top of the Repos panel).
4. In the dialog, enter:

   | Field | Value |
   |---|---|
   | **Git repository URL** | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git` |
   | **Git provider** | `GitHub` |
   | **Repo name** | `databricks-eeg-lakehouse-lab` |

5. Click **Create Repo**. Databricks will clone the repository. If the repo was already cloned in a prior day, navigate directly to it.

---

### Step E — Create or Verify a Unity-Catalog-Enabled Cluster

1. In the left sidebar, navigate to **Compute**.
2. Click **Create Compute** (or locate the existing `eeg-lab-cluster` from Day 2).
3. Confirm or set the following:

   | Setting | Required Value |
   |---|---|
   | **Cluster name** | `eeg-lab-cluster` |
   | **Databricks Runtime** | `14.3 LTS (Scala 2.12, Spark 3.5.0)` or higher |
   | **Node type** | `Standard_DS3_v2` (Azure) — 14 GB RAM, 4 vCores |
   | **Unity Catalog** | Enabled (default for DBR 13.3+) |
   | **Auto-termination** | 30 minutes |

4. Under **Libraries**, verify that the following PyPI packages are installed:

   | Library | Required Version |
   |---|---|
   | `mne` | `1.6.1` |
   | `pyedflib` | `0.1.34` |
   | `scipy` | `1.12.0` |
   | `yasa` | `0.6.4` |

5. If any library is missing, click **Install New**, select **PyPI**, and enter the package name and version.
6. Wait for the cluster to reach **Running** state before continuing.

---

### Step F — Open and Attach the Day 3 Notebook

1. In the left sidebar, navigate to **Workspace > Repos > your-email@example.com > databricks-eeg-lakehouse-lab > notebooks**.
2. Click **day03_bronze_ingestion.py** to open it.
3. In the top-right of the notebook, click the **Connect** dropdown and select `eeg-lab-cluster`.
4. Wait for the cluster indicator to turn green (Connected).

All notebook cells below are now ready to execute in sequence.

---

## Step-by-Step Notebook Instructions

### Step 1 — Run Cell 1: Setup Imports and AppConfig

```python
import sys
import os

sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import create_bronze_table, load_raw_files
from src.bronze.ingest_metadata import create_bronze_metadata_table

cfg = AppConfig()
print("Target Bronze table :", cfg.catalog.bronze_edf_fqn)
print("Volume path         :", cfg.paths.volume_edf_dir)
print("Checkpoint path     :", cfg.paths.autoloader_checkpoint)
```

**Verification checklist:**

| Output field | Expected value |
|---|---|
| Bronze FQN | `eeg_lakehouse.bronze.raw_eeg_files` |
| Volume path | `/Volumes/eeg_lakehouse/bronze/raw_edf` |
| Checkpoint path | `/Volumes/eeg_lakehouse/bronze/checkpoints/autoloader` |

---

### Step 2 — Run Cell 2: Create Unity Catalog Structures

Run this cell **once**. The `IF NOT EXISTS` clauses make each statement idempotent — re-running is safe.

```python
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog.catalog}")
spark.sql(f"USE CATALOG {cfg.catalog.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog.gold_schema}")

spark.sql("""
    CREATE VOLUME IF NOT EXISTS eeg_lakehouse.bronze.raw_edf
    COMMENT 'Raw EDF files from PhysioNet Sleep-EDF Expanded'
""")

print("Unity Catalog structures created successfully.")
```

**Unity Catalog hierarchy created by this cell:**

| Level | Object | Name |
|---|---|---|
| Metastore | (pre-existing, one per region) | — |
| Catalog | Catalog | `eeg_lakehouse` |
| Schema | Schema | `bronze`, `silver`, `gold` |
| Volume | Unstructured storage | `eeg_lakehouse.bronze.raw_edf` |

> **Permission note**: Only account admins or catalog owners can create catalogs. Schema and volume creation requires `USE CATALOG` and `CREATE SCHEMA` / `CREATE VOLUME` privileges on the target catalog.

---

### Step 3 — Upload EDF Files to the UC Volume

Before running ingestion, raw EDF files must be present at `/Volumes/eeg_lakehouse/bronze/raw_edf/`.

**Option A — Download from PhysioNet via the `wfdb` library (recommended for full dataset):**

```python
import os
import subprocess

download_dir = "/tmp/sleep-edf/"
os.makedirs(download_dir, exist_ok=True)

subprocess.run(["pip", "install", "wfdb"], capture_output=True, check=True)

import wfdb
wfdb.dl_database("sleep-edfx", dl_dir=download_dir)

volume_path = "/Volumes/eeg_lakehouse/bronze/raw_edf/"
dbutils.fs.cp(f"file:{download_dir}", volume_path, recurse=True)

print(f"EDF files uploaded to {volume_path}")
```

**Option B — Upload a minimal sample for a quick validation run (4 subjects, 8 files):**

```python
sample_files = [
    "SC4001E0-PSG.edf",
    "SC4001EC-Hypnogram.edf",
    "SC4002E0-PSG.edf",
    "SC4002EC-Hypnogram.edf",
    "SC4003E0-PSG.edf",
    "SC4003EC-Hypnogram.edf",
    "SC4004E0-PSG.edf",
    "SC4004EC-Hypnogram.edf",
]

for file in sample_files:
    dbutils.fs.cp(
        f"file:/tmp/sleep-edf/{file}",
        f"/Volumes/eeg_lakehouse/bronze/raw_edf/{file}"
    )

print(f"Uploaded {len(sample_files)} sample files.")
```

**Verify files are visible in the Volume:**

```python
files = dbutils.fs.ls("/Volumes/eeg_lakehouse/bronze/raw_edf/")
print(f"Files found: {len(files)}")
for f in files[:5]:
    print(f"  {f.name}  ({f.size:,} bytes)")
```

---

### Step 4 — Run Cell 3: Execute Bronze Ingestion with Auto Loader

```python
from src.bronze.ingest_eeg_files import create_bronze_table

create_bronze_table(spark, cfg, trigger_once=True)
```

**Internal execution flow of `create_bronze_table()`:**

1. `load_raw_files()` initialises the streaming source:

   ```python
   spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "binaryFile")
       .option("cloudFiles.schemaLocation", cfg.paths.autoloader_schema_location)
       .option("cloudFiles.useNotifications", "false")
       .option("cloudFiles.includeExistingFiles", "true")
       .option("pathGlobFilter", "*.edf")
       .load(cfg.paths.volume_edf_dir)
   ```

2. UDF enrichment is applied per row:
   - `_extract_subject_id_udf` → `subject_id`
   - `_extract_study_night_udf` → `study_night`
   - `is_hypnogram_file` → `is_hypnogram`

3. Audit columns are added:
   - `ingestion_timestamp = F.current_timestamp()`
   - `dataset_source = "physionet-sleep-edf"`

4. The stream writes to Delta:

   ```python
   df.writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", cfg.paths.autoloader_checkpoint)
     .trigger(availableNow=True)
     .toTable(cfg.catalog.bronze_edf_fqn)
     .awaitTermination()
   ```

**Expected runtime:** ~15–30 seconds for 8 files; ~5–10 minutes for the full 394-file dataset.

---

### Step 5 — Run Cell 4: Inspect Delta Transaction History

```python
from pyspark.sql import functions as F

spark.sql(f"DESCRIBE HISTORY {cfg.catalog.bronze_edf_fqn}").show(truncate=False)
```

**Expected output columns:**

| Column | Example value |
|---|---|
| `version` | `0` |
| `timestamp` | `2026-06-22 00:15:32.123` |
| `operation` | `STREAMING UPDATE` |
| `operationMetrics` | `{"numAddedFiles":"8","numOutputRows":"8"}` |
| `userName` | `your-email@example.com` |

**Exam patterns based on `DESCRIBE HISTORY`:**

| Question | Answer |
|---|---|
| What is version 0? | The initial table write (empty table creation or first streaming batch) |
| What is `STREAMING UPDATE`? | Auto Loader writes via Structured Streaming; each micro-batch creates one version |
| How do you query version 0? | `SELECT * FROM eeg_lakehouse.bronze.raw_eeg_files VERSION AS OF 0` |
| How do you roll back to version 1? | `RESTORE TABLE eeg_lakehouse.bronze.raw_eeg_files TO VERSION AS OF 1` |
| How do you audit who deleted rows? | `DESCRIBE HISTORY` shows `operation = DELETE` with `userName` and `timestamp` |

---

### Step 6 — Run Cell 5: Compare Trigger Types

This cell does not execute any streaming job — it prints the reference table for exam study.

```python
trigger_reference = [
    ("availableNow", ".trigger(availableNow=True)",
     "Processes all pending files in micro-batches, then stops",
     "Batch-style incremental loads (recommended for production)"),
    ("once",         ".trigger(once=True)",
     "Processes everything as one batch, then stops",
     "Deprecated since Spark 3.3; use availableNow instead"),
    ("processingTime",".trigger(processingTime='5 min')",
     "Runs continuously, triggering every 5 minutes",
     "Near-real-time pipelines"),
    ("continuous",   ".trigger(continuous='1 sec')",
     "Ultra-low latency; limited sink support",
     "Sub-second Kafka ingestion"),
]

print(f"{'Trigger':<16} {'Syntax':<38} {'Behaviour':<45} {'Use case'}")
print("-" * 140)
for row in trigger_reference:
    print(f"{row[0]:<16} {row[1]:<38} {row[2]:<45} {row[3]}")
```

> **Exam tip**: For EDF file ingestion (batch delivery), `availableNow=True` is correct. `availableNow` is superior to `once` because it processes files in optimised micro-batches with better parallelism and does not time out on large datasets.

---

### Step 7 — Run Cell 6: Data Quality Checks with Assertions

```python
import pyspark.sql.functions as F

bronze_df = spark.table(cfg.catalog.bronze_edf_fqn)

null_subjects = bronze_df.filter(F.col("subject_id").isNull()).count()
assert null_subjects == 0, f"FAIL: {null_subjects} null subject_ids found"

psg_count = bronze_df.filter(~F.col("is_hypnogram")).count()
hyp_count = bronze_df.filter(F.col("is_hypnogram")).count()
assert psg_count > 0, "FAIL: No PSG files found"
assert hyp_count > 0, "FAIL: No Hypnogram files found"
assert psg_count == hyp_count, (
    f"FAIL: PSG count ({psg_count}) != Hypnogram count ({hyp_count})"
)

wrong_source = bronze_df.filter(
    F.col("dataset_source") != "physionet-sleep-edf"
).count()
assert wrong_source == 0, f"FAIL: {wrong_source} files with wrong dataset_source"

print("PASS: All quality checks passed.")
print(f"  PSG files: {psg_count}  |  Hypnogram files: {hyp_count}")

bronze_df.groupBy("recording_type", "is_hypnogram").count().show()
```

**Expected output (8-file sample):**

```
PASS: All quality checks passed.
  PSG files: 4  |  Hypnogram files: 4

+--------------+------------+-----+
|recording_type|is_hypnogram|count|
+--------------+------------+-----+
|SC            |false       |4    |
|SC            |true        |4    |
+--------------+------------+-----+
```

---

### Step 8 — Verify Idempotency by Re-running Ingestion

```python
count_before = spark.table(cfg.catalog.bronze_edf_fqn).count()

create_bronze_table(spark, cfg, trigger_once=True)

count_after = spark.table(cfg.catalog.bronze_edf_fqn).count()

print(f"Row count before re-run : {count_before}")
print(f"Row count after re-run  : {count_after}")
assert count_before == count_after, (
    f"FAIL: Duplicate records created. Before={count_before}, After={count_after}"
)
print("PASS: Idempotency verified — no duplicates created.")
```

**Why idempotency is guaranteed:**
Auto Loader stores a list of `(file_path, modification_time)` tuples in the checkpoint directory. On the second run, every file is already in the checkpoint, so the streaming query terminates immediately without writing any new rows.

> **Exam question**: “What happens if you delete the checkpoint directory and re-run ingestion?” Auto Loader re-ingests all files, creating duplicates. This is why checkpoint management is critical in production.

---

### Step 9 — Run Cell 7: Ingest the Bronze Metadata Table

The `eeg_lakehouse.bronze.eeg_metadata` table stores subject-level demographics (age, sex, medication status) parsed from PhysioNet annotation headers.

```python
from src.bronze.ingest_metadata import create_bronze_metadata_table

create_bronze_metadata_table(spark, cfg)

spark.table("eeg_lakehouse.bronze.eeg_metadata").show(5)
```

**Expected schema and sample output:**

```
+----------+---+----+----------+--------------------+
|subject_id|age|sex |medication|dataset_source      |
+----------+---+----+----------+--------------------+
|SC4001    |40 |M   |None      |physionet-sleep-edf |
|SC4002    |40 |F   |None      |physionet-sleep-edf |
|SC4003    |44 |M   |None      |physionet-sleep-edf |
|SC4004    |44 |F   |None      |physionet-sleep-edf |
+----------+---+----+----------+--------------------+
```

This metadata will be joined with Gold-layer features in Days 9 and 10.

---

### Step 10 — Exam Reflection Questions

1. What is the difference between `availableNow` and `processingTime` triggers?
2. Where is the Auto Loader checkpoint stored, and what happens if you delete it?
3. How do you verify that Bronze ingestion is idempotent?
4. What does `DESCRIBE HISTORY` return, and how does it differ from `DESCRIBE DETAIL`?
5. Why is the Bronze table a metadata registry rather than a store of raw signal bytes?
6. What Delta feature provides exactly-once semantics for Auto Loader?

**Answers:**

1. `availableNow`: Processes all pending files in micro-batches and then stops — batch-style. `processingTime`: Runs indefinitely, triggering every N minutes — near-real-time.
2. Stored at `cfg.paths.autoloader_checkpoint`. Deleting it causes Auto Loader to re-ingest all files, creating duplicates.
3. Re-run ingestion and assert that the row count is unchanged. Auto Loader’s checkpoint ensures already-processed files are not re-read.
4. `DESCRIBE HISTORY`: Audit log (versions, operations, user, timestamp). `DESCRIBE DETAIL`: Physical table metadata (file count, size, partition count).
5. EDF parsing requires MNE-Python, which cannot run in a JVM executor. Raw signal decoding is deferred to the Silver layer, which uses `mapInPandas`.
6. Auto Loader checkpoint + Delta transaction log together guarantee exactly-once delivery.

---

## Day 3 Artefact Summary

| Artefact | Source |
|---|---|
| Unity Catalog structure (catalog, schemas, volume) | SQL DDL |
| PhysioNet EDF upload to UC Volume | `dbutils.fs.cp` / `wfdb` library |
| Bronze table ingestion via Auto Loader | `src/bronze/ingest_eeg_files.py` |
| Delta transaction history inspection | `DESCRIBE HISTORY` |
| Data quality assertion checks | Spark DataFrame assertions |
| Idempotency verification | Auto Loader checkpoint re-run |
| Bronze subject-metadata table | `src/bronze/ingest_metadata.py` |

**Next**: Day 4 builds the Silver layer with EEG signal preprocessing (bandpass filtering, artifact removal) using Pandas UDFs and MNE-Python.
