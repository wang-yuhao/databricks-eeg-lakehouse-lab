# Day 2: Dataset Interface & Bronze Schema Design

**Notebook**: `notebooks/day02_bronze_schema_design.py`  
**Source modules**: `src/bronze/ingest_eeg_files.py`, `src/utils/config.py`  
**Exam domains**: Auto Loader (Domain 3 — Incremental Processing), Delta schema enforcement (Domain 1 — Lakehouse Platform)  
**Time estimate**: 2–3 hours  
**Prerequisite**: A Databricks workspace with Unity Catalog enabled (DBR 13.3 LTS or higher) and the GitHub repository accessible via browser

---

## Objectives

- Connect this GitHub repository to Databricks Repos and open the Day 2 notebook from within the workspace
- Understand the PhysioNet Sleep-EDF EDF file format and its naming convention
- Design and inspect explicit Bronze schemas, and understand why explicit schemas outperform `inferSchema`
- Explore Auto Loader configuration options for incremental EDF ingestion
- Apply and verify the `extract_subject_id` parsing logic against test filenames

---

## Background

The Bronze layer in this project is a **metadata registry only** — it stores file-level metadata parsed from EDF filenames, not raw EEG signal samples. Raw EDF binary content is processed later in Silver via Pandas UDFs with MNE-Python. The Bronze table acts as the ingestion audit log and enables idempotent incremental loading via Auto Loader checkpoints.

### Sleep-EDF Filename Convention

Example: `SC4001E0-PSG.edf`

| Segment | Meaning |
|---|---|
| `SC` | Sleep Cassette (hospital recording) |
| `ST` | Sleep Telemetry (home recording) |
| `40` | Subject age group |
| `01` | Subject number within age group |
| `E0` / `E1` | Study night 0 or 1 |
| `EC` | Hypnogram cassette (annotation file) |
| `PSG` | Polysomnography signal file |
| `Hypnogram` | Sleep stage annotation file |

---

## Environment Setup

> **Complete all steps in this section before proceeding to any notebook instructions.** A reader starting from a blank Databricks workspace must complete Steps A through F in order.

### Step A — Prerequisites Check

Confirm all of the following before continuing:

1. A Databricks workspace is provisioned (Azure Databricks, AWS Databricks, or GCP Databricks).
2. Unity Catalog is enabled. Verify by navigating to **Catalog** in the left sidebar — a catalog hierarchy should be visible.
3. You have workspace **admin** or **catalog owner** permissions (required to create catalogs in Step 3 of the notebook).
4. A GitHub account exists and is accessible in the browser.

---

### Step B — Generate a GitHub Personal Access Token (PAT)

If GitHub is already connected to Databricks via OAuth from a previous session, skip to Step C.

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
4. In the **Add Repo** dialog, enter:

   | Field | Value |
   |---|---|
   | **Git repository URL** | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git` |
   | **Git provider** | `GitHub` |
   | **Repo name** | `databricks-eeg-lakehouse-lab` |

5. Click **Create Repo**. Databricks will clone the repository.
6. After cloning, the following structure should be visible:

   ```
   Repos/
   └── your-email@example.com/
       └── databricks-eeg-lakehouse-lab/
           ├── notebooks/
           ├── src/
           ├── docs/
           ├── requirements.txt
           └── ...
   ```

> If an authentication error appears, return to Step C and verify the PAT is saved correctly.

---

### Step E — Create a Unity-Catalog-Enabled Cluster

1. In the left sidebar, navigate to **Compute**.
2. Click **Create Compute** (labelled **New Cluster** in older UI versions).
3. Configure the cluster using the following settings:

   | Setting | Required Value |
   |---|---|
   | **Cluster name** | `eeg-lab-cluster` |
   | **Cluster mode** | Single-node (for Day 2 exploration) or Standard |
   | **Databricks Runtime** | `14.3 LTS (Scala 2.12, Spark 3.5.0)` or higher |
   | **Node type** | `Standard_DS3_v2` (Azure) — 14 GB RAM, 4 vCores |
   | **Unity Catalog** | Enabled (default for DBR 13.3+; do not disable) |
   | **Auto-termination** | 30 minutes |

4. Under **Advanced Options > Spark**, add the following Spark environment variables:

   ```
   DATABRICKS_ENV=dev
   AUTOLOADER_CHECKPOINT=/Volumes/eeg_lakehouse/bronze/checkpoints/autoloader
   ```

5. Under **Libraries**, click **Install New** and add the following PyPI packages:

   | Library | Required Version |
   |---|---|
   | `mne` | `1.6.1` |
   | `pyedflib` | `0.1.34` |
   | `scipy` | `1.12.0` |
   | `yasa` | `0.6.4` |

   Alternatively, select **File** > **Upload** and point to the `requirements.txt` in the repo root to install all dependencies at once.

6. Click **Create Cluster** and wait for the cluster status to show **Running** (typically 3–5 minutes).

---

### Step F — Open and Attach the Day 2 Notebook

1. In the left sidebar, navigate to **Workspace > Repos > your-email@example.com > databricks-eeg-lakehouse-lab > notebooks**.
2. Click **day02_bronze_schema_design.py** to open it.
3. In the top-right of the notebook, click the **Connect** dropdown and select `eeg-lab-cluster`.
4. Wait for the cluster indicator to turn green (Connected).

All notebook cells below are now ready to execute in sequence.

---

## Step-by-Step Notebook Instructions

### Step 1 — Validate the AppConfig Object

This cell imports the project configuration module and prints all key paths and catalog names that will be used throughout the pipeline.

```python
import sys
import os

sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import (
    BRONZE_EDF_SCHEMA,
    extract_subject_id,
    extract_study_night,
    is_hypnogram_file,
)

cfg = AppConfig()

print("=== Unity Catalog Configuration ===")
print(f"  Catalog         : {cfg.catalog.catalog}")
print(f"  Bronze EDF FQN  : {cfg.catalog.bronze_edf_fqn}")
print(f"  Bronze Meta FQN : {cfg.catalog.bronze_metadata_fqn}")
print()
print("=== Storage Paths ===")
print(f"  Volume EDF dir  : {cfg.paths.volume_edf_dir}")
print(f"  AutoLoader ckpt : {cfg.paths.autoloader_checkpoint}")
```

**Expected output:**

```
=== Unity Catalog Configuration ===
  Catalog         : eeg_lakehouse
  Bronze EDF FQN  : eeg_lakehouse.bronze.raw_eeg_files
  Bronze Meta FQN : eeg_lakehouse.bronze.subject_metadata

=== Storage Paths ===
  Volume EDF dir  : /Volumes/eeg_lakehouse/bronze/raw_edf
  AutoLoader ckpt : /Volumes/eeg_lakehouse/bronze/checkpoints/autoloader
```

**Key concepts:**

| Concept | Detail |
|---|---|
| `AppConfig` | Uses Python `dataclasses` with `field(default_factory=lambda: os.getenv(...))` — no hardcoded secrets |
| Unity Catalog FQN | Three-part format: `catalog.schema.table` (e.g., `eeg_lakehouse.bronze.raw_eeg_files`) |
| Environment override | Catalog name is overridable via `DATABRICKS_ENV` — enables `dev` / `staging` / `prod` isolation |

---

### Step 2 — Verify EDF Filename Parsing Logic

This cell exercises the three filename-parsing utility functions against a representative set of test filenames.

```python
test_files = [
    "SC4001E0-PSG.edf",
    "SC4001EC-Hypnogram.edf",
    "ST7011J0-PSG.edf",
    "ST7011JC-Hypnogram.edf",
    "SC4002E0-PSG.edf",
]

header = f"{'Filename':<30} {'subject_id':<14} {'night':<10} {'is_hypnogram'}"
print(header)
print("-" * len(header))

for fname in test_files:
    print(
        f"{fname:<30} "
        f"{str(extract_subject_id(fname)):<14} "
        f"{str(extract_study_night(fname)):<10} "
        f"{is_hypnogram_file(fname)}"
    )
```

**Expected output:**

```
Filename                       subject_id     night      is_hypnogram
----------------------------------------------------------------------
SC4001E0-PSG.edf               SC4001         0          False
SC4001EC-Hypnogram.edf         SC4001         None       True
ST7011J0-PSG.edf               ST7011         0          False
ST7011JC-Hypnogram.edf         ST7011         None       True
SC4002E0-PSG.edf               SC4002         0          False
```

**Implementation notes:**

| Function | Behaviour |
|---|---|
| `extract_subject_id()` | Concatenates recording type + age group + subject number (e.g., `SC` + `40` + `01` → `SC4001`) |
| `extract_study_night()` | Reads the trailing digit of the night code (`E0` → `0`, `E1` → `1`); returns `None` for hypnogram files (`EC`) |
| `is_hypnogram_file()` | Returns `True` if the literal substring `"Hypnogram"` is present in the filename |

> **Common pitfall**: Hypnogram files use the night code `EC` rather than `E0` or `E1`. This causes `extract_study_night` to return `None` — this behaviour is intentional. A null study night signals that no signal data is associated with this file.

---

### Step 3 — Inspect the Explicit Bronze Schema

This cell creates an empty DataFrame from the pre-defined `BRONZE_EDF_SCHEMA` and prints the schema to verify column names, types, and nullability constraints.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

empty_df = spark.createDataFrame([], BRONZE_EDF_SCHEMA)
empty_df.printSchema()
```

**Expected output:**

```
root
 |-- file_path: string (nullable = false)
 |-- file_name: string (nullable = false)
 |-- subject_id: string (nullable = true)
 |-- recording_type: string (nullable = true)
 |-- subject_age: integer (nullable = true)
 |-- study_night: integer (nullable = true)
 |-- is_hypnogram: boolean (nullable = false)
 |-- file_size_bytes: long (nullable = true)
 |-- file_modification_time: timestamp (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = false)
 |-- dataset_source: string (nullable = false)
```

**Schema design decisions:**

| Column | Nullability | Rationale |
|---|---|---|
| `file_path` | NOT NULL | Every ingested record must have a traceable storage path |
| `subject_id` | Nullable | Regex extraction may fail on unexpected filenames; `null` is a safe, informative fallback |
| `is_hypnogram` | NOT NULL | A deterministic boolean derived from filename suffix — it is always `True` or `False` |
| `ingestion_timestamp` | NOT NULL | Audit trail requirement — the exact moment a file entered the pipeline must be recorded |
| `dataset_source` | NOT NULL | Multi-dataset pipelines require an unambiguous origin tag on every record |

> **Exam insight**: Always prefer explicit schemas over `inferSchema=True` in production Bronze pipelines. `inferSchema` requires a full data scan on every pipeline restart, introduces risk of type drift between runs, and cannot express nullability constraints.

---

### Step 4 — Study Auto Loader Configuration Options

This cell makes the Auto Loader options visible for inspection. These options are defined inside `load_raw_files()` in `src/bronze/ingest_eeg_files.py`.

```python
auto_loader_options = {
    "cloudFiles.format": "binaryFile",
    "cloudFiles.schemaLocation": cfg.paths.autoloader_schema_location,
    "cloudFiles.useNotifications": "false",
    "cloudFiles.includeExistingFiles": "true",
    "pathGlobFilter": "*.edf",
}

print(f"{'Option':<45} {'Value'}")
print("-" * 75)
for k, v in auto_loader_options.items():
    print(f"{k:<45} {v}")
```

**Auto Loader option reference:**

| Option | Value | Purpose |
|---|---|---|
| `cloudFiles.format` | `binaryFile` | Reads each EDF file as raw bytes; exposes `path`, `length`, and `modificationTime` columns without attempting to parse binary content |
| `cloudFiles.schemaLocation` | UC Volume path | Persists the inferred schema as JSON between runs; prevents re-inference and enables schema evolution tracking |
| `cloudFiles.useNotifications` | `false` | Uses directory listing (polling); simpler to configure than cloud bucket event notifications; suitable for batch workloads |
| `cloudFiles.includeExistingFiles` | `true` | Processes files present in the Volume before the pipeline's first run; set to `false` only for strict new-file-only semantics |
| `pathGlobFilter` | `*.edf` | Restricts ingestion to EDF files; ignores auxiliary files in the same Volume directory |

> **Exam tip**: `cloudFiles.schemaLocation` provides Auto Loader's **exactly-once** restart semantics. If a streaming job crashes, restarting it picks up from the checkpoint and does not re-process already-written files.

---

### Step 5 — Trigger Bronze Table Creation (Optional)

Run this cell only if a UC-enabled cluster is active **and** EDF files are already uploaded to `/Volumes/eeg_lakehouse/bronze/raw_edf/`. If EDF files are not yet present, skip this step and complete it during Day 3.

```python
from src.bronze.ingest_eeg_files import create_bronze_table

# trigger(availableNow=True) processes all pending files in a single micro-batch, then stops.
# This is the recommended pattern; trigger(once=True) is deprecated as of Spark 3.3.
create_bronze_table(spark, cfg, trigger_once=True)
```

**Internal execution flow of `create_bronze_table()`:**

1. Calls `load_raw_files()`, which initialises `spark.readStream.format("cloudFiles")` with the options from Step 4.
2. Registers `_extract_subject_id_udf` and `_extract_study_night_udf` as Spark UDFs applied per row during streaming ingestion.
3. Appends `ingestion_timestamp = F.current_timestamp()` and `dataset_source = "physionet-sleep-edf"` as literal columns.
4. Writes the stream to `eeg_lakehouse.bronze.raw_eeg_files` using `.trigger(availableNow=True).toTable()`.
5. Calls `query.awaitTermination()` to block the driver until the micro-batch completes.

After the cell completes, verify the table was created:

```python
display(spark.table("eeg_lakehouse.bronze.raw_eeg_files").limit(10))
```

---

### Step 6 — Exam Reflection Questions

Answer the following questions without referring to the source code. These patterns appear directly in Databricks Data Engineer Professional exam questions.

1. Why does Auto Loader use `binaryFile` format for EDF files instead of `csv` or `parquet`?
2. What happens if you delete the `schemaLocation` directory and then re-run the ingestion pipeline?
3. Why does the Bronze layer store only file metadata, not raw EEG signal values?
4. What does `cloudFiles.includeExistingFiles = true` do, and what would happen if it were set to `false` on the very first run?
5. Why is `subject_id` nullable in the Bronze schema but `is_hypnogram` is NOT NULL?
6. What is the fully-qualified Unity Catalog name for the Bronze EDF table?

**Answers:**

1. EDF is a binary format — `binaryFile` mode reads it as raw bytes with metadata columns (`path`, `length`, `modificationTime`, `content`). Using `csv` or `parquet` would cause a parse failure.
2. Auto Loader loses all tracking state. On restart it re-scans the entire directory and re-ingests all files, creating duplicate rows in the Bronze table.
3. EDF signal parsing requires MNE-Python, which is Python-only and cannot run natively in a JVM Spark executor. Signal decoding is deferred to the Silver layer where `mapInPandas` enables Python-based processing at scale.
4. `includeExistingFiles = true` ingests all files present in the Volume at pipeline start. With `false`, only files arriving **after** the pipeline started would be processed — all pre-existing files would be silently skipped.
5. `subject_id` is extracted via regex and may be `null` if a filename does not match the expected pattern. `is_hypnogram` is a deterministic check on a filename suffix — it is always `True` or `False` and must never be `null`.
6. `eeg_lakehouse.bronze.raw_eeg_files`

---

## Day 2 Artefact Summary

| Artefact | Source |
|---|---|
| AppConfig exploration and FQN verification | `src/utils/config.py` |
| EDF filename parsing validation (5 test filenames) | `src/bronze/ingest_eeg_files.py` |
| Explicit Bronze schema review (`BRONZE_EDF_SCHEMA`) | `src/bronze/ingest_eeg_files.py` |
| Auto Loader options reference | `src/bronze/ingest_eeg_files.py` |
| Optional Bronze table creation via Auto Loader | `notebooks/day02_bronze_schema_design.py` |

**Next**: Day 3 executes the full Auto Loader Bronze ingestion run, inspects the Delta transaction log, and validates data quality with assertion-based checks.
