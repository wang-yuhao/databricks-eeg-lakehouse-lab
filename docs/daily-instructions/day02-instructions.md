# Day 2: Dataset Interface & Bronze Schema Design

**Notebook**: `notebooks/day02_bronze_schema_design.py`
**Source modules**: `src/bronze/ingest_eeg_files.py`, `src/utils/config.py`
**Exam domains**: Auto Loader (Domain 3 — Incremental Processing), Delta schema enforcement (Domain 1 — Lakehouse Platform)
**Time estimate**: 2–3 hours
**Prerequisite**: Day 1 completed; Databricks workspace with Unity Catalog enabled and the repository cloned into Databricks Repos

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

Sleep-EDF filename convention: `SC4001E0-PSG.edf`

| Segment | Meaning |
|---|---|
| `SC` | Sleep Cassette (hospital recording) |
| `ST` | Sleep Telemetry (home recording) |
| `40` | Subject age |
| `01` | Subject number |
| `E0` / `E1` | Study night 0 or 1 |
| `EC` | Hypnogram cassette |
| `PSG` | Polysomnography signal file |
| `Hypnogram` | Sleep stage annotation file |

---

## Environment Setup: Connect the Repository to Databricks Repos

> **Important**: Complete this section before proceeding to any notebook steps. All instructions below assume you are running code inside a Databricks notebook that has been opened via Repos.

### Step A — Prerequisites Check

Before connecting the repository, confirm the following:

1. You have a Databricks workspace provisioned (Azure Databricks, AWS, or GCP Databricks).
2. Unity Catalog is enabled on your workspace. Verify this by navigating to **Catalog** in the left sidebar — you should see your catalog hierarchy (`eeg_lakehouse` or equivalent).
3. You have a personal access token (PAT) for GitHub, or your GitHub account is connected via OAuth.
4. Git is accessible from your workspace (it is included by default in all Databricks workspaces).

---

### Step B — Generate a GitHub Personal Access Token (PAT)

If you have already connected GitHub via OAuth in a previous session, skip to Step C.

1. In your browser, navigate to [https://github.com/settings/tokens](https://github.com/settings/tokens).
2. Click **Generate new token (classic)**.
3. Set a descriptive name, e.g. `databricks-repos-access`.
4. Set the expiry to at least 90 days.
5. Under **Scopes**, select:
   - `repo` (full repository access — required to read and write code)
6. Click **Generate token** and copy the token immediately. You will not be able to see it again.

---

### Step C — Connect GitHub to Databricks via Git Integration

1. In your Databricks workspace, click your **username** in the top-right corner.
2. Select **User Settings** from the dropdown.
3. Navigate to the **Linked Accounts** tab (or **Git Integration** tab, depending on your Databricks version).
4. Under **Git provider**, select `GitHub`.
5. In the **Token** field, paste the PAT you generated in Step B.
6. Click **Save**. A green confirmation message should appear.

> **Databricks Runtime 14.3+ note**: In newer workspaces, Git integration may be managed under **Settings > Developer Settings > Git credentials**. The steps are equivalent.

---

### Step D — Add the Repository to Databricks Repos

1. In the left sidebar, click **Workspace**.
2. Navigate to the **Repos** section (it appears as a folder icon labelled `Repos` under your username, e.g. `Repos / your-email@example.com /`).
3. Click the **Add Repo** button (top-right of the Repos panel, or via **+** icon).
4. In the dialog that appears:
   - **Git repository URL**: `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git`
   - **Git provider**: `GitHub`
   - **Repo name** (optional override): `databricks-eeg-lakehouse-lab`
5. Click **Create Repo**.
6. Databricks will clone the repository. Once complete, you will see the folder structure:
   ```
   Repos/
   └── your-email@example.com/
       └── databricks-eeg-lakehouse-lab/
           ├── notebooks/
           ├── src/
           ├── docs/
           └── ...
   ```

> If you encounter an authentication error, return to Step C and verify the PAT is saved correctly.

---

### Step E — Create or Attach a Unity-Catalog-Enabled Cluster

1. In the left sidebar, navigate to **Compute**.
2. Click **Create Compute** (or **New Cluster** in older UI versions).
3. Configure the cluster as follows:

   | Setting | Recommended Value |
   |---|---|
   | **Cluster name** | `eeg-lab-cluster` |
   | **Cluster mode** | Single-node (for Day 2 exploration) or Standard |
   | **Databricks Runtime** | `14.3 LTS (Scala 2.12, Spark 3.5.0)` or higher |
   | **Node type** | `Standard_DS3_v2` (Azure) or equivalent — 14 GB RAM, 4 cores |
   | **Unity Catalog** | Enabled (this is the default for DBR 13.3+) |
   | **Auto-termination** | 30 minutes |

4. Under **Advanced Options > Spark**, add the following environment variable to ensure Unity Catalog catalog defaults are loaded correctly:

   ```
   DATABRICKS_ENV=dev
   AUTOLOADER_CHECKPOINT=/Volumes/eeg_lakehouse/bronze/checkpoints/autoloader
   ```

5. Under **Libraries**, click **Install New** and add the following PyPI packages one at a time (or upload `requirements.txt`):

   | Library | Version |
   |---|---|
   | `mne` | `1.6.1` |
   | `pyedflib` | `0.1.34` |
   | `scipy` | `1.12.0` |
   | `yasa` | `0.6.4` |

   Alternatively, attach the entire `requirements.txt` from the repo root by selecting **File** > Upload and pointing to `requirements.txt`.

6. Click **Create Cluster** and wait for the cluster to reach **Running** state (typically 3–5 minutes).

---

### Step F — Open the Day 2 Notebook

1. In the left sidebar, navigate to **Workspace > Repos > your-email@example.com > databricks-eeg-lakehouse-lab > notebooks**.
2. Click **day02_bronze_schema_design.py** to open it.
3. In the top-right of the notebook, click the **Connect** dropdown and select the cluster you created in Step E (`eeg-lab-cluster`).
4. Wait for the cluster indicator to turn green (Connected).

You are now ready to execute the notebook cells in order.

---

## Step-by-Step Notebook Instructions

### Step 1 — Run Cell 1: Validate the AppConfig Object

This cell imports the project configuration module and prints all key paths and catalog names that will be used throughout the pipeline.

```python
import sys, os

# Add the repository root to the Python path so that src/ modules are importable
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

**Key concepts to understand:**

- `AppConfig` uses Python `dataclasses` with `field(default_factory=lambda: os.getenv(...))` — this is the standard production configuration pattern that avoids hardcoded secrets and paths.
- Unity Catalog FQNs follow the three-part format: `catalog.schema.table` (e.g., `eeg_lakehouse.bronze.raw_eeg_files`).
- The catalog name is overridable via the `DATABRICKS_ENV` environment variable, which you set in Step E. This enables environment-specific isolation (`dev`, `staging`, `prod`).

---

### Step 2 — Run Cell 2: Verify EDF Filename Parsing Logic

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

**Implementation details** (from `src/bronze/ingest_eeg_files.py`):

- `_EDF_FILENAME_PATTERN` is a pre-compiled regex that matches the Sleep-EDF naming convention. Compiling once at module load avoids repeated compilation during streaming ingestion.
- `extract_subject_id()` concatenates the recording type, age group, and subject number segments (e.g., `SC` + `40` + `01` → `SC4001`).
- `extract_study_night()` reads the trailing digit of the night code (`E0` → `0`, `E1` → `1`). Returns `None` for hypnogram files, which use `EC` as the night code.
- `is_hypnogram_file()` checks for the literal substring `"Hypnogram"` in the filename.

> **Common pitfall**: Hypnogram files use the night code `EC` rather than `E0` or `E1`. This causes `extract_study_night` to return `None` — this behaviour is intentional and correct. A null study night signals that no signal data is associated with this file.

---

### Step 3 — Run Cell 3: Inspect the Explicit Bronze Schema

This cell creates an empty DataFrame from the pre-defined `BRONZE_EDF_SCHEMA` and prints the schema to verify the column names, types, and nullability constraints.

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
| `subject_id` | Nullable | Regex extraction may fail on unexpected filenames; null is a safe, informative fallback |
| `is_hypnogram` | NOT NULL | This is a deterministic boolean derived from filename suffix — it must always be set |
| `ingestion_timestamp` | NOT NULL | Audit trail requirement — the exact moment a file entered the pipeline must be recorded |
| `dataset_source` | NOT NULL | Multi-dataset pipelines require an unambiguous origin tag on every record |

> **Exam insight**: Always prefer explicit schemas over `inferSchema=True` in production Bronze pipelines. `inferSchema` requires a full data scan on every pipeline restart, introduces risk of type drift between runs, and cannot express nullability constraints.

---

### Step 4 — Run Cell 4: Study Auto Loader Configuration Options

This cell makes the Auto Loader options visible for inspection and discussion. These options are defined inside `load_raw_files()` in `src/bronze/ingest_eeg_files.py`.

```python
auto_loader_options = {
    # EDF is a binary format; binaryFile mode yields path, length,
    # modificationTime, and content columns without attempting to parse the binary body.
    "cloudFiles.format": "binaryFile",

    # Persists the inferred schema as JSON in a UC Volume path.
    # On restart, schema is loaded from this location rather than re-inferred.
    "cloudFiles.schemaLocation": cfg.paths.autoloader_schema_location,

    # Directory listing mode: Databricks scans the Volume directory to detect new files.
    # Set to "true" for event-based notification (Azure Event Grid / AWS S3 events) in production.
    "cloudFiles.useNotifications": "false",

    # Ensures files already present in the Volume before the first pipeline run
    # are included in the initial ingestion batch.
    "cloudFiles.includeExistingFiles": "true",

    # Filters files by extension. Any non-.edf files in the same Volume directory are ignored.
    "pathGlobFilter": "*.edf",
}

print(f"{'Option':<45} {'Value'}")
print("-" * 75)
for k, v in auto_loader_options.items():
    print(f"{k:<45} {v}")
```

**Option reference:**

| Option | Value | Why it matters |
|---|---|---|
| `cloudFiles.format` | `binaryFile` | EDF is binary — this mode reads files as raw bytes and exposes metadata columns (`path`, `length`, `modificationTime`) without attempting to parse the content |
| `cloudFiles.schemaLocation` | UC Volume path | Stores the inferred schema as a JSON file between runs; prevents re-inference and enables schema evolution tracking |
| `cloudFiles.useNotifications` | `false` | Uses directory listing (polling); simpler to configure than cloud bucket notifications, suitable for batch workloads |
| `cloudFiles.includeExistingFiles` | `true` | Processes files that existed in the Volume before the pipeline first ran; set to `false` only when you want strict new-file-only semantics |
| `pathGlobFilter` | `*.edf` | Restricts ingestion to EDF files; ignores auxiliary files (`.txt`, `.csv`, `.json`) that may share the same Volume directory |

> **Exam tip**: `cloudFiles.schemaLocation` is what gives Auto Loader its **exactly-once** restart semantics. If a streaming job crashes mid-run, re-starting it picks up from the Auto Loader checkpoint and does not re-process files that were already written to the Delta table.

---

### Step 5 — Run Cell 5 (Optional): Trigger Bronze Table Creation

Run this cell only if a UC-enabled cluster is active **and** EDF files are already uploaded to the Volume at `/Volumes/eeg_lakehouse/bronze/raw_edf/`.

If you have not yet uploaded EDF files, skip this cell and return to it during Day 3.

```python
from src.bronze.ingest_eeg_files import create_bronze_table

# trigger(availableNow=True) processes all pending files in a single micro-batch, then stops.
# This is preferred over trigger(once=True), which is deprecated as of Spark 3.3.
create_bronze_table(spark, cfg, trigger_once=True)
```

**What `create_bronze_table()` does internally:**

1. Calls `load_raw_files()`, which initialises `spark.readStream.format("cloudFiles")` with the options documented in Step 4.
2. Registers `_extract_subject_id_udf` and `_extract_study_night_udf` as Spark UDFs (applied per row during streaming ingestion).
3. Appends `ingestion_timestamp = F.current_timestamp()` and `dataset_source = "physionet-sleep-edf"` as literal columns.
4. Writes the stream to `eeg_lakehouse.bronze.raw_eeg_files` using `.trigger(availableNow=True).toTable()`.
5. Calls `query.awaitTermination()` to block the driver process until the micro-batch completes.

After the cell completes, verify the table was created:

```python
display(spark.table("eeg_lakehouse.bronze.raw_eeg_files").limit(10))
```

---

### Step 6 — Self-Check: Exam Reflection Questions

Answer the following questions without referring to the source code. These map directly to exam question patterns.

1. Why does Auto Loader use `binaryFile` format for EDF files instead of `csv` or `parquet`?
2. What happens if you delete the `schemaLocation` directory and then re-run the ingestion pipeline?
3. Why does the Bronze layer in this project store only file metadata, not raw EEG signal values?
4. What does `cloudFiles.includeExistingFiles = true` do, and what would happen if it were set to `false` on the very first run?
5. Why is `subject_id` nullable in the Bronze schema but `is_hypnogram` is defined as NOT NULL?
6. What is the fully-qualified Unity Catalog name for the Bronze EDF table in this project?

**Answers:**

1. EDF is a binary format — `binaryFile` mode reads it as raw bytes with metadata columns (`path`, `length`, `modificationTime`, `content`). Using `csv` or `parquet` would cause a parse failure.
2. Auto Loader loses all tracking state. On restart it re-scans the entire directory and re-ingests all files, creating duplicate rows in the Bronze table.
3. EDF signal parsing requires MNE-Python, which is Python-only and cannot run natively in a JVM Spark executor. Signal decoding is deferred to the Silver layer where `mapInPandas` (Pandas UDF) enables Python-based processing at scale.
4. `includeExistingFiles = true` ensures the first pipeline run ingests all files already present in the Volume. With `false`, only files arriving **after** the pipeline started would be processed — all pre-existing files would be silently skipped.
5. `subject_id` is extracted via regex and may be null if a filename does not match the expected pattern (safe, informative fallback). `is_hypnogram` is a deterministic boolean check on a filename suffix — it is always either `True` or `False` and therefore must never be null.
6. `eeg_lakehouse.bronze.raw_eeg_files`

---

## Day 2 Build Summary

| Artefact | Source File |
|---|---|
| AppConfig exploration and FQN verification | `src/utils/config.py` |
| EDF filename parsing validation (5 test filenames) | `src/bronze/ingest_eeg_files.py` |
| Explicit Bronze schema review (`BRONZE_EDF_SCHEMA`) | `src/bronze/ingest_eeg_files.py` |
| Auto Loader options reference and documentation | `src/bronze/ingest_eeg_files.py` |
| Optional: Bronze table creation via Auto Loader | `notebooks/day02_bronze_schema_design.py` |

**Next**: Day 3 executes the full Auto Loader Bronze ingestion run, inspects the Delta transaction log, and validates data quality with Great Expectations assertions.
