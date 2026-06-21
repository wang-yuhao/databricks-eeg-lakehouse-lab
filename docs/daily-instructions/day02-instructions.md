# Day 2: Dataset Interface & Bronze Schema Design

**Notebook**: `notebooks/day02_bronze_schema_design.py`  
**Source modules**: `src/bronze/ingest_eeg_files.py`, `src/utils/config.py`  
**Exam domains**: Auto Loader (Domain 3 — Incremental Processing), Delta schema enforcement (Domain 1 — Lakehouse Platform)  
**Time estimate**: 2–3 hours  
**Prerequisite**: Day 1 completed; Databricks workspace with Unity Catalog enabled

---

## Objectives

- Understand the PhysioNet Sleep-EDF EDF file format and naming convention
- Design explicit Bronze schemas (know why explicit > inferSchema)
- Preview Auto Loader options for incremental EDF ingestion
- Apply `extract_subject_id` logic and verify it works

---

## Background

The Bronze layer in this project is a **metadata registry only** — it stores file-level metadata parsed from EDF filenames, not raw EEG signal samples. Raw EDF binary content is processed later in Silver via Pandas UDFs with MNE-Python. The Bronze table acts as the ingestion audit log and enables idempotent incremental loading via Auto Loader checkpoints.

Sleep-EDF filename convention: `SC4001E0-PSG.edf`
- `SC` = Sleep Cassette (hospital), `ST` = Sleep Telemetry (home)
- `40` = subject age
- `01` = subject number
- `E0` = study night 0, `E1` = study night 1, `EC` = cassette (hypnogram)
- `PSG` = polysomnography signal, `Hypnogram` = sleep stage annotations

---

## Step-by-Step Instructions

### Step 1 — Open the notebook

1. In Databricks Workspace, navigate to **Repos** > your repo > `notebooks/`
2. Open `day02_bronze_schema_design.py`
3. Attach the notebook to a cluster with **Unity Catalog enabled** (DBR 13.3 LTS or higher)
4. Confirm the cluster has `mne`, `pyedflib`, and `scipy` installed (check `requirements.txt`)

---

### Step 2 — Run Cell 1: Explore AppConfig

```python
import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.bronze.ingest_eeg_files import (
    BRONZE_EDF_SCHEMA,
    extract_subject_id,
    extract_study_night,
    is_hypnogram_file,
)

cfg = AppConfig()
print("Catalog:", cfg.catalog.catalog)
print("Bronze EDF FQN:", cfg.catalog.bronze_edf_fqn)
print("Bronze Metadata FQN:", cfg.catalog.bronze_metadata_fqn)
print("Volume EDF path:", cfg.paths.volume_edf_dir)
print("Auto Loader checkpoint:", cfg.paths.autoloader_checkpoint)
```

**What to observe:**
- `cfg.catalog.catalog` = `"eeg_lakehouse"` (default, overridable via `DATABRICKS_ENV` env var)
- `cfg.catalog.bronze_edf_fqn` = `"eeg_lakehouse.bronze.raw_eeg_files"`
- `cfg.paths.volume_edf_dir` = `"/Volumes/eeg_lakehouse/bronze/raw_edf"` (UC Volume path)
- `cfg.paths.autoloader_checkpoint` reads from `AUTOLOADER_CHECKPOINT` env var

**Why this matters for the exam:** `AppConfig` uses Python dataclasses with `field(default_factory=lambda: os.getenv(...))` — this is the production config management pattern. Unity Catalog FQNs follow the format `catalog.schema.table`.

---

### Step 3 — Run Cell 2: Verify filename parsing logic

```python
test_files = [
    "SC4001E0-PSG.edf",
    "SC4001EC-Hypnogram.edf",
    "ST7011J0-PSG.edf",
    "ST7011JC-Hypnogram.edf",
    "SC4002E0-PSG.edf",
]

print(f"{'Filename':<30} {'subject_id':<12} {'night':<8} {'hypnogram'}")
print("-" * 65)
for f in test_files:
    print(f"{f:<30} {str(extract_subject_id(f)):<12} "
          f"{str(extract_study_night(f)):<8} {is_hypnogram_file(f)}")
```

**Expected output:**
```
Filename                       subject_id   night    hypnogram
-----------------------------------------------------------------
SC4001E0-PSG.edf               SC4001       0        False
SC4001EC-Hypnogram.edf         SC4001       None     True
ST7011J0-PSG.edf               ST7011       0        False
ST7011JC-Hypnogram.edf         ST7011       None     True
SC4002E0-PSG.edf               SC4002       0        False
```

**How it works internally** (from `src/bronze/ingest_eeg_files.py`):
- `_EDF_FILENAME_PATTERN` is a compiled regex matching the Sleep-EDF naming convention
- `extract_subject_id()` returns `rec_type + age + subj_num` (e.g., `"SC4001"`)
- `extract_study_night()` reads the last character of the night group (e.g., `"E0"` → `0`)
- `is_hypnogram_file()` checks if `"Hypnogram"` appears in the filename

**Common pitfall:** Hypnogram files use `EC` as the night code (not `E0`/`E1`), so `extract_study_night` returns `None` for them — this is correct and expected.

---

### Step 4 — Run Cell 3: Inspect the explicit Bronze schema

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

empty_df = spark.createDataFrame([], BRONZE_EDF_SCHEMA)
empty_df.printSchema()
```

**Expected schema output:**
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

**Key design decisions to understand for the exam:**

| Decision | Reason |
|---|---|
| Explicit schema over `inferSchema` | Prevents type drift, avoids expensive scan on every restart |
| `file_path` NOT NULL | Every record must have a traceable source path |
| `subject_id` nullable | Regex may fail on unexpected filenames — fail gracefully |
| `is_hypnogram` NOT NULL | Boolean classification must always be definitive |
| `ingestion_timestamp` NOT NULL | Audit trail requirement — when was this file first seen |
| `dataset_source` NOT NULL | Multi-dataset support — always tag the origin |

**Exam note:** This is a Bronze table — it stores **raw metadata**, not processed signal data. The Bronze layer in Medallion Architecture should be append-only and schema-enforced but otherwise minimally transformed.

---

### Step 5 — Run Cell 4: Study Auto Loader configuration options

Review and understand each Auto Loader option used in `src/bronze/ingest_eeg_files.py`:

```python
# These options are set inside load_raw_files() in src/bronze/ingest_eeg_files.py
auto_loader_options = {
    "cloudFiles.format": "binaryFile",          # Read raw EDF bytes, not CSV/JSON
    "cloudFiles.schemaLocation": cfg.paths.autoloader_schema_location,  # Persist schema between runs
    "cloudFiles.useNotifications": "false",     # Directory listing mode (no cloud event setup needed)
    "cloudFiles.includeExistingFiles": "true",  # Process pre-existing files on first run
    "pathGlobFilter": "*.edf",                  # Only process EDF files
}

print("Auto Loader options:")
for k, v in auto_loader_options.items():
    print(f"  {k}: {v}")
```

**Option-by-option explanation:**

| Option | Value | Why |
|---|---|---|
| `cloudFiles.format` | `binaryFile` | EDF is a binary format — reading as binaryFile gives `path`, `length`, `modificationTime`, `content` columns |
| `cloudFiles.schemaLocation` | UC Volume path | Stores inferred schema as JSON — avoids re-inference on restart |
| `cloudFiles.useNotifications` | `false` | Uses directory listing — simpler, no cloud bucket notification setup |
| `cloudFiles.includeExistingFiles` | `true` | Ensures the first run picks up all files already in the Volume |
| `pathGlobFilter` | `*.edf` | Ignores any non-EDF files (e.g., `.txt` metadata files) in the same directory |

**Exam tip:** `schemaLocation` is the key that gives Auto Loader exactly-once semantics on restart. If the pipeline crashes mid-run, re-running reads from the checkpoint and does NOT re-process already-ingested files.

---

### Step 6 — Run Cell 5: (Optional) Trigger Bronze table creation

Only run this if you have a UC-enabled cluster with EDF files in the Volume:

```python
from src.bronze.ingest_eeg_files import create_bronze_table

# trigger_once=True uses .trigger(availableNow=True) — processes all pending files then stops
create_bronze_table(spark, cfg, trigger_once=True)
```

**If you do NOT yet have EDF files uploaded**, skip this cell and come back to it during Day 3 after uploading files to the Volume.

**What `create_bronze_table()` does internally:**
1. Calls `load_raw_files()` which sets up `spark.readStream.format("cloudFiles")` with the options above
2. Registers `_extract_subject_id_udf` and `_extract_study_night_udf` as Spark UDFs
3. Adds `ingestion_timestamp = F.current_timestamp()` and `dataset_source = "physionet-sleep-edf"`
4. Writes stream to `eeg_lakehouse.bronze.raw_eeg_files` via `.trigger(availableNow=True).toTable()`
5. Calls `query.awaitTermination()` to block until all files are processed

---

### Step 7 — Self-check: Answer exam reflection questions

Before ending Day 2, answer these without looking at the code:

1. Why does Auto Loader use `binaryFile` format for EDF files instead of `csv` or `parquet`?
2. What happens if you delete the `schemaLocation` directory and re-run ingestion?
3. Why is the Bronze table a metadata registry and not a table of raw EEG signal values?
4. What does `cloudFiles.includeExistingFiles = true` do — and what would happen without it?
5. Why is `subject_id` nullable in the Bronze schema but `is_hypnogram` is NOT NULL?
6. What is the Unity Catalog FQN for the Bronze EDF table in this project?

**Answers:**
1. EDF is binary — Auto Loader's `binaryFile` format reads it as raw bytes with metadata columns; `csv`/`parquet` would fail to parse the EDF binary structure
2. Auto Loader loses track of already-processed files and re-ingests everything, creating duplicates
3. EDF parsing requires MNE-Python (Python-only) — Bronze stores metadata only; actual signal parsing happens in Silver via `mapInPandas`
4. First run would skip files already in the Volume before the pipeline was started
5. `subject_id` is extracted via regex and may fail on unexpected filenames (null = safe fallback); `is_hypnogram` is a boolean derived from filename suffix — always deterministic
6. `eeg_lakehouse.bronze.raw_eeg_files`

---

## Day 2 Summary

| What was built | Source |
|---|---|
| AppConfig exploration and FQN verification | `src/utils/config.py` |
| EDF filename parsing validation (5 test files) | `src/bronze/ingest_eeg_files.py` |
| Explicit Bronze schema review (`BRONZE_EDF_SCHEMA`) | `src/bronze/ingest_eeg_files.py` |
| Auto Loader options reference table | `src/bronze/ingest_eeg_files.py` |
| Optional: Bronze table creation with Auto Loader | `notebooks/day02_bronze_schema_design.py` |

**Next**: Day 3 runs the actual Bronze ingestion with Auto Loader, inspects Delta transaction history, and runs data quality assertions.
