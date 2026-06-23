# Day 4: Silver Layer Preprocessing — EEG Signal Cleaning

**Notebook**: `notebooks/day04_silver_preprocessing.py`  
**Source modules**: `src/silver/preprocess_eeg.py`  
**Exam domains**: Pandas UDFs and `mapInPandas` (Domain 2 — ELT with Spark SQL & Python), Delta partitioning (Domain 1 — Lakehouse Platform)  
**Time estimate**: 2–3 hours  
**Prerequisite**: Day 3 completed; Bronze EDF table (`eeg_lakehouse.bronze.raw_eeg_files`) populated

---

## Objectives

- Apply `mapInPandas` for full-partition EDF file preprocessing
- Understand the distinction between Pandas UDF types and when to apply each (exam critical)
- Parse EDF files using MNE-Python within Spark partitions
- Write the Silver epochs Delta table, partitioned by `subject_id`
- Compute band-power features (sigma, delta, theta) per epoch
- Inspect signal quality distributions and artifact rates

---

## Background

EDF file parsing requires MNE-Python (`mne.io.read_raw_edf`), which is a Python-only library that needs access to the entire file as bytes. Standard Spark UDFs operate row-by-row, making them unsuitable for file-level operations. `mapInPandas` gives the user-defined function an entire partition as a `pd.DataFrame`, allowing arbitrary Python operations (MNE, YASA, SciPy) and returning a new `pd.DataFrame` with a predefined schema.

### Pandas UDF vs. `mapInPandas` — Exam Decision Guide

| Method | Access pattern | Typical EEG use case |
|---|---|---|
| `@pandas_udf` (scalar) | One column → one column | Simple per-value transforms: `log(sigma_power)`, `sqrt(x)` |
| `@pandas_udf` (grouped aggregate) | Group → aggregate | Per-subject mean power across all epochs |
| `mapInPandas(func, schema)` | Full partition as `pd.DataFrame` | EDF file reading, epoch segmentation, TDA computation |
| `mapInArrow(func, schema)` | Full partition as Arrow `RecordBatch` | Same as `mapInPandas` but with native Arrow memory format |

For this project, **`mapInPandas` is required** because `mne.io.read_raw_edf(file_path)` requires a file path and returns a full MNE Raw object that cannot be expressed as a row-level transform.

---

## Environment Setup

> **Complete all steps in this section before proceeding to any notebook instructions.**

### Step A — Prerequisites Check

1. A Databricks workspace is provisioned with Unity Catalog enabled.
2. Day 3 is complete and `eeg_lakehouse.bronze.raw_eeg_files` contains at least one row.
3. Workspace admin or catalog owner permissions are available.

---

### Step B — Generate a GitHub Personal Access Token (PAT)

Skip this step if GitHub is already connected to Databricks from a prior session.

1. Open [https://github.com/settings/tokens](https://github.com/settings/tokens).
2. Click **Generate new token (classic)**.
3. Set **Note** to `databricks-repos-access`, set **Expiration** to 90+ days, and enable the `repo` scope.
4. Click **Generate token** and copy it immediately.

---

### Step C — Configure Databricks Git Integration

1. Click your username (top-right) > **User Settings** > **Linked Accounts** (or **Git Integration** / **Developer Settings > Git credentials** on DBR 14.3+).
2. Set **Git provider** to `GitHub`, paste the PAT, and click **Save**.

---

### Step D — Verify the Cloned Repository

1. In the left sidebar, navigate to **Workspace > Repos > your-email@example.com**.
2. Confirm `databricks-eeg-lakehouse-lab` is present. If it is not, click **Add Repo** and enter:

   | Field | Value |
   |---|---|
   | **Git repository URL** | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git` |
   | **Git provider** | `GitHub` |
   | **Repo name** | `databricks-eeg-lakehouse-lab` |

---

### Step E — Create or Verify a Unity-Catalog-Enabled Cluster

1. Navigate to **Compute** and locate or create `eeg-lab-cluster`.
2. Confirm the following settings:

   | Setting | Required Value |
   |---|---|
   | **Databricks Runtime** | `14.3 LTS (Scala 2.12, Spark 3.5.0)` or higher |
   | **Node type** | `Standard_DS3_v2` (Azure) — 14 GB RAM, 4 vCores |
   | **Unity Catalog** | Enabled |
   | **Auto-termination** | 30 minutes |

3. Under **Libraries**, verify that the following PyPI packages are installed:

   | Library | Required Version |
   |---|---|
   | `mne` | `1.6.1` |
   | `pyedflib` | `0.1.34` |
   | `scipy` | `1.12.0` |
   | `yasa` | `0.6.4` |

4. Wait for the cluster to reach **Running** state.

---

### Step F — Open and Attach the Day 4 Notebook

1. Navigate to **Workspace > Repos > your-email@example.com > databricks-eeg-lakehouse-lab > notebooks**.
2. Open **day04_silver_preprocessing.py**.
3. Click the **Connect** dropdown (top-right) and select `eeg-lab-cluster`.
4. Wait for the connection indicator to turn green.

---

## Step-by-Step Notebook Instructions

### Step 1 — Run Cell 1: Setup Imports and Verify Library Versions

```python
import sys
import os

sys.path.insert(0, os.path.join(os.getcwd(), ".."))

import mne
import scipy
import yasa

print(f"MNE version   : {mne.__version__}")
print(f"SciPy version : {scipy.__version__}")
print(f"YASA version  : {yasa.__version__}")

from src.utils.config import AppConfig
from src.silver.preprocess_eeg import preprocess_eeg, SILVER_EPOCH_SCHEMA
from pyspark.sql import functions as F

cfg = AppConfig()
print("Silver Epochs FQN:", cfg.catalog.silver_epochs_fqn)
```

**Expected output:**

```
MNE version   : 1.6.1
SciPy version : 1.12.0
YASA version  : 0.6.4
Silver Epochs FQN: eeg_lakehouse.silver.cleaned_epochs
```

---

### Step 2 — Run Cell 2: Read Bronze EDF Records

```python
bronze_df = spark.table(cfg.catalog.bronze_edf_fqn)
psg_df = bronze_df.filter(~F.col("is_hypnogram"))

print(f"Total EDF records in Bronze  : {bronze_df.count()}")
print(f"PSG records (signal files)   : {psg_df.count()}")
print(f"Hypnogram records            : {bronze_df.filter(F.col('is_hypnogram')).count()}")
```

**Expected output (4-subject sample):**

```
Total EDF records in Bronze  : 8
PSG records (signal files)   : 4
Hypnogram records            : 4
```

---

### Step 3 — Run Cell 3: Execute Silver Preprocessing with `mapInPandas`

```python
from src.silver.preprocess_eeg import preprocess_eeg

silver_epochs_df = preprocess_eeg(bronze_df, cfg)

print("Silver preprocessing complete.")
silver_epochs_df.printSchema()
```

**Internal execution flow inside `preprocess_eeg()`:**

1. Filters PSG files: `bronze_df.filter(~F.col("is_hypnogram"))`
2. Calls `psg_df.mapInPandas(_preprocess_partition, schema=SILVER_EPOCH_SCHEMA)`
3. `_preprocess_partition` receives an `Iterator[pd.DataFrame]` and, for each row:
   - Opens the EDF file: `raw = mne.io.read_raw_edf(file_path, preload=True, verbose=False)`
   - Applies bandpass filter (0.5–40 Hz): `raw.filter(l_freq=0.5, h_freq=40.0)`
   - Applies notch filter (50 Hz power-line): `raw.notch_filter(freqs=50.0)`
   - Segments the signal into 30-second epochs
   - Matches epochs with hypnogram annotations to assign a sleep stage label per epoch
   - Computes band powers (delta, theta, sigma) using `scipy.signal.welch`
   - Flags artifacts using a z-score threshold of 3.0
4. Returns a `pd.DataFrame` conforming to `SILVER_EPOCH_SCHEMA`

**Silver epoch schema:**

| Column | Type | Nullability | Description |
|---|---|---|---|
| `subject_id` | `StringType` | NOT NULL | Parsed from Bronze filename |
| `study_night` | `IntegerType` | Nullable | 0 or 1; `null` for hypnogram rows |
| `epoch_idx` | `IntegerType` | NOT NULL | Zero-based epoch index within the recording |
| `epoch_start_sec` | `FloatType` | NOT NULL | Start time in seconds from recording start |
| `epoch_end_sec` | `FloatType` | NOT NULL | End time (= `epoch_start_sec + 30.0`) |
| `sleep_stage` | `StringType` | Nullable | One of: `W`, `N1`, `N2`, `N3`, `REM` |
| `signal_blob` | `BinaryType` | Nullable | Compressed numpy array (gzip + `numpy.save`) |
| `sample_rate_hz` | `IntegerType` | NOT NULL | Sampling frequency (100 Hz for Sleep-EDF) |
| `is_artifact` | `BooleanType` | NOT NULL | `True` if z-score > 3.0 |
| `sigma_power` | `FloatType` | Nullable | Band power 11–16 Hz (sleep spindles) |
| `delta_power` | `FloatType` | Nullable | Band power 0.5–4 Hz (slow-wave sleep) |
| `dataset_source` | `StringType` | Nullable | `"physionet-sleep-edf"` |

---

### Step 4 — Run Cell 4: Write the Silver Table with Delta Partitioning

```python
(
    silver_epochs_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("subject_id")
    .saveAsTable(cfg.catalog.silver_epochs_fqn)
)

print(f"Silver epochs written to: {cfg.catalog.silver_epochs_fqn}")
```

**Partitioning rationale:**

| Candidate column | Cardinality | Recommended? | Reason |
|---|---|---|---|
| `subject_id` | ~200 unique values | **Yes** | Optimal partition granularity; most Gold queries filter by subject |
| `sleep_stage` | 5 unique values | No | Too few partitions; does not reduce data scanned for subject queries |
| `epoch_idx` | 1000+ per subject | No | Thousands of tiny files; creates the small-file problem |
| `is_artifact` | 2 unique values | No | Extremely low cardinality; minimal benefit |

---

### Step 5 — Run Cell 5: Inspect the Silver Table

```python
silver_df = spark.table(cfg.catalog.silver_epochs_fqn)

print(f"Total epochs: {silver_df.count()}")

print("\nSleep stage distribution:")
silver_df.groupBy("sleep_stage").count().orderBy("sleep_stage").show()
```

**Expected output:**

```
Total epochs: 2400

+-----------+-----+
|sleep_stage|count|
+-----------+-----+
|N1         |180  |
|N2         |840  |
|N3         |520  |
|REM        |420  |
|W          |440  |
+-----------+-----+
```

---

### Step 6 — Run Cell 6: Inspect Band Power Distributions

```python
silver_df.select(
    F.mean("sigma_power").alias("mean_sigma"),
    F.stddev("sigma_power").alias("std_sigma"),
    F.mean("delta_power").alias("mean_delta"),
    F.stddev("delta_power").alias("std_delta"),
    F.percentile_approx("sigma_power", 0.5).alias("median_sigma"),
    F.percentile_approx("delta_power", 0.5).alias("median_delta"),
).show()
```

**Band power reference:**

| Band | Frequency range | Sleep relevance |
|---|---|---|
| Delta | 0.5–4 Hz | Slow-wave sleep (N3); high delta power → deep sleep |
| Theta | 4–8 Hz | Drowsiness and N1 sleep onset |
| Sigma / Spindle | 11–16 Hz | Sleep spindles in N2; marker of memory consolidation |

---

### Step 7 — Run Cell 7: Validate Artifact Rate

```python
total_epochs = silver_df.count()
artifact_count = silver_df.filter(F.col("is_artifact") == True).count()
artifact_rate = artifact_count / total_epochs

print(f"Total epochs     : {total_epochs}")
print(f"Artifact epochs  : {artifact_count}")
print(f"Artifact rate    : {artifact_rate:.1%}")

assert artifact_rate < 0.15, (
    f"FAIL: Artifact rate {artifact_rate:.1%} exceeds the 15% threshold."
    " Verify bandpass filter settings and z-score threshold."
)
print("PASS: Artifact rate is within acceptable bounds.")
```

**Expected artifact rate:** 5–10% (z-score > 3.0 threshold applied to all channels).

---

### Step 8 — Exam Reflection Questions

1. Why is `mapInPandas` used for EDF parsing instead of a scalar Pandas UDF?
2. What does `signal_blob` contain, and why is it stored as `BinaryType`?
3. Why is the Silver table partitioned by `subject_id` and not `epoch_idx`?
4. What is the difference between `@pandas_udf(returnType=DoubleType())` and `mapInPandas(func, schema)`?
5. What Delta write mode is used for Silver, and why not `append`?
6. What bandpass filter range is applied, and which EEG bands does it preserve?

**Answers:**

1. `mapInPandas` delivers an entire partition as a `pd.DataFrame`, enabling file-level operations such as `mne.io.read_raw_edf(file_path)`. Scalar Pandas UDFs operate column-by-column and cannot open files or perform arbitrary Python I/O.
2. `signal_blob` stores the epoch’s raw signal samples as a gzip-compressed numpy array. `BinaryType` is used to allow lazy decompression — only Gold tasks that require raw signals pay the decompression cost.
3. `subject_id` has ~200 unique values, yielding optimal partition granularity. `epoch_idx` exceeds 1,000 per subject, which would produce thousands of tiny files and degrade read performance.
4. `@pandas_udf` operates on Series or grouped DataFrames (vectorised column transforms). `mapInPandas` delivers an entire partition and returns a new `pd.DataFrame` with an arbitrary schema — required for file I/O and multi-output operations.
5. `overwrite` — Silver preprocessing is a full refresh that re-processes all PSG files from Bronze. `append` would create duplicates on re-run.
6. 0.5–40 Hz: removes DC drift (below 0.5 Hz) and high-frequency noise (above 40 Hz). Preserved bands: delta (0.5–4 Hz), theta (4–8 Hz), alpha (8–13 Hz), sigma/spindle (11–16 Hz), and beta (13–30 Hz).

---

## Day 4 Artefact Summary

| Artefact | Source |
|---|---|
| Pandas UDF vs. `mapInPandas` decision guide | Exam reference table |
| Silver EEG epoch preprocessing (30-second windows) | `src/silver/preprocess_eeg.py` |
| MNE-Python EDF parsing with bandpass + notch filtering | `mne.io.read_raw_edf`, `mne.filter` |
| Band power computation (sigma, delta) | `scipy.signal.welch` |
| Artifact detection (z-score > 3.0) | Statistical thresholding |
| Silver Delta table with `subject_id` partitioning | `eeg_lakehouse.silver.cleaned_epochs` |

**Next**: Day 5 explores Delta Lake internals (transaction log, time travel, OPTIMIZE, ZORDER, VACUUM).
