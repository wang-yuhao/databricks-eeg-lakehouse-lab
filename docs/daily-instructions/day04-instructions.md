# Day 4: Silver Layer Preprocessing — EEG Signal Cleaning

**Notebook**: `notebooks/day04_silver_preprocessing.py`  
**Source modules**: `src/silver/preprocess_eeg.py`  
**Exam domains**: Pandas UDFs and `mapInPandas` (Domain 2 — ELT with Spark SQL & Python), Delta partitioning (Domain 1)  
**Time estimate**: 2–3 hours  
**Prerequisite**: Day 3 completed; Bronze EDF files ingested

---

## Objectives

- Apply `mapInPandas` for full-partition EDF file preprocessing
- Understand the distinction between Pandas UDF types (exam critical)
- Parse EDF files using MNE-Python within Spark partitions
- Write Silver epochs Delta table, partitioned by `subject_id`
- Compute band power features (sigma, delta, theta) per epoch
- Inspect signal quality distributions

---

## Background

**Why Silver needs `mapInPandas` instead of regular Spark transformations:**

EDF file parsing requires MNE-Python (`mne.io.read_raw_edf`), which is a Python-only library that needs access to **entire file content as bytes**. Standard Spark UDFs operate row-by-row, but EDF parsing needs:
1. Full file content (stored as binary in Bronze `file_path`)
2. Sequential processing via MNE-Python (not parallelizable at the signal level)
3. Conversion to 30-second epochs with sleep stage labels from hypnograms

`mapInPandas` solves this by:
- Giving the UDF an entire partition as a `pd.DataFrame` (not row-by-row)
- Allowing arbitrary Python operations (MNE, YASA, scipy)
- Returning a new `pd.DataFrame` with a predefined schema

**Exam distinction (commonly tested):**

| Method | Access Pattern | Use Case |
|---|---|---|
| `@pandas_udf` (scalar) | Row → row | Simple transforms: `log(x)`, `sqrt(x)` |
| `@pandas_udf` (grouped) | Group → aggregate | Per-subject aggregations |
| `mapInPandas` | Full partition → DataFrame | EDF parsing, TDA computation, file I/O |
| `mapInArrow` | Same as `mapInPandas` | Optimized Arrow format (not needed here) |

For this project: **`mapInPandas` is required** because we need to call `mne.io.read_raw_edf(file_path)` which requires file-level access.

---

## Step-by-Step Instructions

### Step 1 — Open the notebook

1. In Databricks Workspace, navigate to **Repos** > your repo > `notebooks/`
2. Open `day04_silver_preprocessing.py`
3. Attach to the same Unity Catalog cluster from Days 2–3
4. Verify `mne`, `scipy`, and `yasa` are installed:

```python
import mne
import scipy
import yasa
print(f"MNE version: {mne.__version__}")
print(f"SciPy version: {scipy.__version__}")
print(f"YASA version: {yasa.__version__}")
```

Expected output:
```
MNE version: 1.6.1
SciPy version: 1.11.4
YASA version: 0.6.4
```

---

### Step 2 — Run Cell 1: Setup imports and config

```python
import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from src.utils.config import AppConfig
from src.silver.preprocess_eeg import preprocess_eeg, SILVER_EPOCH_SCHEMA
from pyspark.sql import functions as F

cfg = AppConfig()
print("Silver Epochs FQN:", cfg.catalog.silver_epochs_fqn)
```

**What to observe:**
- Silver epochs table: `eeg_lakehouse.silver.cleaned_epochs`
- This table will store **30-second epoch records**, not raw signal samples

---

### Step 3 — Understand the `mapInPandas` decision guide (exam study)

Review this reference table before running any code:

```python
# Pandas UDF vs mapInPandas — Exam Decision Guide

# Method | Access pattern | Best for EEG use case |
# -------|----------------|----------------------|
# @pandas_udf(returnType=scalar) | One column -> one column | Simple per-value transforms (e.g., log(sigma_power)) |
# @pandas_udf(returnType=..., functionType=grouped_agg) | Group -> rows | Per-subject aggregations |
# mapInPandas(func, schema) | Full partition as pd.DataFrame | EDF file reading, TDA computation (needs full epoch as numpy) |
# mapInArrow | Full partition as Arrow RecordBatch | Same as mapInPandas but native Arrow |
```

**Example Pandas UDF for simple scalar transform:**

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np

@pandas_udf("double")
def log_sigma_power(sigma_series: pd.Series) -> pd.Series:
    """Log-transform sigma power (stabilizes variance for ML)."""
    return sigma_series.apply(lambda x: float(np.log1p(x)) if x is not None and x > 0 else None)

# Usage: df.withColumn("log_sigma", log_sigma_power(F.col("sigma_power")))
```

**But for EDF parsing, we use `mapInPandas`:**

```python
# This is what preprocess_eeg() does internally:
psg_df = df_bronze.filter(~F.col("is_hypnogram"))
silver_df = psg_df.mapInPandas(_preprocess_partition, schema=SILVER_EPOCH_SCHEMA)
```

---

### Step 4 — Run Cell 2: Read Bronze EDF files

```python
bronze_df = spark.table(cfg.catalog.bronze_edf_fqn)

# Filter PSG files only (not hypnograms)
psg_df = bronze_df.filter(~F.col("is_hypnogram"))

print(f"Total EDF files in Bronze: {bronze_df.count()}")
print(f"PSG files (for signal processing): {psg_df.count()}")
```

**Expected output (for 4 subjects / 8 files):**
```
Total EDF files in Bronze: 8
PSG files (for signal processing): 4
```

---

### Step 5 — Run Cell 3: Execute Silver preprocessing with `mapInPandas`

```python
from src.silver.preprocess_eeg import preprocess_eeg

# This internally calls psg_df.mapInPandas(_preprocess_partition, schema=SILVER_EPOCH_SCHEMA)
silver_epochs_df = preprocess_eeg(bronze_df, cfg)

print("Silver preprocessing complete.")
silver_epochs_df.printSchema()
```

**What happens internally** (from `src/silver/preprocess_eeg.py`):

1. **`_preprocess_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]`**:
   - Receives an iterator of Pandas DataFrames (one per partition)
   - For each row in the partition:
     - Reads EDF file: `raw = mne.io.read_raw_edf(file_path, preload=True)`
     - Applies bandpass filter (0.5–40 Hz): `raw.filter(l_freq=0.5, h_freq=40.0)`
     - Applies notch filter (50 Hz powerline): `raw.notch_filter(freqs=50.0)`
     - Segments into 30-second epochs
     - Matches with hypnogram annotations to get sleep stage per epoch
     - Computes band powers (delta, theta, sigma) using `scipy.signal.welch`
     - Flags artifacts using z-score > 3 threshold
   - Returns a DataFrame with schema: `SILVER_EPOCH_SCHEMA`

2. **Schema of Silver epochs:**

```python
SILVER_EPOCH_SCHEMA = StructType([
    StructField("subject_id", StringType(), nullable=False),
    StructField("study_night", IntegerType(), nullable=True),
    StructField("epoch_idx", IntegerType(), nullable=False),
    StructField("epoch_start_sec", FloatType(), nullable=False),
    StructField("epoch_end_sec", FloatType(), nullable=False),
    StructField("sleep_stage", StringType(), nullable=True),  # W, N1, N2, N3, REM
    StructField("sleep_stage_source", StringType(), nullable=True),
    StructField("channel_names", ArrayType(StringType()), nullable=True),
    StructField("signal_blob", BinaryType(), nullable=True),  # Compressed numpy array
    StructField("sample_rate_hz", IntegerType(), nullable=False),
    StructField("is_artifact", BooleanType(), nullable=False),
    StructField("sigma_power", FloatType(), nullable=True),  # 11-16 Hz spindle band
    StructField("delta_power", FloatType(), nullable=True),  # 0.5-4 Hz slow wave
    StructField("dataset_source", StringType(), nullable=True),
])
```

**Key fields for exam understanding:**
- `signal_blob`: Stores raw signal as compressed binary (via `numpy.save` + gzip) — allows lazy decompression
- `sigma_power` / `delta_power`: Pre-computed for faster Gold aggregations
- `is_artifact`: Boolean flag for quality filtering in downstream tasks

---

### Step 6 — Run Cell 4: Write Silver table with Delta partitioning

```python
(
    silver_epochs_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("subject_id")  # Critical for query performance
    .saveAsTable(cfg.catalog.silver_epochs_fqn)
)

print(f"Silver epochs written to: {cfg.catalog.silver_epochs_fqn}")
```

**Why partition by `subject_id`?**
1. **Query patterns**: Most Gold aggregations filter by subject (`WHERE subject_id = 'SC4001'`)
2. **Partition pruning**: Delta Lake skips reading irrelevant partitions (faster scans)
3. **Update efficiency**: If re-processing one subject, only that partition is rewritten

**Exam question:** "Should we partition by `epoch_idx` instead?" 
**Answer:** No. `epoch_idx` has high cardinality (1000+ epochs per subject) — would create too many small files. Partition by **low-to-medium cardinality columns** (subject_id ~200 unique values).

---

### Step 7 — Run Cell 5: Inspect Silver table

```python
silver_df = spark.table(cfg.catalog.silver_epochs_fqn)
silver_df.printSchema()

print(f"Total epochs: {silver_df.count()}")
print("\nSleep stage distribution:")
silver_df.groupBy("sleep_stage").count().orderBy("sleep_stage").show()
```

**Expected output:**
```
Total epochs: 2400  # ~600 epochs/subject * 4 subjects

Sleep stage distribution:
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

### Step 8 — Run Cell 6: Inspect band power distributions

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

**Expected output (example):**
```
+----------+---------+----------+---------+------------+------------+
|mean_sigma|std_sigma|mean_delta|std_delta|median_sigma|median_delta|
+----------+---------+----------+---------+------------+------------+
|4.2       |2.1      |12.5      |6.3      |3.8         |11.2        |
+----------+---------+----------+---------+------------+------------+
```

**Why this matters:**
- Sigma power (11–16 Hz) → sleep spindles → memory consolidation marker
- Delta power (0.5–4 Hz) → slow waves → deep sleep (N3) marker
- These features will be used in Gold for TDA and ML models

---

### Step 9 — Run Cell 7: Inspect artifact rate

```python
artifact_rate = (
    silver_df.filter(F.col("is_artifact") == True).count() / silver_df.count()
)
print(f"Artifact rate: {artifact_rate:.1%}")

assert artifact_rate < 0.15, f"FAIL: Artifact rate too high ({artifact_rate:.1%})"
print("✓ PASS: Artifact rate acceptable.")
```

**Expected artifact rate:** 5–10% (z-score > 3 threshold)

**Common causes of high artifact rates:**
- EEG electrodes loose (real data issue)
- Incorrect bandpass filter settings
- Z-score threshold too strict (< 3)

---

### Step 10 — Self-check: Answer exam reflection questions

1. Why use `mapInPandas` for EDF parsing instead of a Pandas UDF?
2. What is the schema of the Silver epochs table, and why is `signal_blob` stored as BinaryType?
3. Why partition Silver by `subject_id` and not `epoch_idx`?
4. What is the difference between `@pandas_udf(returnType=DoubleType())` and `mapInPandas(func, schema)`?
5. What Delta write mode is used for Silver (`overwrite`, `append`, or `merge`)?
6. What bandpass filter range is applied, and why?

**Answers:**

1. `mapInPandas` gives access to entire partition as `pd.DataFrame`, allowing file-level operations like `mne.io.read_raw_edf(file_path)`. Pandas UDFs operate row-by-row or column-by-column, which cannot open EDF files.
2. Schema includes `subject_id`, `epoch_idx`, `sleep_stage`, `sigma_power`, `delta_power`, `signal_blob` (compressed numpy array), `is_artifact`. BinaryType stores compressed signal bytes — allows lazy decompression only when needed (saves memory).
3. `subject_id` has ~200 unique values (optimal partition count). `epoch_idx` has 1000+ values per subject — would create thousands of tiny files (small file problem).
4. `@pandas_udf` operates on Series/DataFrame columns (vectorized). `mapInPandas` operates on entire partition as `pd.DataFrame` — needed for arbitrary Python logic like file I/O.
5. `overwrite` — Silver preprocessing is a full refresh (re-processes all PSG files from Bronze).
6. 0.5–40 Hz: Removes DC drift (< 0.5 Hz) and high-frequency noise (> 40 Hz). Preserves physiological EEG bands: delta (0.5–4 Hz), theta (4–8 Hz), alpha (8–13 Hz), sigma/spindle (11–16 Hz), beta (13–30 Hz).

---

## Day 4 Summary

| What was built | Source |
|---|---|
| Pandas UDF vs `mapInPandas` decision guide | Exam reference |
| Silver EEG epoch preprocessing (30-second windows) | `src/silver/preprocess_eeg.py` |
| MNE-Python EDF parsing with bandpass filtering | `mne.io.read_raw_edf`, `mne.filter` |
| Band power computation (sigma, delta) | `scipy.signal.welch` |
| Artifact detection (z-score > 3) | Statistical thresholding |
| Silver Delta table with `subject_id` partitioning | `eeg_lakehouse.silver.cleaned_epochs` |
| Signal quality inspection (artifact rate, band power distributions) | Spark DataFrame aggregations |

**Next**: Day 5 explores Delta Lake internals (transaction log, time travel, OPTIMIZE, ZORDER, VACUUM).
