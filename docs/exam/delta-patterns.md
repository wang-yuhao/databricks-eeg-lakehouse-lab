# Delta Lake Patterns — Exam Cheat Sheet

> Updated as new patterns are implemented. Domain 1 (Lakehouse) + Domain 3 (Incremental).

---

## 1. Auto Loader vs COPY INTO

| Feature | Auto Loader | COPY INTO |
|---------|-------------|----------|
| Trigger | Streaming (continuous or once) | Batch (SQL statement) |
| File tracking | Checkpoint-based (RocksDB) | Delta transaction log |
| Scale | Billions of files | Millions of files |
| Schema inference | Yes (+ `schemaLocation`) | Yes |
| Format | `format("cloudFiles")` | `COPY INTO table FROM path` |
| Best for | Ongoing incremental ingestion | One-time or infrequent batch loads |

**Exam tip:** Auto Loader = `readStream.format("cloudFiles")`. COPY INTO = SQL DML.

---

## 2. Delta Table Write Modes

```python
df.write.format("delta").mode("append").saveAsTable("table")
# append   -> add rows, never overwrite (Bronze layer)
# overwrite -> replace all data (small reference tables)
# ignore   -> skip if table exists
# error    -> raise error if table exists (default)
```

**Exam pitfall:** `overwrite` + `overwriteSchema=True` replaces BOTH data and schema.
`overwrite` alone fails if new schema doesn't match existing schema.

---

## 3. DESCRIBE Commands

```sql
DESCRIBE TABLE eeg_lakehouse.bronze.raw_eeg_files;
-- Shows: column names, types, comments

DESCRIBE DETAIL eeg_lakehouse.bronze.raw_eeg_files;
-- Shows: numFiles, sizeInBytes, partitionColumns, location, format

DESCRIBE HISTORY eeg_lakehouse.bronze.raw_eeg_files;
-- Shows: version, timestamp, operation, operationParameters, userMetadata
```

---

## 4. Schema Enforcement vs Evolution

```python
# Schema enforcement (default) — rejects rows with extra/missing columns
df.write.format("delta").save("/path")

# Schema evolution — merge new columns into existing schema
df.write.format("delta").option("mergeSchema", "true").save("/path")

# Auto Loader schema evolution
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # default
.option("cloudFiles.schemaEvolutionMode", "rescue")  # saves unparseable data to _rescued_data column
.option("cloudFiles.schemaEvolutionMode", "none")  # strict, reject new columns
```

---

## 5. OPTIMIZE and Z-Ordering

```sql
-- Compact small files into larger ones (reduces file count overhead)
OPTIMIZE eeg_lakehouse.gold.tda_features;

-- Z-Order: co-locate rows with similar values in the same files
-- Best for columns used in WHERE filters and JOIN conditions
OPTIMIZE eeg_lakehouse.gold.tda_features
ZORDER BY (subject_id, spindle_density);

-- Liquid Clustering (Databricks 13.3+) — replaces ZORDER, auto-selects files
ALTER TABLE eeg_lakehouse.gold.tda_features
CLUSTER BY (subject_id, spindle_density);
```

**When to ZORDER vs partition:**
- Partition by: high-cardinality columns used in EVERY query (date, study_night)
- ZORDER by: medium-cardinality columns used in SOME queries (subject_id, sleep_stage)
- Never ZORDER a column with only 2-3 distinct values (no benefit)

---

## 6. Time Travel

```sql
-- By version number
SELECT * FROM eeg_lakehouse.gold.tda_features VERSION AS OF 3;

-- By timestamp
SELECT * FROM eeg_lakehouse.gold.tda_features
TIMESTAMP AS OF '2026-06-01 00:00:00';

-- Python API
df = spark.read.format("delta") \
    .option("versionAsOf", 3) \
    .load("/path/to/table")

-- Restore to previous version (modifies the table)
RESTORE TABLE eeg_lakehouse.gold.tda_features TO VERSION AS OF 2;
```

---

## 7. VACUUM

```sql
-- Remove files no longer referenced by Delta transaction log
-- Default retention: 7 days (168 hours)
VACUUM eeg_lakehouse.gold.tda_features;

-- Custom retention (minimum 7 days unless override enabled)
VACUUM eeg_lakehouse.gold.tda_features RETAIN 336 HOURS;  -- 14 days

-- DRY RUN to preview what would be deleted
VACUUM eeg_lakehouse.gold.tda_features DRY RUN;
```

**Exam pitfall:** After VACUUM, time travel to versions older than retention period is IMPOSSIBLE.
You cannot restore deleted files.

---

## 8. Nested Data Patterns (Silver EEG Events)

```python
from pyspark.sql.types import ArrayType, StructType, StructField, FloatType, StringType
from pyspark.sql import functions as F

# Define nested spindle event schema
spindle_schema = ArrayType(StructType([
    StructField("start_sec",   FloatType()),
    StructField("end_sec",     FloatType()),
    StructField("duration_sec",FloatType()),
    StructField("channel",     StringType()),
    StructField("amplitude_uv",FloatType()),
]))

# Explode array of spindle structs into individual rows
spindle_df = epoch_df.select(
    "subject_id", "epoch_idx",
    F.explode("spindle_events").alias("spindle")
).select(
    "subject_id", "epoch_idx",
    F.col("spindle.start_sec"),
    F.col("spindle.duration_sec"),
    F.col("spindle.channel"),
    F.col("spindle.amplitude_uv"),
)

# Count spindles per subject per epoch
spindle_density = spindle_df.groupBy("subject_id", "epoch_idx") \
    .agg(F.count("*").alias("spindle_count"))
```

---

## 9. Pandas UDF vs Python UDF vs mapInPandas

| API | Serialization | Best for | Exam note |
|-----|--------------|---------|----------|
| Python UDF | Row-by-row pickle | Simple scalar transforms | Slowest, avoid for signal processing |
| Pandas UDF (`@pandas_udf`) | Arrow columnar batch | Vectorized scalar/agg transforms | Best for MNE/YASA per-epoch processing |
| `mapInPandas` | Arrow columnar, full partition | Full DataFrame transformations | Best for TDA (need full epoch as numpy array) |
| `mapInArrow` | Arrow IPC | Same as mapInPandas but native Arrow | Databricks 13+ |

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def compute_sigma_power(signal_series: pd.Series) -> pd.Series:
    """Vectorized sigma-band power computation."""
    import numpy as np
    from scipy.signal import welch
    def _sigma(sig):
        f, psd = welch(np.frombuffer(sig, dtype=np.float32), fs=100, nperseg=256)
        mask = (f >= 11) & (f <= 16)
        return float(np.trapz(psd[mask], f[mask]))
    return signal_series.apply(_sigma)
```
