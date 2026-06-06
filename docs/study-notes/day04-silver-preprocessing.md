# Day 4 — Silver Preprocessing (EEG Cleaning) in PySpark

**Exam Domain:** Pandas UDFs, Arrow-based vectorized UDFs, Silver transformation patterns 
**Pipeline Layer:** Silver 
**Session Time:** ~2-3 hours

---

## Learning Objectives

- Implement Pandas UDFs (`@pandas_udf`) for EEG bandpass filtering
- Understand the difference between scalar UDFs, Pandas UDFs, and `mapInPandas`
- Design Silver tables with proper schema enforcement and null handling
- Articulate when to use each UDF type in a Spark context

---

## Core Concepts

### 1. UDF Type Comparison

| UDF Type | Decorator | Input | Output | Performance | Use case |
|----------|-----------|-------|--------|-------------|----------|
| Python UDF | `@udf` | Single row | Single value | Slowest (no vectorization) | Simple logic |
| Pandas Scalar UDF | `@pandas_udf(returnType, PandasUDFType.SCALAR)` | `pd.Series` | `pd.Series` | Fast (Arrow) | Column transforms |
| Pandas Grouped Map | `@pandas_udf` + `applyInPandas` | `pd.DataFrame` | `pd.DataFrame` | Fast | Group-level transforms |
| `mapInPandas` | N/A | Iterator[`pd.DataFrame`] | Iterator[`pd.DataFrame`] | Fast, streaming | Whole-partition transforms |

### 2. Pandas Scalar UDF Pattern

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType
import pandas as pd
import numpy as np

@pandas_udf(ArrayType(DoubleType()))
def bandpass_filter_udf(signal_series: pd.Series) -> pd.Series:
    """Apply a 0.5-40 Hz bandpass filter to each EEG epoch.
    
    In production: uses scipy.signal.butter + filtfilt.
    In this mock: returns normalised random signal for CI compatibility.
    """
    def filter_single(signal_list: list) -> list:
        if signal_list is None:
            return None
        arr = np.array(signal_list, dtype=float)
        # Production: arr = bandpass(arr, 0.5, 40, fs=100)
        # Mock: normalise to [0, 1] range
        if arr.std() > 0:
            arr = (arr - arr.mean()) / arr.std()
        return arr.tolist()
    
    return signal_series.apply(filter_single)
```

### 3. Silver Table Design for EEG

```python
SILVER_EEG_SCHEMA = StructType([
    StructField("subject_id",        StringType(),    nullable=False),
    StructField("study_night",        IntegerType(),   nullable=False),
    StructField("epoch_index",        IntegerType(),   nullable=False),  # 30-second epoch
    StructField("sleep_stage",        StringType(),    nullable=True),   # W/N1/N2/N3/R
    StructField("epoch_start_sec",    DoubleType(),    nullable=False),
    StructField("channel_name",       StringType(),    nullable=False),
    StructField("raw_signal",         ArrayType(DoubleType()), nullable=True),
    StructField("filtered_signal",    ArrayType(DoubleType()), nullable=True),
    StructField("is_artifact",        BooleanType(),   nullable=False),
    StructField("artifact_reason",    StringType(),    nullable=True),
    StructField("processing_version", StringType(),    nullable=False),
])
```

### 4. Silver Transformation Pipeline

```python
def preprocess_bronze_to_silver(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
) -> None:
    """Bronze -> Silver: filter, validate, enrich."""
    bronze_df = spark.table(bronze_table)
    
    silver_df = (
        bronze_df
        .filter(col("is_hypnogram") == False)  # PSG files only
        .filter(col("subject_id").isNotNull())
        # Apply UDF: simulate signal loading + filtering
        .withColumn("filtered_signal", bandpass_filter_udf(col("raw_signal")))
        .withColumn("is_artifact", detect_artifact_udf(col("filtered_signal")))
        .withColumn("processing_version", lit("v1.0.0"))
    )
    
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("subject_id", "study_night") \
        .saveAsTable(silver_table)
```

### 5. Null Handling Patterns

```python
# Filter out nulls before UDF
df.filter(col("raw_signal").isNotNull())

# Fill nulls
df.fillna({"sleep_stage": "W", "is_artifact": False})

# Null-safe comparison
df.filter(col("sleep_stage").eqNullSafe("N2"))

# Count nulls per column
from pyspark.sql.functions import count, when, isnan
df.select([
    count(when(col(c).isNull(), c)).alias(f"{c}_nulls")
    for c in df.columns
])
```

---

## Exam Focus Areas

### Pandas UDF Registration and Type Hints

```python
# Modern API (Spark 3.0+): use type hints, no PandasUDFType needed
@pandas_udf(DoubleType())
def compute_rms(signal: pd.Series) -> pd.Series:
    """Root mean square of each signal array."""
    return signal.apply(lambda x: np.sqrt(np.mean(np.array(x)**2)) if x else None)

# Register for SQL use
spark.udf.register("compute_rms", compute_rms)
```

### When to Use Each UDF Type

```
Simple string/numeric transform        → @pandas_udf (scalar)
Aggregate over a group (one row out)   → applyInPandas or GroupedData.applyInPandas
Transform whole partition              → mapInPandas
Access cluster-local files per row     → mapInPandas (can open files per partition)
Call external API per row              → @udf (but batch with foreach if possible)
```

### Nested Data Types

```python
# Create a StructType column
df.withColumn(
    "signal_stats",
    struct(
        col("filtered_signal").alias("signal"),
        compute_rms(col("filtered_signal")).alias("rms"),
        size(col("filtered_signal")).alias("length"),
    )
)

# Explode array to rows
df.withColumn("sample", explode(col("filtered_signal")))

# Access nested field
df.select(col("signal_stats.rms"))
```

---

## Research Context

The Silver preprocessing layer implements the following EEG signal processing steps:

1. **Band-pass filter (0.5–40 Hz):** Removes DC drift and high-frequency muscle artefacts
   - Low cut: 0.5 Hz removes slow EEG drift
   - High cut: 40 Hz is sufficient for sleep spindles (12–15 Hz) and slow oscillations (0.5–1 Hz)
   - Notch filter at 50 Hz: European power line interference removal
   
2. **Artefact rejection (peak-to-peak > 150 µV):** Standard threshold in AASM guidelines
   - Epochs exceeding this threshold are flagged, not deleted
   - Flagged epochs are excluded from Silver→Gold feature computation
   
3. **Epoch segmentation (30-second windows):** AASM standard
   - 30 s at 100 Hz = 3000 samples per epoch per channel
   - Each epoch has exactly one sleep stage label from the hypnogram

**Mock implementation note:** Because MNE-Python (the production EEG processing library) is not installed in CI to keep build times short, the Silver UDFs use NumPy mock operations. See `requirements.txt` comments for how to activate real MNE processing.

---

## Key Files Created Today

| File | Purpose |
|------|---------|
| `src/silver/preprocess_eeg.py` | `bandpass_filter_udf`, `detect_artifact_udf`, `preprocess_bronze_to_silver()` |
| `notebooks/day04_silver_preprocessing.py` | Load Bronze, apply UDFs, write Silver, validate schema |
| `tests/test_silver.py` | Schema validation, null flagging, output column existence |

---

## Self-Check Questions

1. What is the difference between `@pandas_udf` and `mapInPandas`?
2. Why are Pandas UDFs faster than regular Python UDFs?
3. How does `partitionBy` on write affect downstream query performance?
4. When would you flag an epoch as an artefact rather than delete it?
5. What does `eqNullSafe` do that `==` does not?
6. How do you access a nested struct field in Spark SQL vs PySpark DSL?

---

## Further Reading

- [Pandas UDFs documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html)
- [MNE-Python EEG preprocessing](https://mne.tools/stable/auto_tutorials/preprocessing/)
- [AASM sleep scoring manual](https://aasm.org/clinical-resources/scoring-manual/)
