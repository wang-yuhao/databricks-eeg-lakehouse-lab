# Day 07 – Delta Live Tables (DLT): Declarative Pipelines & Data Quality

## Overview
Delta Live Tables (DLT) is Databricks' declarative ETL framework. Instead of writing
imperative Spark code, you declare **what** your tables should contain; DLT manages
orchestration, retries, lineage, and data quality expectations automatically.

---

## Core Concepts

### 1. DLT Table Types
| Keyword | Type | Description |
|---|---|---|
| `@dlt.table` | Materialized table | Persisted Delta table; incrementally updated |
| `@dlt.view` | View | Virtual; not persisted; good for intermediate transforms |
| `@dlt.table(temporary=True)` | Temp table | Exists only within the pipeline; not visible in catalog |

### 2. Streaming vs Batch in DLT
```python
import dlt
from pyspark.sql import functions as F

# Streaming source (Auto Loader)
@dlt.table(name="bronze_eeg_raw", comment="Raw EEG files from ADLS")
def bronze_eeg_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/mnt/raw/eeg/")
    )

# Batch transformation (Silver)
@dlt.table(name="silver_epochs", comment="Cleaned 30-s EEG epochs")
@dlt.expect_or_drop("valid_duration", "duration_s BETWEEN 25 AND 35")
@dlt.expect_or_drop("no_artefact", "artefact_flag = false")
def silver_epochs():
    return dlt.read_stream("bronze_eeg_raw").select(
        "subject_id", "night_id", "epoch_id",
        F.col("duration_s"),
        F.col("artefact_flag"),
    )
```

### 3. Data Quality Expectations
| Decorator | On Violation |
|---|---|
| `@dlt.expect(name, condition)` | Log warning; keep row |
| `@dlt.expect_or_drop(name, condition)` | Drop non-conforming rows |
| `@dlt.expect_or_fail(name, condition)` | Fail pipeline on any violation |

**Best practice:** Use `expect_or_drop` for data quality gates in Bronze→Silver; use
`expect` for monitoring in Silver→Gold.

### 4. Live Tables vs Standard Tables
| Aspect | Standard Spark | DLT |
|---|---|---|
| Orchestration | Manual (Jobs) | Automatic |
| Retries | Manual | Automatic |
| Lineage | None | Built-in DAG |
| Data quality | DIY checks | Declarative expectations |
| Incremental updates | DIY with `foreachBatch` | Automatic (CDC, append) |

### 5. Change Data Capture (CDC) with DLT
```python
@dlt.table(name="silver_subjects", comment="SCD Type 1 subject metadata")
def silver_subjects():
    return dlt.read_stream("bronze_subjects")

dlt.apply_changes(
    target="silver_subjects",
    source="bronze_subjects_cdc",
    keys=["subject_id"],
    sequence_by=F.col("_commit_timestamp"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["_rescued_data"],
)
```
- `apply_changes` implements SCD Type 1 (upsert) automatically.
- For SCD Type 2 (history), use `stored_as_scd_type="2"`.

### 6. Pipeline Configuration
```json
{
  "name": "eeg-lakehouse-pipeline",
  "clusters": [{"num_workers": 2}],
  "libraries": [{"notebook": {"path": "/notebooks/dlt_eeg_pipeline"}}],
  "continuous": false,
  "development": true,
  "catalog": "eeg_catalog",
  "target": "silver"
}
```
- `continuous: false` = triggered mode (run on schedule).
- `continuous: true` = always-on streaming mode.

---

## EEG / Neuroscience Context

### Why DLT for EEG Pipelines?
- EEG recordings arrive continuously from sleep labs; DLT auto-handles incremental loads.
- Data quality expectations enforce signal validity (amplitude ranges, epoch lengths).
- Built-in lineage tracks which subjects’ data was cleaned and when.
- CDC with `apply_changes` handles subject metadata updates (e.g., diagnosis corrections).

### EEG-Specific Expectations
```python
@dlt.expect_or_drop("valid_amplitude", "max_amp_uv < 500 AND min_amp_uv > -500")
@dlt.expect_or_drop("valid_sample_rate", "sample_rate_hz = 256")
@dlt.expect_or_drop("valid_epoch_length", "n_samples = 7680")  # 30s * 256 Hz
```

---

## Exam-Focused Summary

| Topic | Key Fact |
|---|---|
| DLT table vs view | Tables persisted; views virtual |
| expect vs expect_or_drop | Log vs drop violating rows |
| Streaming source | `spark.readStream` / `dlt.read_stream` |
| CDC | `dlt.apply_changes` with `keys` + `sequence_by` |
| Pipeline modes | `continuous=true` (streaming) vs `false` (triggered) |
| Auto Loader in DLT | `cloudFiles` format, handles new files automatically |

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `src/dlt/eeg_pipeline.py` | DLT pipeline: Bronze→Silver→Gold with expectations |
| `notebooks/day07_dlt_pipeline.py` | Interactive DLT notebook |
| `tests/test_dlt_expectations.py` | Unit tests for quality logic |

---

## Self-Check Questions
1. What is the difference between `@dlt.table` and `@dlt.view`?
2. When should you use `expect_or_fail` versus `expect_or_drop`?
3. How does `dlt.apply_changes` implement SCD Type 1?
4. What is the difference between continuous and triggered pipeline modes?
5. How does Auto Loader integrate with DLT?
6. Why is built-in lineage valuable for clinical EEG data?

---

## Further Reading
- [Databricks DLT Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [DLT Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
- [Apply Changes API (CDC)](https://docs.databricks.com/en/delta-live-tables/cdc.html)
