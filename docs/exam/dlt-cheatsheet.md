# DLT Cheatsheet — Databricks Data Engineer 2026 Exam

> **Exam weight**: ~20% of questions touch DLT / production pipelines
> **Repo example**: `resources/eeg_dlt_pipeline.yml` + DLT decorators in `src/`

---

## 1. What is Delta Live Tables (DLT)?

DLT is a **declarative ETL framework** built on Apache Spark that:
- Automatically manages dependencies between tables
- Enforces data quality via **Expectations**
- Handles retries, checkpointing, and incremental processing
- Runs as a managed **Pipeline** (not a standard Databricks Job)

```
Notebook/Python file  ->  DLT Pipeline  ->  Delta Tables (Bronze/Silver/Gold)
```

---

## 2. Core Decorators

| Decorator | Purpose | Creates |
|-----------|---------|--------|
| `@dlt.table` | Define a materialized Delta table | Managed Delta table |
| `@dlt.view` | Define a temp view (not persisted) | In-memory view |
| `@dlt.expect(name, condition)` | Warn if violated; rows kept | Data quality metric |
| `@dlt.expect_or_drop(name, cond)` | Drop violating rows | Filtered table |
| `@dlt.expect_or_fail(name, cond)` | Fail pipeline if violated | Hard constraint |

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_eeg_raw",
    comment="Raw EDF binary files ingested via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_eeg_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load("/Volumes/eeg_lakehouse/bronze/raw_edf/")
    )
```

---

## 3. Reading from DLT Tables

```python
# Read ANOTHER DLT table (within same pipeline)
df = dlt.read("bronze_eeg_raw")          # batch
df = dlt.read_stream("bronze_eeg_raw")   # streaming

# NEVER use spark.table() inside DLT — it bypasses lineage tracking
```

**Exam trap**: `dlt.read()` vs `dlt.read_stream()` — use `read_stream` when
the source table is a streaming table; use `read` for materialized views.

---

## 4. Expectations in Detail

```python
@dlt.table
@dlt.expect("valid_subject", "subject_id IS NOT NULL")
@dlt.expect_or_drop("valid_amplitude", "amplitude_uv BETWEEN -500 AND 500")
@dlt.expect_or_fail("valid_session", "session IN ('night1', 'night2', 'nap')")
def silver_eeg_preprocessed():
    return dlt.read_stream("bronze_eeg_raw").select(...)
```

| Mode | Row behavior | Pipeline behavior |
|------|-------------|-------------------|
| `expect` | Keep row | Log warning |
| `expect_or_drop` | Drop row | Continue |
| `expect_or_fail` | Drop row | **Fail pipeline** |

---

## 5. Pipeline Modes

| Mode | Description | Use case |
|------|-------------|----------|
| **Triggered** | Run once, then stop | Batch / scheduled |
| **Continuous** | Run permanently | Near-real-time |
| **Development** | No retries, fast feedback | Local debugging |
| **Production** | Auto retries, alerts | Live pipelines |

```yaml
# In databricks.yml / pipeline config
pipelines:
  - name: eeg_dlt_pipeline
    development: false     # true for dev mode
    continuous: false      # true for streaming-always-on
```

---

## 6. Pipeline Configuration (YAML / UI)

```yaml
pipelines:
  - name: eeg_bronze_to_gold
    target: eeg_lakehouse          # Unity Catalog or Hive metastore
    libraries:
      - notebook: /Repos/wang-yuhao/databricks-eeg-lakehouse-lab/src/bronze/dlt_bronze
      - notebook: /Repos/wang-yuhao/databricks-eeg-lakehouse-lab/src/silver/dlt_silver
      - notebook: /Repos/wang-yuhao/databricks-eeg-lakehouse-lab/src/gold/dlt_gold
    configuration:
      eeg.sampling_rate: "256"
      eeg.catalog: "eeg_lakehouse"
    clusters:
      - label: default
        autoscale:
          min_workers: 1
          max_workers: 4
```

---

## 7. DLT vs. Standard Spark Jobs

| Feature | DLT | Spark Job |
|---------|-----|-----------|
| Dependency graph | Automatic | Manual |
| Incremental by default | Yes | Manual with watermarks |
| Data quality | Built-in Expectations | Manual assert/filter |
| Retries | Configurable | Manual |
| Observability | Pipeline UI + event log | Manual logging |
| Bronze→Gold in one run | Yes | Orchestrate separately |

---

## 8. EEG Pipeline DLT Flow

```
bronze_eeg_raw          (Auto Loader cloudFiles)
    |  @dlt.expect_or_drop("valid_subject")
    v
silver_eeg_preprocessed (Pandas UDF bandpass, epoch segmentation)
    |  @dlt.expect("valid_amplitude")
    v
silver_eeg_events       (spindle + SO detection)
    |  @dlt.expect_or_drop("valid_event_type")
    v
gold_eeg_features       (join + aggregate features)
```

---

## 9. Key Exam Questions

**Q: What decorator keeps violating rows but records the metric?**
A: `@dlt.expect()` — rows kept, violation logged as metric in pipeline event log.

**Q: What is the difference between a DLT table and a DLT view?**
A: Tables are materialized to Delta files; views are transient and only exist during pipeline execution.

**Q: Can you use `spark.table()` inside a DLT notebook?**
A: No — you must use `dlt.read()` or `dlt.read_stream()` to maintain lineage.

**Q: What pipeline mode runs indefinitely for streaming?**
A: Continuous mode.

**Q: What does `expect_or_fail` do to the pipeline?**
A: Stops the entire pipeline run if any row violates the constraint.

---

## 10. EEG Research Notes

- **Bronze DLT table**: raw EDF binary blobs + metadata (Auto Loader cloudFiles)
- **Silver DLT table**: cleaned epochs (Pandas UDF), `expect_or_drop` on amplitude bounds
- **Silver events**: spindle/SO detections, `expect_or_drop` on event_type and duration
- **Gold features**: aggregated metrics, `expect_or_fail` on subject_id not null
- Expectations metrics are queried from `event_log()` table for QC dashboards

```python
# Query DLT event log for quality metrics
df_log = spark.sql("""
    SELECT
        timestamp, details:flow_progress.metrics.num_output_rows,
        details:flow_progress.data_quality.dropped_records
    FROM event_log('eeg_lakehouse.bronze.eeg_raw')
    ORDER BY timestamp DESC
    LIMIT 20
""")
```
