# Day 7: Delta Live Tables (DLT) — Orchestrating Bronze-Silver-Gold Pipeline

**Notebook**: `notebooks/day07_dlt_pipeline.py`
**Source modules**: All Bronze/Silver/Gold modules
**Exam domains**: Delta Live Tables (Domain 1 — primary focus), Data quality (Domain 1)
**Time estimate**: 3–4 hours
**Prerequisite**: Days 2–6 completed

---

## Objectives

- Understand Delta Live Tables (DLT) architecture
- Convert standalone notebooks into DLT pipeline
- Implement data quality constraints (`expect`, `expect_or_drop`, `expect_or_fail`)
- Configure DLT pipeline settings (target database, storage location)
- Monitor pipeline execution with DLT UI
- Handle schema evolution and auto-recovery

---

## Background

**What is Delta Live Tables?**

Delta Live Tables (DLT) is a **declarative ETL framework** on Databricks that simplifies data pipeline development:

| Feature | Traditional Spark | Delta Live Tables |
|---|---|---|
| Pipeline orchestration | Manual (jobs, workflows) | Automatic (dependency graph) |
| Error handling | Manual try/catch | Auto-retry, quarantine bad data |
| Data quality | Manual assertions | Built-in `expect` constraints |
| Monitoring | Custom logging | Auto-generated lineage UI |
| Schema evolution | Manual ALTER TABLE | Automatic schema inference |

**Key concepts:**

1. **Datasets**: Tables or views defined with `@dlt.table` or `@dlt.view`
2. **Expectations**: Data quality rules (`@dlt.expect("rule_name", "condition")`)
3. **Streaming vs. Batch**: `@dlt.table(spark_conf={"spark.databricks.delta.properties.autoOptimize.optimizeWrite": "true"})`
4. **Materialized views**: DLT auto-manages OPTIMIZE and VACUUM

**Exam tip:**

- `expect("rule", "condition")` → logs violations, keeps data
- `expect_or_drop("rule", "condition")` → drops rows that violate
- `expect_or_fail("rule", "condition")` → fails pipeline if any row violates

---

## Step-by-Step Instructions

### Step 1 — Create DLT notebook

1. Go to Databricks Workspace
2. Create new notebook: `notebooks/day07_dlt_pipeline.py`
3. **Important**: Set language to **Python**
4. Do NOT attach to a cluster (DLT manages its own cluster)

---

### Step 2 — Import DLT library

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
```

**Note**: `dlt` library is only available in DLT pipelines, not interactive notebooks.

---

### Step 3 — Define Bronze layer ingestion

```python
@dlt.table(
    name="bronze_eeg_files",
    comment="Raw EDF files ingested from PhysioNet",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "subject_id"
    }
)
@dlt.expect_or_drop("valid_file_path", "file_path IS NOT NULL")
@dlt.expect("reasonable_file_size", "file_size_mb > 0 AND file_size_mb < 100")
def bronze_eeg_files():
    """
    Ingest EDF files from PhysioNet.
    Equivalent to Day 2 Bronze ingestion.
    """
    return (
        spark.read.format("binaryFile")
        .load("/dbfs/physionet/sleep-edfx/*-PSG.edf")
        .selectExpr(
            "regexp_extract(path, 'SC(\\d+)-.*', 1) AS subject_id",
            "path AS file_path",
            "length / (1024 * 1024) AS file_size_mb",
            "modificationTime AS ingestion_time",
            "content AS file_content"
        )
    )
```

**Key points:**

- `@dlt.table` declares a managed Delta table
- `expect_or_drop` removes rows with NULL `file_path`
- `expect` logs but keeps rows with unreasonable file sizes
- No explicit `write.format("delta")` — DLT handles this

---

### Step 4 — Define Silver layer preprocessing

```python
@dlt.table(
    name="silver_cleaned_epochs",
    comment="Preprocessed 30-second EEG epochs with band power features",
    partition_cols=["subject_id"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect_or_fail("valid_epoch_idx", "epoch_idx >= 0")
@dlt.expect_or_drop("valid_sleep_stage", "sleep_stage IN ('Wake', 'N1', 'N2', 'N3', 'REM')")
@dlt.expect("low_artifact_rate", "is_artifact = false OR artifact_rate < 0.20")
def silver_cleaned_epochs():
    """
    Parse EDF files and compute band power per epoch.
    Equivalent to Day 4 Silver preprocessing.
    """
    from pyspark.sql.functions import pandas_udf, PandasUDFType
    import pandas as pd
    
    # Read Bronze table as streaming source
    bronze_df = dlt.read_stream("bronze_eeg_files")
    
    # Define mapInPandas schema (from Day 4)
    output_schema = StructType([
        StructField("subject_id", StringType()),
        StructField("epoch_idx", IntegerType()),
        StructField("sleep_stage", StringType()),
        StructField("sigma_power", DoubleType()),
        StructField("delta_power", DoubleType()),
        StructField("is_artifact", BooleanType()),
        StructField("signal_blob", BinaryType()),
    ])
    
    def process_edf_partition(iterator):
        """Process EDF files using MNE-Python (from Day 4 logic)."""
        import mne
        import numpy as np
        from scipy import signal
        
        for pdf in iterator:
            # Simplified: In real implementation, parse EDF with mne.io.read_raw_edf
            # For now, return dummy data
            result = pd.DataFrame({
                "subject_id": [pdf["subject_id"].iloc[0]] * 10,
                "epoch_idx": range(10),
                "sleep_stage": ["N2"] * 10,
                "sigma_power": np.random.rand(10) * 5,
                "delta_power": np.random.rand(10) * 15,
                "is_artifact": [False] * 9 + [True],  # 10% artifact
                "signal_blob": [b"\x00"] * 10,
            })
            yield result
    
    return bronze_df.mapInPandas(process_edf_partition, schema=output_schema)
```

**DLT-specific features:**

- `dlt.read_stream("bronze_eeg_files")` reads Bronze as a **streaming** source
- `expect_or_fail` stops pipeline if any `epoch_idx < 0`
- `partition_cols=["subject_id"]` auto-partitions output

---

### Step 5 — Define Gold layer aggregations

```python
@dlt.table(
    name="gold_subject_features",
    comment="Per-subject aggregated features for ML/TDA",
    table_properties={
        "quality": "gold"
    }
)
@dlt.expect_or_fail("valid_subject_count", "total_epochs > 0")
@dlt.expect("percentages_sum_to_100", "ABS((wake_pct + n1_pct + n2_pct + n3_pct + rem_pct) - 100) < 1")
@dlt.expect("acceptable_artifact_rate", "artifact_rate < 0.20")
def gold_subject_features():
    """
    Aggregate Silver epochs into Gold features.
    Equivalent to Day 6 Gold layer.
    """
    silver_df = dlt.read("silver_cleaned_epochs")
    
    # Sleep stage distribution (from Day 6)
    sleep_dist = (
        silver_df
        .groupBy("subject_id")
        .agg(
            F.count("*").alias("total_epochs"),
            (F.expr("count(*) filter(where sleep_stage = 'Wake')") / F.count("*") * 100).alias("wake_pct"),
            (F.expr("count(*) filter(where sleep_stage = 'N1')") / F.count("*") * 100).alias("n1_pct"),
            (F.expr("count(*) filter(where sleep_stage = 'N2')") / F.count("*") * 100).alias("n2_pct"),
            (F.expr("count(*) filter(where sleep_stage = 'N3')") / F.count("*") * 100).alias("n3_pct"),
            (F.expr("count(*) filter(where sleep_stage = 'REM')") / F.count("*") * 100).alias("rem_pct"),
        )
    )
    
    # Band power stats (from Day 6)
    band_power_stats = (
        silver_df
        .filter(F.col("is_artifact") == False)
        .groupBy("subject_id")
        .agg(
            F.mean("sigma_power").alias("sigma_mean"),
            F.stddev("sigma_power").alias("sigma_std"),
            F.mean("delta_power").alias("delta_mean"),
            F.stddev("delta_power").alias("delta_std"),
        )
    )
    
    # Artifact rate (from Day 6)
    artifact_rate = (
        silver_df
        .groupBy("subject_id")
        .agg(
            (F.expr("count(*) filter(where is_artifact = true)") / F.count("*")).alias("artifact_rate"),
        )
    )
    
    # Join all features
    return (
        sleep_dist
        .join(band_power_stats, on="subject_id", how="inner")
        .join(artifact_rate, on="subject_id", how="inner")
    )
```

**Gold layer expectations:**

- `expect_or_fail("valid_subject_count", ...)` ensures no empty subjects
- `expect("percentages_sum_to_100", ...)` logs but allows slight rounding errors

---

### Step 6 — Create DLT pipeline via UI

1. Go to **Workflows** > **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure pipeline:
   - **Pipeline name**: `eeg_lakehouse_pipeline`
   - **Notebook libraries**: Select `notebooks/day07_dlt_pipeline.py`
   - **Target**: `eeg_lakehouse` (database name)
   - **Storage location**: `dbfs:/eeg_lakehouse/dlt_storage`
   - **Cluster mode**: **Enhanced autoscaling** (recommended for production)
   - **Min workers**: 1, **Max workers**: 4
4. Click **Create**

---

### Step 7 — Configure pipeline settings (advanced)

In the pipeline UI, click **Settings** and add:

```json
{
  "configuration": {
    "pipelines.enableTrackHistory": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true"
  }
}
```

**What these do:**

- `enableTrackHistory`: Tracks data lineage for auditing
- `autoOptimize.optimizeWrite`: Automatically compacts small files during write
- `autoOptimize.autoCompact`: Runs OPTIMIZE after every write

---

### Step 8 — Run the pipeline

1. In the DLT pipeline UI, click **Start**
2. Pipeline will:
   - Create a managed cluster
   - Execute Bronze → Silver → Gold in dependency order
   - Apply data quality expectations
   - Write to Delta tables in `eeg_lakehouse` database

**Expected output (in DLT UI):**

```
✓ bronze_eeg_files: 50 rows inserted
✓ silver_cleaned_epochs: 500 rows inserted (10 dropped due to invalid_sleep_stage)
✓ gold_subject_features: 50 rows inserted
```

---

### Step 9 — Monitor data quality violations

1. In DLT UI, click on **silver_cleaned_epochs** table
2. View **Data Quality** tab
3. See metrics:
   - `valid_epoch_idx`: 0 failures (expect_or_fail)
   - `valid_sleep_stage`: 10 rows dropped (expect_or_drop)
   - `low_artifact_rate`: 45 rows passed, 5 rows logged (expect)

**Interpretation:**

- `expect_or_drop` automatically quarantined 10 invalid rows
- `expect` logged 5 rows with high artifact rate but kept them

---

### Step 10 — Inspect pipeline lineage

1. In DLT UI, click **Lineage** tab
2. View DAG (Directed Acyclic Graph):

```
bronze_eeg_files
       ↓
silver_cleaned_epochs
       ↓
gold_subject_features
```

**Use case:**

- Lineage shows data flow from Bronze → Silver → Gold
- Click any table to see schema, row counts, and quality metrics

---

### Step 11 — Trigger incremental update

1. Add new EDF files to `/dbfs/physionet/sleep-edfx/` (simulated)
2. In DLT UI, click **Start** (again)
3. Pipeline will:
   - Detect new Bronze files (streaming source)
   - Process only new data (incremental)
   - Update Silver and Gold tables

**Expected output:**

```
✓ bronze_eeg_files: 5 new rows inserted
✓ silver_cleaned_epochs: 50 new rows inserted
✓ gold_subject_features: 5 rows updated (aggregations recomputed)
```

---

### Step 12 — Handle schema evolution

What if you add a new column to Silver (e.g., `theta_power`)?

1. Update `silver_cleaned_epochs` to add `theta_power: DoubleType()`
2. Re-run pipeline
3. DLT will:
   - Detect schema change
   - Automatically run `ALTER TABLE ... ADD COLUMN theta_power DOUBLE`
   - Backfill existing rows with `NULL`

**No manual schema migration needed!**

---

### Step 13 — Query DLT tables from SQL

```sql
-- Query Gold table
SELECT subject_id, wake_pct, n3_pct, sigma_mean, delta_mean
FROM eeg_lakehouse.gold_subject_features
WHERE artifact_rate < 0.10
ORDER BY n3_pct DESC
LIMIT 10;
```

**Expected output:**

```
+----------+--------+------+----------+----------+
|subject_id|wake_pct|n3_pct|sigma_mean|delta_mean|
+----------+--------+------+----------+----------+
|001       |8.0     |22.0  |4.5       |13.2      |
|015       |10.0    |20.5  |4.1       |12.8      |
...
```

---

### Step 14 — Schedule pipeline for daily runs

1. In DLT UI, click **Schedules**
2. Add schedule:
   - **Trigger type**: **Scheduled**
   - **Cron expression**: `0 2 * * *` (daily at 2 AM)
3. Save

**Production tip:**

- Use **Continuous** mode for real-time streaming
- Use **Triggered** mode for batch processing

---

### Step 15 — Clean up (optional)

To delete the pipeline:

1. In DLT UI, click **Settings** > **Delete Pipeline**
2. Confirm deletion

**Note**: This deletes the pipeline definition, not the Delta tables. Tables remain in `eeg_lakehouse` database.

---

## Self-Check: Answer Exam Reflection Questions

1. What is the difference between `@dlt.table` and `@dlt.view`?
2. When should you use `expect_or_fail` vs. `expect_or_drop`?
3. How does DLT handle streaming sources differently from batch sources?
4. What happens if you add a new column to a DLT table?
5. How do you read a Bronze table from Silver in DLT?
6. What is the benefit of DLT auto-optimization?

**Answers:**

1. `@dlt.table` creates a **materialized** Delta table (persisted to storage). `@dlt.view` creates a **temporary view** (not persisted, recomputed on each query).
2. Use `expect_or_fail` when bad data should **stop the pipeline** (critical violations). Use `expect_or_drop` when bad data should be **quarantined** but pipeline continues.
3. Streaming sources (`dlt.read_stream()`) process new data incrementally. Batch sources (`dlt.read()`) reprocess all data on each run.
4. DLT automatically detects schema changes and runs `ALTER TABLE ADD COLUMN`. Existing rows get NULL values for the new column.
5. Use `dlt.read("bronze_table_name")` for batch or `dlt.read_stream("bronze_table_name")` for streaming.
6. DLT auto-optimization runs OPTIMIZE and VACUUM in the background, eliminating the need for manual file compaction and reducing read latency.

---

## Day 7 Summary

| What was built | DLT feature | Benefit |
|---|---|---|
| Bronze ingestion | `@dlt.table` + `expect_or_drop` | Auto-validates file paths |
| Silver preprocessing | `dlt.read_stream()` + `mapInPandas` | Incremental EDF parsing |
| Gold aggregations | `@dlt.table` + `expect_or_fail` | Enforces quality constraints |
| Data quality monitoring | `expect`, `expect_or_drop`, `expect_or_fail` | Auto-quarantine bad data |
| Pipeline orchestration | DLT dependency graph | Auto-executes in correct order |
| Schema evolution | Auto-ALTER TABLE | No manual migration |
| Auto-optimization | OPTIMIZE + VACUUM | Reduces file overhead |

**Next**: Day 8 explores advanced topics — Topological Data Analysis (TDA) on Gold features and deploying the pipeline to production with CI/CD.
