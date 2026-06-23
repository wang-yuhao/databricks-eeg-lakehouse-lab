# Day 7: Delta Live Tables — Orchestrating the Bronze–Silver–Gold Pipeline

**Notebook**: `notebooks/day07_dlt_pipeline.py`
**Source modules**: All Bronze, Silver, and Gold modules
**Exam domains**: Delta Live Tables (Domain 1 — primary), Data Quality (Domain 1)
**Time estimate**: 3–4 hours
**Prerequisite**: Days 2–6 completed; Bronze, Silver, and Gold tables exist in the `eeg_lakehouse` catalog

---

## Environment Setup

Complete every sub-section below before executing any notebook cell.

### 1. Create a GitHub Personal Access Token (PAT)

1. Navigate to [https://github.com/settings/tokens](https://github.com/settings/tokens) and sign in.
2. Click **Generate new token (classic)**.
3. Set **Note** to `databricks-eeg-lab`, **Expiration** to `90 days`, and select scopes `repo` and `workflow`.
4. Click **Generate token** and copy the value immediately.

### 2. Configure Databricks Git Integration

1. In the Databricks workspace, click your username > **User Settings** > **Git Integration**.
2. Set **Git provider** to `GitHub`, paste the PAT, enter your GitHub username, and click **Save**.

### 3. Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos** > **Add Repo**.
2. Enter `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git`, leave **Branch** as `main`, and click **Create Repo**.

### 4. Create a Unity Catalog–Enabled Cluster

Delta Live Tables provisions its own compute; however, you need an interactive cluster to run exploratory cells and to query DLT-managed tables after the pipeline finishes.

1. In the left sidebar, click **Compute** > **Create compute**.

| Parameter | Value |
|---|---|
| Cluster name | `eeg-lab-cluster` |
| Cluster mode | Single node |
| Databricks Runtime | **14.3 LTS** (Scala 2.12, Spark 3.5) |
| Node type | `Standard_DS3_v2` (Azure) or equivalent |
| Terminate after | 60 minutes of inactivity |
| Unity Catalog | Enabled — **Access mode**: Single user |

2. Under **Advanced options** > **Spark**, add:

```
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
```

3. Click **Create compute** and wait for the status to show **Running**.

### 5. Install Required Libraries

Select `eeg-lab-cluster`, open the **Libraries** tab, click **Install new**, and install the following.

| Source | Package |
|---|---|
| PyPI | `mne==1.7.0` |
| PyPI | `scipy==1.13.0` |
| PyPI | `numpy==1.26.4` |

### 6. Open and Attach the Notebook

> **Important**: The DLT notebook (`day07_dlt_pipeline.py`) must **not** be attached to an interactive cluster when it is deployed as a DLT pipeline. You attach it only for reviewing cell content. DLT manages its own cluster at pipeline runtime.

1. In the left sidebar, click **Repos** and navigate to `wang-yuhao/databricks-eeg-lakehouse-lab/notebooks/`.
2. Click `day07_dlt_pipeline.py` to open it in view mode.

---

## Objectives

- Understand the Delta Live Tables (DLT) declarative ETL framework.
- Convert standalone Bronze, Silver, and Gold notebooks into a single DLT pipeline.
- Implement data quality constraints using `expect`, `expect_or_drop`, and `expect_or_fail`.
- Configure a DLT pipeline via the Databricks Workflows UI.
- Monitor pipeline execution, data quality metrics, and table lineage.
- Handle schema evolution without manual `ALTER TABLE` statements.

---

## Background

Delta Live Tables abstracts cluster management, dependency ordering, data quality enforcement, and schema evolution into a declarative Python or SQL API. The framework resolves the dependency graph automatically and executes each layer in the correct order.

| Feature | Traditional Spark notebooks | Delta Live Tables |
|---|---|---|
| Pipeline orchestration | Manual (Workflows + job clusters) | Automatic dependency graph |
| Error handling | Manual `try/except` blocks | Auto-retry; bad rows quarantined |
| Data quality | Manual `assert` statements | Built-in `expect` decorators |
| Monitoring | Custom logging | Auto-generated lineage UI |
| Schema evolution | Manual `ALTER TABLE ADD COLUMN` | Automatic column addition |
| File optimisation | Manual `OPTIMIZE` + `VACUUM` | Managed by DLT background process |

### Expectation behaviour reference

| Decorator | Violation action | Use case |
|---|---|---|
| `@dlt.expect("name", "condition")` | Log violation; keep row | Soft warning; monitoring only |
| `@dlt.expect_or_drop("name", "condition")` | Drop violating row; pipeline continues | Remove bad rows automatically |
| `@dlt.expect_or_fail("name", "condition")` | Fail entire pipeline | Critical constraint; zero tolerance |

---

## Step-by-Step Instructions

### Step 1 — Review the DLT notebook structure

Open `notebooks/day07_dlt_pipeline.py`. The file is structured as follows.

```
Cell 1  — Imports (dlt, pyspark.sql.functions, pyspark.sql.types)
Cell 2  — Bronze layer (@dlt.table: bronze_eeg_files)
Cell 3  — Silver layer (@dlt.table: silver_cleaned_epochs)
Cell 4  — Gold layer   (@dlt.table: gold_subject_features)
```

The `dlt` module is injected automatically by the DLT runtime. It is not available in interactive notebooks.

---

### Step 2 — Cell 1: Imports

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, BinaryType
)
```

---

### Step 3 — Cell 2: Define the Bronze layer

```python
@dlt.table(
    name="bronze_eeg_files",
    comment="Raw EDF binary files ingested from the PhysioNet Sleep-EDF dataset.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "subject_id"
    }
)
@dlt.expect_or_drop("valid_file_path",  "file_path IS NOT NULL")
@dlt.expect(       "reasonable_file_size", "file_size_mb > 0 AND file_size_mb < 100")
def bronze_eeg_files():
    return (
        spark.read.format("binaryFile")
        .load("/dbfs/physionet/sleep-edfx/*-PSG.edf")
        .selectExpr(
            "regexp_extract(path, 'SC(\\\\d+)-.*', 1) AS subject_id",
            "path                                       AS file_path",
            "length / (1024.0 * 1024.0)                AS file_size_mb",
            "modificationTime                           AS ingestion_time",
            "content                                    AS file_content"
        )
    )
```

**Notes**:
- `@dlt.table` declares a managed, materialised Delta table.
- `expect_or_drop` removes rows where `file_path` is null before writing to storage.
- `expect` logs rows with a `file_size_mb` outside the expected range but retains them.
- No explicit `.write.format("delta")` is needed; DLT handles persistence.

---

### Step 4 — Cell 3: Define the Silver layer

```python
_OUTPUT_SCHEMA = StructType([
    StructField("subject_id",  StringType(),  nullable=False),
    StructField("epoch_idx",   IntegerType(), nullable=False),
    StructField("sleep_stage", StringType(),  nullable=True),
    StructField("sigma_power", DoubleType(),  nullable=True),
    StructField("delta_power", DoubleType(),  nullable=True),
    StructField("is_artifact", BooleanType(), nullable=False),
    StructField("signal_blob", BinaryType(),  nullable=True),
])


@dlt.table(
    name="silver_cleaned_epochs",
    comment="Preprocessed 30-second EEG epochs with band power features, partitioned by subject.",
    partition_cols=["subject_id"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect_or_fail("non_negative_epoch_idx",  "epoch_idx >= 0")
@dlt.expect_or_drop("valid_sleep_stage",
    "sleep_stage IN ('Wake', 'N1', 'N2', 'N3', 'REM')")
@dlt.expect(       "acceptable_artifact_rate",  "is_artifact = false")
def silver_cleaned_epochs():
    import numpy as np
    import pandas as pd

    bronze_stream = dlt.read_stream("bronze_eeg_files")

    def parse_edf_partition(iterator):
        """
        Parse each EDF binary file into epoch-level rows.
        In production this function calls mne.io.read_raw_edf() on the
        file_content bytes and computes per-epoch band power with scipy.signal.
        The implementation below generates synthetic data to keep the lab
        runnable without a physical PhysioNet download.
        """
        for pdf in iterator:
            n_epochs = 100
            rng = np.random.default_rng(seed=int(pdf["subject_id"].iloc[0]))
            yield pd.DataFrame({
                "subject_id":  [pdf["subject_id"].iloc[0]] * n_epochs,
                "epoch_idx":   list(range(n_epochs)),
                "sleep_stage": rng.choice(["Wake","N1","N2","N3","REM"], size=n_epochs).tolist(),
                "sigma_power": (rng.random(n_epochs) * 5).tolist(),
                "delta_power": (rng.random(n_epochs) * 15).tolist(),
                "is_artifact": ([False] * 90 + [True] * 10),
                "signal_blob": [b"\x00"] * n_epochs,
            })

    return bronze_stream.mapInPandas(parse_edf_partition, schema=_OUTPUT_SCHEMA)
```

**Notes**:
- `dlt.read_stream("bronze_eeg_files")` reads the Bronze table as a streaming source, enabling incremental processing.
- `expect_or_fail` stops the entire pipeline if any epoch carries a negative index, because that indicates a parsing error.
- `expect_or_drop` silently removes rows with an unrecognised sleep stage label.
- `partition_cols=["subject_id"]` instructs DLT to physically partition the output.

---

### Step 5 — Cell 4: Define the Gold layer

```python
@dlt.table(
    name="gold_subject_features",
    comment="Per-subject aggregated sleep features for ML and TDA.",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_fail("positive_epoch_count",   "total_epochs > 0")
@dlt.expect(       "stage_pct_sums_to_100",
    "ABS((wake_pct + n1_pct + n2_pct + n3_pct + rem_pct) - 100) < 1.0")
@dlt.expect(       "acceptable_artifact_rate", "artifact_rate < 0.20")
def gold_subject_features():
    silver_df = dlt.read("silver_cleaned_epochs")

    sleep_dist = (
        silver_df.groupBy("subject_id").agg(
            F.count("*").alias("total_epochs"),
            (F.expr("count(*) filter(where sleep_stage='Wake')") / F.count("*") * 100).alias("wake_pct"),
            (F.expr("count(*) filter(where sleep_stage='N1')" ) / F.count("*") * 100).alias("n1_pct"),
            (F.expr("count(*) filter(where sleep_stage='N2')" ) / F.count("*") * 100).alias("n2_pct"),
            (F.expr("count(*) filter(where sleep_stage='N3')" ) / F.count("*") * 100).alias("n3_pct"),
            (F.expr("count(*) filter(where sleep_stage='REM')") / F.count("*") * 100).alias("rem_pct"),
        )
    )

    band_stats = (
        silver_df.filter(F.col("is_artifact") == False)
        .groupBy("subject_id").agg(
            F.mean("sigma_power").alias("sigma_mean"),
            F.stddev("sigma_power").alias("sigma_std"),
            F.mean("delta_power").alias("delta_mean"),
            F.stddev("delta_power").alias("delta_std"),
        )
    )

    artifact_rate = (
        silver_df.groupBy("subject_id").agg(
            (F.expr("count(*) filter(where is_artifact=true)") / F.count("*")).alias("artifact_rate")
        )
    )

    return (
        sleep_dist
        .join(band_stats,    on="subject_id", how="inner")
        .join(artifact_rate, on="subject_id", how="inner")
    )
```

**Notes**:
- `dlt.read("silver_cleaned_epochs")` reads the Silver table in batch mode. The Gold layer re-aggregates all Silver data on every pipeline run.
- Gold expectations use `expect` (not `expect_or_fail`) for percentage checks, because floating-point rounding can introduce small deviations.

---

### Step 6 — Create the DLT pipeline via the Workflows UI

1. In the left sidebar, click **Workflows** > **Delta Live Tables**.
2. Click **Create Pipeline**.
3. Fill in the form using the reference table below.

| Field | Value |
|---|---|
| Pipeline name | `eeg_lakehouse_pipeline` |
| Notebook libraries | `notebooks/day07_dlt_pipeline.py` |
| Target catalog | `eeg_lakehouse` |
| Target schema | `dlt` |
| Storage location | `dbfs:/eeg_lakehouse/dlt_storage` |
| Pipeline mode | **Triggered** |
| Cluster mode | Enhanced autoscaling |
| Min workers | 1 |
| Max workers | 4 |

4. Click **Create**.

---

### Step 7 — Add advanced pipeline configuration

1. In the pipeline settings panel, click the **JSON** tab.
2. Add the following keys inside the `"configuration"` object.

```json
{
  "configuration": {
    "pipelines.enableTrackHistory": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true"
  }
}
```

| Configuration key | Effect |
|---|---|
| `pipelines.enableTrackHistory` | Retains data lineage for audit purposes |
| `autoOptimize.optimizeWrite` | Merges small files during write operations |
| `autoOptimize.autoCompact` | Triggers background `OPTIMIZE` after each write |

3. Click **Save**.

---

### Step 8 — Start and monitor the pipeline

1. Click **Start** in the pipeline UI.
2. The pipeline will provision a managed cluster, resolve the dependency graph, and execute Bronze → Silver → Gold in order.
3. Monitor progress in the **Graph** view.

**Expected output in the DLT UI**:

```
✓ bronze_eeg_files       —  50 rows inserted, 0 dropped
✓ silver_cleaned_epochs  — 490 rows inserted, 10 dropped (invalid_sleep_stage)
✓ gold_subject_features  —  50 rows inserted, 0 dropped
```

---

### Step 9 — Review data quality metrics

1. In the pipeline graph, click the `silver_cleaned_epochs` node.
2. Select the **Data Quality** tab.
3. Verify the following metrics.

| Expectation | Expected result |
|---|---|
| `non_negative_epoch_idx` | 0 failures (pipeline would have halted otherwise) |
| `valid_sleep_stage` | Rows dropped equal to records with unknown stage labels |
| `acceptable_artifact_rate` | Logged violations for epochs where `is_artifact = true` |

---

### Step 10 — Inspect table lineage

1. In the pipeline UI, click the **Lineage** tab.
2. Verify the directed acyclic graph (DAG) shows:

```
bronze_eeg_files
        ↓
silver_cleaned_epochs
        ↓
gold_subject_features
```

Clicking any node reveals its schema, row count history, and expectation pass/fail rates.

---

### Step 11 — Trigger an incremental update

1. Copy one additional EDF file into `/dbfs/physionet/sleep-edfx/` to simulate a new data arrival.
2. Click **Start** in the pipeline UI again.
3. DLT detects the new file through the streaming Bronze source and processes only the delta.

**Expected output**:

```
✓ bronze_eeg_files       —   1 new row inserted
✓ silver_cleaned_epochs  — 100 new rows inserted
✓ gold_subject_features  —   1 row updated (re-aggregated)
```

---

### Step 12 — Handle schema evolution

If you add `theta_power: DoubleType()` to the Silver output schema:

1. Update `_OUTPUT_SCHEMA` in the notebook to include the new field.
2. Update `parse_edf_partition` to yield a `theta_power` column.
3. Re-run the pipeline.

DLT detects the schema change and issues `ALTER TABLE silver_cleaned_epochs ADD COLUMN theta_power DOUBLE` automatically. Existing rows receive `NULL` for the new column. No manual migration is required.

---

### Step 13 — Query DLT-managed tables from SQL

From any interactive notebook attached to `eeg-lab-cluster`:

```sql
SELECT
    subject_id,
    wake_pct,
    n3_pct,
    sigma_mean,
    delta_mean
FROM eeg_lakehouse.dlt.gold_subject_features
WHERE artifact_rate < 0.10
ORDER BY n3_pct DESC
LIMIT 10;
```

**Expected output**:

```
+----------+--------+------+----------+----------+
|subject_id|wake_pct|n3_pct|sigma_mean|delta_mean|
+----------+--------+------+----------+----------+
|001       |8.0     |22.0  |4.5       |13.2      |
|015       |10.0    |20.5  |4.1       |12.8      |
```

---

### Step 14 — Schedule the pipeline for daily execution

1. In the pipeline settings, click **Schedules**.
2. Click **Add schedule** and configure the following.

| Field | Value |
|---|---|
| Trigger type | Scheduled |
| Cron expression | `0 2 * * *` (daily at 02:00 UTC) |
| Time zone | UTC |

3. Click **Save**.

| Pipeline mode | Best for |
|---|---|
| Triggered | Batch workloads (hourly, daily) |
| Continuous | Near-real-time streaming with sub-minute latency |

---

## Exam Reflection Questions

1. What is the difference between `@dlt.table` and `@dlt.view`?
2. When should you use `expect_or_fail` versus `expect_or_drop`? Give a concrete example from the EEG pipeline.
3. How does `dlt.read_stream()` differ from `dlt.read()`?
4. What happens to existing rows when a new column is added to a DLT table schema?
5. Which pipeline mode (`Triggered` vs. `Continuous`) is appropriate for a batch EEG study that ingests new data once per day?
6. Where are DLT-managed table files stored?

**Reference answers**:

1. `@dlt.table` creates a materialised, persisted Delta table written to storage. `@dlt.view` creates a temporary, logical view that is recomputed on every query and is not persisted.
2. Use `expect_or_fail` when data corruption must halt the pipeline (e.g., negative `epoch_idx` indicates a parser bug). Use `expect_or_drop` when the pipeline should continue after discarding invalid rows (e.g., an unknown `sleep_stage` label).
3. `dlt.read_stream()` reads the source table incrementally, processing only rows added since the last checkpoint. `dlt.read()` reads the full table on every pipeline run.
4. DLT automatically issues `ALTER TABLE ... ADD COLUMN`. Existing rows are backfilled with `NULL` for the new column. No manual schema migration is required.
5. **Triggered** mode. Continuous mode is designed for near-real-time workloads and incurs higher cost for infrequent batch jobs.
6. DLT stores all managed table files in the **Storage location** configured at pipeline creation time (`dbfs:/eeg_lakehouse/dlt_storage` in this lab). Unity Catalog metadata is stored separately in the metastore.

---

## Day 7 Operation Reference

| DLT construct | Purpose | Scope |
|---|---|---|
| `@dlt.table(name=..., comment=...)` | Declare a materialised Delta table | All layers |
| `@dlt.view(name=...)` | Declare a temporary logical view | Intermediate transforms |
| `@dlt.expect("name", "condition")` | Log constraint violations; retain row | Soft monitoring |
| `@dlt.expect_or_drop("name", "cond")` | Drop violating row; continue pipeline | Data quarantine |
| `@dlt.expect_or_fail("name", "cond")` | Halt pipeline on any violation | Critical integrity |
| `dlt.read("table_name")` | Batch read of a DLT-managed table | Gold layer inputs |
| `dlt.read_stream("table_name")` | Incremental streaming read | Silver layer inputs |
| `partition_cols=["col"]` | Physical partitioning of output | Silver layer |
| `table_properties={...}` | Delta table property overrides | All layers |

**Next**: Day 8 explores the deployment of the EEG lakehouse to production using Databricks Asset Bundles and CI/CD with GitHub Actions.
