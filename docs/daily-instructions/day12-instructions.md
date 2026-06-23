# Day 12: Delta Live Tables — Advanced Features, CDC, and Pipeline Event Log Analysis

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day12_dlt_advanced.py` |
| **Exam domains** | Domain 4 — Delta Live Tables; Domain 3 — Incremental Data Processing |
| **Time estimate** | 5–6 hours |
| **Prerequisite** | Days 1–11 completed; Bronze and Silver streaming tables exist; Unity Catalog configured |

---

## Section 1: Environment Setup

Complete every step in this section before opening any notebook cell. A reader starting from a blank Databricks workspace must follow these steps in order.

### 1.1 Create a GitHub Personal Access Token

1. Sign in to [github.com](https://github.com).
2. Navigate to **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. Click **Generate new token (classic)**.
4. Set the following fields:

   | Field | Value |
   |---|---|
   | Note | `databricks-eeg-lab` |
   | Expiration | 90 days |
   | Scopes | `repo` (full), `workflow` |

5. Click **Generate token**, copy it immediately, and store it in a password manager. It will not be shown again.

### 1.2 Configure Databricks Git Integration

1. Click your username in the top-right corner of the workspace and select **User Settings**.
2. Select the **Git integration** tab.
3. Enter the following:

   | Field | Value |
   |---|---|
   | Git provider | GitHub |
   | Git provider username | Your GitHub username |
   | Personal access token | Token from step 1.1 |

4. Click **Save**.

### 1.3 Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos**.
2. Click **Add repo**.
3. Enter the following:

   | Field | Value |
   |---|---|
   | Git repository URL | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab` |
   | Git provider | GitHub |
   | Repo name | `databricks-eeg-lakehouse-lab` |

4. Click **Create repo**. Databricks clones the repository and makes it available under `/Repos/<your-username>/databricks-eeg-lakehouse-lab`.

### 1.4 Create a Cluster with Unity Catalog Enabled

1. In the left sidebar, click **Compute**.
2. Click **Create compute**.
3. Apply the following configuration:

   | Setting | Value |
   |---|---|
   | Cluster name | `eeg-lab-day12` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS** |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled (Single user access mode) |

4. Click **Create compute** and wait for the cluster to reach the **Running** state.

> **Note**: DLT pipelines provision their own cluster automatically. The cluster created above is used only for the event-log analysis and CDC validation cells in this notebook.

### 1.5 Install Required Libraries

All required packages are included in DBR 14.3 LTS. No additional installation is needed for this day.

Verify availability:

```python
# Cell 1: verify required imports
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
from delta.tables import DeltaTable
print("All required modules imported successfully.")
print(f"Spark version: {spark.version}")
```

### 1.6 Open and Attach the Notebook

1. In the left sidebar, click **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Click `day12_dlt_advanced.py` to open it.
3. Click **Connect** in the top-right and select `eeg-lab-day12`.
4. Confirm the cluster name appears in the toolbar before executing any cell.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Define multi-hop DLT pipelines with Bronze → Silver → Gold dependencies | Domain 4 — Delta Live Tables |
| Apply `APPLY CHANGES INTO` for CDC (Change Data Capture) | Domain 3 — Incremental Data Processing |
| Use `dlt.read_stream()` vs `dlt.read()` appropriately | Domain 4 — DLT table types |
| Configure pipeline settings: `target`, `continuous` mode, and `development` mode | Domain 4 — DLT pipeline configuration |
| Query and analyse the DLT event log for pipeline health | Domain 4 — DLT monitoring |
| Define parameterised pipelines using `spark.conf.get()` | Domain 4 — DLT configuration |

---

## Section 3: Background

### DLT Table Types

| Type | Decorator | Storage | Exam relevance |
|---|---|---|---|
| Streaming Live Table | `@dlt.table` on a streaming source | Delta; append-only by default | Auto Loader and streaming sources |
| Materialized view | `@dlt.table` on a batch source | Delta; refreshed on each update | Aggregations and joins on static data |
| Live view | `@dlt.view` | No physical storage; computed at query time | Intermediate transformations without storage cost |

### DLT Execution Modes

| Mode | Trigger | Cluster lifecycle | Typical use |
|---|---|---|---|
| Triggered | Pipeline runs on demand or on schedule | Cluster starts and stops per run | Batch-style scheduled ETL |
| Continuous | Pipeline runs indefinitely; processes new data as it arrives | Cluster stays running | Low-latency streaming pipelines |
| Development | Same as Triggered but skips retries and reuses cluster | Shared development cluster | Interactive development and debugging |

### APPLY CHANGES INTO — Key Behaviour

| Parameter | Description | Exam consideration |
|---|---|---|
| `target` | Destination Live Table name | Must reference a table defined by `dlt.create_target_table()` |
| `source` | Source streaming table | Must be a streaming source |
| `keys` | Primary key columns | Identifies rows for upsert vs. delete |
| `sequence_by` | Ordering column (timestamp or monotonic integer) | Resolves out-of-order CDC events; higher value wins |
| `apply_as_deletes` | Boolean condition for delete events | Rows matching the condition are deleted from the target |
| `stored_as_scd_type` | `1` or `2` | SCD Type 1 overwrites; SCD Type 2 retains history with `__start_at` / `__end_at` columns |

---

## Section 4: Part 1 — Multi-Hop DLT Pipeline Definition

### Step 1 — Create the DLT Notebook File

The following code constitutes the full content of `notebooks/day12_dlt_advanced.py`. Every `@dlt.table` and `@dlt.view` function must reside in a notebook that is registered as a library in the DLT pipeline settings.

```python
# ============================================================
# Day 12 — DLT Advanced Pipeline
# File: notebooks/day12_dlt_advanced.py
# ============================================================
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType,
)

# Pipeline-level configuration values (set via DLT pipeline settings → Configuration)
LANDING_PATH   = spark.conf.get("eeg.landing_path",   "/mnt/eeg_streaming/landing")
CATALOG        = spark.conf.get("eeg.catalog",         "eeg_lakehouse")
SILVER_SCHEMA  = spark.conf.get("eeg.silver_schema",   "silver")
GOLD_SCHEMA    = spark.conf.get("eeg.gold_schema",     "gold")

BRONZE_SCHEMA_DEF = StructType([
    StructField("recording_id",     StringType(),  nullable=False),
    StructField("patient_id",       StringType(),  nullable=False),
    StructField("event_time",       StringType(),  nullable=False),
    StructField("num_channels",     IntegerType(), nullable=True),
    StructField("sampling_rate",    IntegerType(), nullable=True),
    StructField("duration_seconds", DoubleType(),  nullable=True),
    StructField("mean_amplitude",   DoubleType(),  nullable=True),
    StructField("std_amplitude",    DoubleType(),  nullable=True),
    StructField("delta_power",      DoubleType(),  nullable=True),
    StructField("theta_power",      DoubleType(),  nullable=True),
    StructField("alpha_power",      DoubleType(),  nullable=True),
    StructField("beta_power",       DoubleType(),  nullable=True),
    StructField("gamma_power",      DoubleType(),  nullable=True),
    StructField("signal_quality",   DoubleType(),  nullable=True),
    StructField("has_seizure",      IntegerType(), nullable=True),
    StructField("file_format",      StringType(),  nullable=True),
    StructField("_change_type",     StringType(),  nullable=True),  # CDC field
    StructField("_commit_timestamp",StringType(),  nullable=True),  # CDC ordering field
])
```

---

### Step 2 — Define the Bronze Streaming Live Table

```python
# Bronze: raw ingestion from landing zone via Auto Loader
@dlt.table(
    name="eeg_bronze_advanced",
    comment="Raw EEG recordings from landing zone. Schema-enforced; no quality filtering applied.",
    table_properties={
        "quality":               "bronze",
        "pipelines.reset.allowed": "true",
    },
)
@dlt.expect_or_drop("non_null_recording_id", "recording_id IS NOT NULL")
@dlt.expect_or_drop("non_null_patient_id",   "patient_id IS NOT NULL")
@dlt.expect_or_fail("valid_file_format",     "file_format IN ('edf', 'fif', 'set')")
def bronze_eeg_advanced():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",          "json")
        .option("cloudFiles.schemaLocation",  "/mnt/checkpoints/schema/day12_bronze")
        .option("cloudFiles.inferColumnTypes","true")
        .schema(BRONZE_SCHEMA_DEF)
        .load(LANDING_PATH)
        .withColumn("event_timestamp",      F.to_timestamp("event_time"))
        .withColumn("ingestion_timestamp",  F.current_timestamp())
        .withColumn("source_file",          F.col("_metadata.file_path"))
        .withColumn("ingestion_date",       F.to_date(F.current_timestamp()))
        .drop("event_time")
    )
```

---

### Step 3 — Define an Intermediate Live View

```python
# Live view: intermediate cleaning — no physical storage allocated
@dlt.view(
    name="eeg_cleaned_view",
    comment="Intermediate view applying column-level cleaning rules before Silver materialisation.",
)
def eeg_cleaned_view():
    return (
        dlt.read_stream("eeg_bronze_advanced")
        .withColumn(
            "signal_quality",
            F.when(F.col("signal_quality").isNull(), F.lit(0.0))
             .when(F.col("signal_quality") > 1.0,    F.lit(1.0))
             .when(F.col("signal_quality") < 0.0,    F.lit(0.0))
             .otherwise(F.col("signal_quality")),
        )
        .withColumn(
            "theta_alpha_ratio",
            F.col("theta_power") / (F.col("alpha_power") + F.lit(1e-6)),
        )
        .withColumn(
            "delta_theta_ratio",
            F.col("delta_power") / (F.col("theta_power") + F.lit(1e-6)),
        )
    )
```

---

### Step 4 — Define the Silver Streaming Live Table

```python
# Silver: validated, cleaned, deduplicated EEG recordings
@dlt.table(
    name="eeg_silver_advanced",
    comment="Silver EEG recordings: validated, cleaned, and enriched with computed band ratios.",
    partition_cols=["ingestion_date"],
    table_properties={
        "quality":                "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)
@dlt.expect_or_drop("valid_channel_count",  "num_channels > 0 AND num_channels <= 256")
@dlt.expect_or_drop("valid_sampling_rate",  "sampling_rate >= 100 AND sampling_rate <= 10000")
@dlt.expect_or_drop("valid_duration",       "duration_seconds > 0 AND duration_seconds <= 86400")
@dlt.expect(        "good_signal_quality",  "signal_quality >= 0.5")
def silver_eeg_advanced():
    return (
        dlt.read_stream("eeg_cleaned_view")
        .select(
            "recording_id",
            "patient_id",
            "event_timestamp",
            "num_channels",
            "sampling_rate",
            "duration_seconds",
            "mean_amplitude",
            "std_amplitude",
            "delta_power",
            "theta_power",
            "alpha_power",
            "beta_power",
            "gamma_power",
            "theta_alpha_ratio",
            "delta_theta_ratio",
            "signal_quality",
            "has_seizure",
            "source_file",
            "ingestion_date",
            F.current_timestamp().alias("processed_timestamp"),
        )
    )
```

---

### Step 5 — Define the Gold Batch Materialized View

```python
# Gold: per-patient aggregated features (batch materialised view, refreshed on each pipeline update)
@dlt.table(
    name="eeg_gold_patient_summary",
    comment="Gold layer: per-patient EEG recording summary statistics, refreshed on each pipeline update.",
    table_properties={
        "quality": "gold",
    },
)
def gold_patient_summary():
    return (
        dlt.read("eeg_silver_advanced")       # batch read — reads the current snapshot
        .groupBy("patient_id")
        .agg(
            F.count("recording_id").alias("total_recordings"),
            F.avg("signal_quality").alias("avg_signal_quality"),
            F.avg("mean_amplitude").alias("avg_mean_amplitude"),
            F.avg("theta_alpha_ratio").alias("avg_theta_alpha_ratio"),
            F.avg("delta_theta_ratio").alias("avg_delta_theta_ratio"),
            F.sum(F.when(F.col("has_seizure") == 1, 1).otherwise(0)).alias("seizure_count"),
            F.max("processed_timestamp").alias("last_processed"),
        )
    )
```

---

## Section 5: Part 2 — CDC with APPLY CHANGES INTO

### Step 6 — Create the CDC Target Table Definition

```python
# CDC target table: must be created with dlt.create_target_table() before referencing it
dlt.create_target_table(
    name="eeg_patient_scd2",
    comment="SCD Type 2 patient dimension maintained via CDC from the source Bronze table.",
    schema=StructType([
        StructField("patient_id",        StringType(),  nullable=False),
        StructField("recording_id",      StringType(),  nullable=True),
        StructField("signal_quality",    DoubleType(),  nullable=True),
        StructField("has_seizure",       IntegerType(), nullable=True),
        StructField("ingestion_date",    StringType(),  nullable=True),
        StructField("__start_at",        TimestampType(),nullable=True),
        StructField("__end_at",          TimestampType(),nullable=True),
    ]),
    table_properties={"quality": "silver"},
)
```

### Step 7 — Apply CDC Changes

```python
# APPLY CHANGES INTO: consume the CDC stream and maintain SCD Type 2 history
dlt.apply_changes(
    target         = "eeg_patient_scd2",
    source         = "eeg_bronze_advanced",
    keys           = ["patient_id"],
    sequence_by    = F.col("_commit_timestamp").cast("timestamp"),
    apply_as_deletes = F.col("_change_type") == "DELETE",
    stored_as_scd_type = 2,
    column_list    = ["recording_id", "signal_quality", "has_seizure", "ingestion_date"],
)
```

> **Exam note**: `apply_changes` must reference a target created via `dlt.create_target_table()`. The `sequence_by` column determines which CDC event wins when two events share the same key. Higher values (later timestamps) take precedence.

---

## Section 6: Part 3 — DLT Pipeline Configuration

### Step 8 — Create the DLT Pipeline via the UI

1. In the left sidebar, click **Workflows** → **Delta Live Tables**.
2. Click **Create pipeline**.
3. Apply the following settings:

   | Field | Value |
   |---|---|
   | Pipeline name | `eeg_lakehouse_day12` |
   | Product edition | Advanced (required for `APPLY CHANGES INTO`) |
   | Pipeline mode | Triggered |
   | Notebook libraries | `/Repos/<your-username>/databricks-eeg-lakehouse-lab/notebooks/day12_dlt_advanced.py` |
   | Target catalog | `eeg_lakehouse` |
   | Target schema | `silver` |
   | Storage location | `abfss://eeg-lakehouse@<storage-account>.dfs.core.windows.net/dlt/day12` |
   | Development mode | Enabled |

4. Under **Configuration**, add the following key-value pairs:

   | Key | Value |
   |---|---|
   | `eeg.landing_path` | `/mnt/eeg_streaming/landing` |
   | `eeg.catalog` | `eeg_lakehouse` |
   | `eeg.silver_schema` | `silver` |
   | `eeg.gold_schema` | `gold` |

5. Click **Create**.

> Replace `<storage-account>` with your Azure Data Lake Storage account name. On AWS, use an `s3://` URI.

### Step 9 — Start the Pipeline

1. On the pipeline detail page, click **Start** (Triggered mode runs once and stops).
2. Observe the pipeline DAG as it renders. Verify the following nodes are present:

   | Node | Type | Expected colour |
   |---|---|---|
   | `eeg_bronze_advanced` | Streaming Live Table | Blue |
   | `eeg_cleaned_view` | Live View | Grey |
   | `eeg_silver_advanced` | Streaming Live Table | Blue |
   | `eeg_gold_patient_summary` | Materialized View | Green |
   | `eeg_patient_scd2` | CDC Target | Purple |

3. Wait for the status to show **Completed** before proceeding to the event log analysis.

---

## Section 7: Part 4 — DLT Event Log Analysis

### Step 10 — Retrieve the Pipeline Storage Location

```python
# Cell 2: set event log path — replace with the actual storage location from the pipeline settings
DLT_STORAGE     = "abfss://eeg-lakehouse@<storage-account>.dfs.core.windows.net/dlt/day12"
EVENT_LOG_PATH  = f"{DLT_STORAGE}/system/events"

print(f"Event log path: {EVENT_LOG_PATH}")
```

### Step 11 — Read the Event Log

```python
# Cell 3: read DLT event log as a Delta table
event_df = spark.read.format("delta").load(EVENT_LOG_PATH)

print("Event log schema:")
event_df.printSchema()

display(
    event_df
    .select(
        F.col("timestamp"),
        F.col("event_type"),
        F.col("level"),
        F.col("origin.update_id").alias("update_id"),
        F.col("origin.table_name").alias("table_name"),
        F.col("message"),
    )
    .orderBy(F.col("timestamp").desc())
    .limit(100)
)
```

### Step 12 — Extract Data Quality Metrics from the Event Log

```python
# Cell 4: extract per-table expectation violation counts
quality_events = (
    event_df
    .filter(F.col("event_type") == "flow_progress")
    .select(
        F.col("timestamp"),
        F.col("origin.table_name").alias("table_name"),
        F.col("details.flow_progress.data_quality.expectations").alias("expectations"),
    )
    .filter(F.col("expectations").isNotNull())
)

# Explode expectations array and compute pass/fail counts
from pyspark.sql.functions import explode

expectations_df = (
    quality_events
    .withColumn("expectation", explode("expectations"))
    .select(
        F.col("timestamp"),
        F.col("table_name"),
        F.col("expectation.name").alias("expectation_name"),
        F.col("expectation.passed_records").cast("long").alias("passed_records"),
        F.col("expectation.failed_records").cast("long").alias("failed_records"),
    )
)

display(expectations_df.orderBy(F.col("timestamp").desc()))
```

### Step 13 — Identify Pipeline Errors from the Event Log

```python
# Cell 5: filter ERROR-level events for root-cause analysis
error_events = (
    event_df
    .filter(F.col("level") == "ERROR")
    .select(
        F.col("timestamp"),
        F.col("event_type"),
        F.col("origin.table_name").alias("table_name"),
        F.col("message"),
        F.col("error.fatal").alias("is_fatal"),
        F.col("error.exceptions").alias("exceptions"),
    )
    .orderBy(F.col("timestamp").desc())
)

error_count = error_events.count()
print(f"Total ERROR events: {error_count}")
if error_count > 0:
    display(error_events)
else:
    print("No errors found. Pipeline completed cleanly.")
```

---

## Section 8: Part 5 — Validate CDC Results

### Step 14 — Inspect the SCD Type 2 Table

```python
# Cell 6: verify SCD Type 2 history in the CDC target table
scd2_df = spark.table("eeg_lakehouse.silver.eeg_patient_scd2")

print(f"SCD2 total rows (including history): {scd2_df.count():,}")
print(f"Unique patients                     : {scd2_df.filter(F.col('__end_at').isNull()).count():,}")

display(
    scd2_df
    .orderBy("patient_id", F.col("__start_at").desc())
    .limit(20)
)
```

### Step 15 — Verify Active vs. Historical Records

```python
# Cell 7: split active and historical SCD2 records
active_records = scd2_df.filter(F.col("__end_at").isNull())
historical_records = scd2_df.filter(F.col("__end_at").isNotNull())

print(f"Active records     : {active_records.count():,}")
print(f"Historical records : {historical_records.count():,}")

# Assert that each patient has exactly one active record
duplicates = (
    active_records
    .groupBy("patient_id")
    .count()
    .filter(F.col("count") > 1)
)
duplicate_count = duplicates.count()
assert duplicate_count == 0, f"SCD2 integrity violation: {duplicate_count} patients have multiple active records."
print("SCD2 integrity check passed: each patient has exactly one active record.")
```

---

## Section 9: Part 6 — Pipeline Reset and Full Refresh

### Step 16 — Trigger a Full Refresh via the API

A full refresh reprocesses all source data from the beginning, discarding existing table contents.

```python
# Cell 8: trigger a full refresh for the Silver table using the Databricks REST API
import requests

WORKSPACE_URL   = dbutils.secrets.get(scope="eeg-lab", key="workspace_url")
DATABRICKS_TOKEN = dbutils.secrets.get(scope="eeg-lab", key="databricks_token")
PIPELINE_ID     = dbutils.secrets.get(scope="eeg-lab", key="dlt_pipeline_id_day12")

response = requests.post(
    f"{WORKSPACE_URL}/api/2.0/pipelines/{PIPELINE_ID}/updates",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json={"full_refresh": True},
)

assert response.status_code == 200, f"Pipeline start failed: {response.text}"
update_id = response.json().get("update_id")
print(f"Full refresh triggered. Update ID: {update_id}")
```

> **When to use full refresh**: use a full refresh after correcting a data quality bug in the Bronze or Silver table definition, or when adding a new column that requires backfilling from raw source data.

---

## Section 10: Exam Reference Tables

### DLT `dlt.read()` vs. `dlt.read_stream()`

| Method | Reads as | Source table must be | Output table type |
|---|---|---|---|
| `dlt.read("table")` | Batch snapshot | Any Live Table (streaming or batch) | Materialized view (batch) |
| `dlt.read_stream("table")` | Streaming source | Streaming Live Table | Streaming Live Table |

### DLT Pipeline Editions and Feature Availability

| Edition | `APPLY CHANGES INTO` | Enhanced autoscaling | Cost |
|---|---|---|---|
| Core | No | No | Lowest |
| Pro | No | Yes | Medium |
| Advanced | Yes | Yes | Highest |

### Pipeline Configuration Keys

| Key | Purpose | Default |
|---|---|---|
| `pipelines.reset.allowed` | Allows full refresh on the table | `true` |
| `delta.autoOptimize.optimizeWrite` | Coalesces small files during writes | `false` |
| `delta.autoOptimize.autoCompact` | Automatically compacts after writes | `false` |
| `pipelines.tableSamplingFraction` | Fraction of rows sampled for schema inference | `0.25` |

### Certified Professional Exam Domain Mapping — Day 12 Topics

| Topic | Professional exam domain |
|---|---|
| Multi-hop DLT pipeline with Bronze/Silver/Gold | Domain 4 — Delta Live Tables |
| `@dlt.view` vs. `@dlt.table` | Domain 4 — DLT table types |
| `dlt.apply_changes()` for SCD Type 2 | Domain 3 — Incremental Data Processing |
| `sequence_by` and out-of-order CDC | Domain 3 — CDC semantics |
| DLT event log quality metrics | Domain 4 — DLT monitoring |
| Full refresh via REST API | Domain 4 — Pipeline operations |

---

## Section 11: Self-Check Questions

Answer each question before proceeding to Day 13.

1. What is the difference between a DLT Streaming Live Table and a Materialized View?
2. Why does `APPLY CHANGES INTO` require the **Advanced** edition of DLT?
3. What column does `sequence_by` use, and why is it necessary for CDC correctness?
4. What happens to the existing contents of a Streaming Live Table when a full refresh is triggered?
5. How does `dlt.read()` differ from `dlt.read_stream()` in terms of the output table type?
6. Under which circumstance should `expect_or_fail` be preferred over `expect_or_drop`?

**Reference answers:**

1. A Streaming Live Table consumes a streaming source and processes new data incrementally with each pipeline update; it is append-oriented. A Materialized View reads a batch snapshot of its source table and recomputes the entire result on each pipeline update, similar to a refreshed SQL view backed by Delta storage.
2. `APPLY CHANGES INTO` (implemented via `dlt.apply_changes()` in Python) requires the Advanced edition because it uses the underlying CDC merge engine that is only available in that tier.
3. `sequence_by` specifies the column DLT uses to determine which CDC event is the most recent when two events with the same primary key arrive out of order. Without it, DLT cannot guarantee idempotent CDC application for late-arriving events.
4. A full refresh drops all existing data in the target table and reprocesses all source files from the beginning of the source. This is equivalent to resetting the checkpoint and rerunning the pipeline from scratch.
5. `dlt.read()` reads a batch snapshot of the referenced table, producing a Materialized View as the output. `dlt.read_stream()` reads the table as a streaming source, producing a Streaming Live Table as the output.
6. `expect_or_fail` should be used when a constraint violation indicates a systemic upstream problem (e.g., a schema change or a corrupted source feed) that would make all downstream data unreliable. Dropping individual rows would silently mask the root cause; failing the pipeline forces immediate investigation.

---

## Section 12: Day 12 Summary

| Artifact | Tool | Medallion layer | Exam domain |
|---|---|---|---|
| `eeg_bronze_advanced` Streaming Live Table | Auto Loader + `@dlt.table` | Bronze | Domain 4 |
| `eeg_cleaned_view` Live View | `@dlt.view` | Bronze → Silver | Domain 4 |
| `eeg_silver_advanced` Streaming Live Table | `@dlt.table` + expectations | Silver | Domain 4 |
| `eeg_gold_patient_summary` Materialized View | `dlt.read()` + aggregation | Gold | Domain 4 |
| `eeg_patient_scd2` SCD Type 2 CDC table | `dlt.apply_changes()` | Silver | Domain 3 |
| DLT event log quality report | PySpark Delta read + explode | Monitoring | Domain 4 |
| Full refresh trigger | Databricks REST API | — | Domain 4 |

**Next**: Day 13 covers Unity Catalog governance — lineage tracking, column-level security, row filters, and fine-grained access control for the EEG lakehouse.
