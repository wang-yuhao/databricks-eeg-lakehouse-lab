# Day 11: Structured Streaming and Real-Time EEG Ingestion

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day11_streaming.py` |
| **Exam domains** | Domain 3 — Incremental Data Processing; Domain 4 — Building Data Pipelines |
| **Time estimate** | 4–5 hours |
| **Prerequisite** | Days 1–10 completed; Bronze, Silver, and Gold tables exist; Unity Catalog configured |

---

## Section 1: Environment Setup

Complete every step in this section before opening any notebook cell.

### 1.1 Create a GitHub Personal Access Token

1. Sign in to [github.com](https://github.com).
2. Navigate to **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. Click **Generate new token (classic)**.
4. Configure the token:

   | Field | Value |
   |---|---|
   | Note | `databricks-eeg-lab` |
   | Expiration | 90 days |
   | Scopes | `repo` (full), `workflow` |

5. Copy the token immediately and store it in a password manager.

### 1.2 Configure Databricks Git Integration

1. Click your username (top-right) → **User Settings** → **Git integration**.
2. Enter:

   | Field | Value |
   |---|---|
   | Git provider | GitHub |
   | Git provider username | Your GitHub username |
   | Personal access token | Token from step 1.1 |

3. Click **Save**.

### 1.3 Clone the Repository into Databricks Repos

1. Left sidebar → **Repos** → **Add repo**.
2. Enter:

   | Field | Value |
   |---|---|
   | Git repository URL | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab` |
   | Git provider | GitHub |
   | Repo name | `databricks-eeg-lakehouse-lab` |

3. Click **Create repo**.

### 1.4 Create a Cluster with Unity Catalog Enabled

1. Left sidebar → **Compute** → **Create compute**.
2. Apply:

   | Setting | Value |
   |---|---|
   | Cluster name | `eeg-lab-day11` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS** |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled (Single user access mode) |

3. Click **Create compute**.

### 1.5 Install Required Libraries

All required packages (`pyspark`, `delta`) are included in DBR 14.3 LTS. No additional installation is needed.

Verify:

```python
# Cell 1: verify streaming dependencies
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from delta.tables import DeltaTable
print("Streaming dependencies verified.")
print(f"Spark version: {spark.version}")
```

### 1.6 Open and Attach the Notebook

1. Left sidebar → **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Open `day11_streaming.py`.
3. Click **Connect** (top-right) → select `eeg-lab-day11`.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Configure Auto Loader for event-driven file ingestion | Domain 3 — Auto Loader |
| Understand trigger modes and their trade-offs | Domain 3 — Structured Streaming |
| Apply watermarking for late-arriving data | Domain 3 — Watermarks and state |
| Write a streaming Silver transformation with `foreachBatch` | Domain 4 — Building Data Pipelines |
| Manage streaming checkpoints and restart behaviour | Domain 3 — Checkpoints |
| Query streaming state via `streamingQuery.status` | Domain 3 — Monitoring |

---

## Section 3: Background

### Trigger Modes

| Trigger | API call | Behaviour | Typical use |
|---|---|---|---|
| Default (micro-batch) | `.trigger(processingTime="0 seconds")` | Runs as fast as possible; new micro-batch starts immediately after previous completes | Development and latency-critical pipelines |
| Fixed interval | `.trigger(processingTime="30 seconds")` | Waits the specified interval between micro-batches | Balanced latency and cost |
| Available-now | `.trigger(availableNow=True)` | Processes all available data in one or more micro-batches, then stops | Scheduled batch-style incremental loads |
| Continuous | `.trigger(continuous="1 second")` | Millisecond latency via continuous processing engine | Ultra-low latency; limited sink/source support |

### Watermark and Late Data Handling

| Concept | API | Effect |
|---|---|---|
| Watermark | `.withWatermark("event_time", "10 minutes")` | Allows records up to 10 minutes late; state older than watermark is dropped |
| State retention | Controlled by the watermark delay | Reduces memory consumption for long-running streams |
| Late data beyond watermark | Silently dropped | Records that arrive after the watermark cutoff are not included in aggregations |

### Checkpoint Behaviour

| Concept | Detail |
|---|---|
| Checkpoint location | A directory on cloud storage or DBFS; stores stream progress and operator state |
| Restart semantics | Exactly-once delivery when the checkpoint is intact; the stream resumes from the last committed offset |
| Schema evolution | Changing the schema of the output without deleting the checkpoint causes a `StreamingQueryException`; use `mergeSchema` or clear the checkpoint |

---

## Section 4: Part 1 — Simulated EEG Event Source

### Step 1 — Generate Synthetic Streaming Data

```python
# Cell 2: generate synthetic streaming EEG JSON files to a landing zone
import json
import os
import time
import random
import uuid
from datetime import datetime, timezone

LANDING_PATH   = "/mnt/eeg_streaming/landing"
dbutils.fs.mkdirs(LANDING_PATH)

CHANNEL_LABELS = ["Fp1", "Fp2", "F3", "F4", "C3", "C4", "P3", "P4", "O1", "O2"]

def generate_eeg_batch(batch_id: int, n_records: int = 20) -> str:
    """Generate a JSON-lines file simulating one batch of EEG recordings."""
    records = []
    for _ in range(n_records):
        record = {
            "recording_id"      : str(uuid.uuid4()),
            "patient_id"        : f"P{random.randint(1, 50):04d}",
            "event_time"        : datetime.now(timezone.utc).isoformat(),
            "num_channels"      : len(CHANNEL_LABELS),
            "sampling_rate"     : 256,
            "duration_seconds"  : random.uniform(30.0, 300.0),
            "mean_amplitude"    : random.uniform(-50.0, 50.0),
            "std_amplitude"     : random.uniform(1.0, 30.0),
            "max_amplitude"     : random.uniform(50.0, 200.0),
            "min_amplitude"     : random.uniform(-200.0, -50.0),
            "delta_power"       : random.uniform(0.1, 2.0),
            "theta_power"       : random.uniform(0.1, 1.5),
            "alpha_power"       : random.uniform(0.1, 1.0),
            "beta_power"        : random.uniform(0.05, 0.8),
            "gamma_power"       : random.uniform(0.02, 0.3),
            "signal_quality"    : random.uniform(0.4, 1.0),
            "has_seizure"       : int(random.random() < 0.05),
            "file_format"       : random.choice(["edf", "fif"]),
        }
        records.append(json.dumps(record))

    file_path = f"{LANDING_PATH}/batch_{batch_id:04d}.json"
    dbutils.fs.put(file_path, "\n".join(records), overwrite=True)
    return file_path

# Generate three initial batches
for i in range(3):
    path = generate_eeg_batch(i)
    print(f"Generated: {path}")
```

---

## Section 5: Part 2 — Auto Loader Bronze Streaming Ingest

### Step 2 — Define the Bronze Streaming Schema

```python
# Cell 3: define the explicit Bronze schema for Auto Loader
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

BRONZE_SCHEMA = StructType([
    StructField("recording_id",    StringType(),  nullable=False),
    StructField("patient_id",      StringType(),  nullable=False),
    StructField("event_time",      StringType(),  nullable=False),  # read as string; parse below
    StructField("num_channels",    IntegerType(), nullable=True),
    StructField("sampling_rate",   IntegerType(), nullable=True),
    StructField("duration_seconds",DoubleType(),  nullable=True),
    StructField("mean_amplitude",  DoubleType(),  nullable=True),
    StructField("std_amplitude",   DoubleType(),  nullable=True),
    StructField("max_amplitude",   DoubleType(),  nullable=True),
    StructField("min_amplitude",   DoubleType(),  nullable=True),
    StructField("delta_power",     DoubleType(),  nullable=True),
    StructField("theta_power",     DoubleType(),  nullable=True),
    StructField("alpha_power",     DoubleType(),  nullable=True),
    StructField("beta_power",      DoubleType(),  nullable=True),
    StructField("gamma_power",     DoubleType(),  nullable=True),
    StructField("signal_quality",  DoubleType(),  nullable=True),
    StructField("has_seizure",     IntegerType(), nullable=True),
    StructField("file_format",     StringType(),  nullable=True),
])

print("Bronze schema defined.")
for f in BRONZE_SCHEMA.fields:
    print(f"  {f.name:<25}: {f.dataType}")
```

### Step 3 — Start the Auto Loader Bronze Stream

```python
# Cell 4: configure and start the Auto Loader Bronze ingestion stream
CHECKPOINT_BRONZE = "/mnt/checkpoints/streaming/bronze"
BRONZE_TABLE       = "eeg_lakehouse.bronze.eeg_bronze_stream"

dbutils.fs.mkdirs(CHECKPOINT_BRONZE)

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",         "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema/bronze_stream")
    .option("cloudFiles.inferColumnTypes","true")
    .schema(BRONZE_SCHEMA)
    .load(LANDING_PATH)
    .withColumn("event_timestamp",     F.to_timestamp("event_time"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file",         F.col("_metadata.file_path"))
    .withColumn("ingestion_date",      F.to_date(F.current_timestamp()))
    .drop("event_time")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(processingTime="10 seconds")
    .toTable(BRONZE_TABLE)
)

print(f"Bronze stream started. Query ID: {bronze_stream.id}")
print(f"Status: {bronze_stream.status}")
```

### Step 4 — Verify Bronze Stream Progress

```python
# Cell 5: monitor stream progress
import time
time.sleep(15)  # allow at least one micro-batch to complete

print("Stream status  :", bronze_stream.status)
print("Recent progress:", bronze_stream.recentProgress[-1] if bronze_stream.recentProgress else "No progress yet")
row_count = spark.table(BRONZE_TABLE).count()
print(f"Bronze table row count: {row_count:,}")
assert row_count > 0, "Bronze stream has not written any rows. Check the landing path and schema."
```

---

## Section 6: Part 3 — Streaming Silver Transformation with `foreachBatch`

### Step 5 — Define the `foreachBatch` Transform Function

```python
# Cell 6: define foreachBatch Silver upsert function
from delta.tables import DeltaTable

SILVER_STREAM_TABLE  = "eeg_lakehouse.silver.eeg_silver_stream"
CHECKPOINT_SILVER    = "/mnt/checkpoints/streaming/silver"

# Create Silver table if it does not yet exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_STREAM_TABLE} (
        recording_id        STRING      NOT NULL,
        patient_id          STRING      NOT NULL,
        event_timestamp     TIMESTAMP,
        num_channels        INT,
        sampling_rate       INT,
        duration_seconds    DOUBLE,
        mean_amplitude      DOUBLE,
        std_amplitude       DOUBLE,
        max_amplitude       DOUBLE,
        min_amplitude       DOUBLE,
        delta_power         DOUBLE,
        theta_power         DOUBLE,
        alpha_power         DOUBLE,
        beta_power          DOUBLE,
        gamma_power         DOUBLE,
        signal_quality      DOUBLE,
        has_seizure         INT,
        is_valid            BOOLEAN,
        processed_timestamp TIMESTAMP,
        ingestion_date      DATE
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Streaming Silver layer: validated and deduplicated EEG recordings.'
""")


def upsert_to_silver(micro_batch_df, batch_id: int) -> None:
    """
    Apply quality rules, deduplicate within the micro-batch,
    and merge into the Silver Delta table.
    """
    # Quality rules
    validated_df = (
        micro_batch_df
        .filter(F.col("recording_id").isNotNull())
        .filter(F.col("patient_id").isNotNull())
        .withColumn(
            "is_valid",
            (
                F.col("num_channels").between(1, 256) &
                F.col("sampling_rate").between(100, 10_000) &
                F.col("duration_seconds").between(1.0, 86_400.0) &
                F.col("signal_quality").between(0.0, 1.0)
            ),
        )
        .withColumn("processed_timestamp", F.current_timestamp())
        .withColumn("ingestion_date",       F.to_date(F.current_timestamp()))
        # Deduplicate within the micro-batch (keep the last-seen row per recording_id)
        .dropDuplicates(["recording_id"])
    )

    # Merge into Silver (upsert by recording_id)
    silver_table = DeltaTable.forName(spark, SILVER_STREAM_TABLE)
    (
        silver_table.alias("target")
        .merge(
            validated_df.alias("source"),
            "target.recording_id = source.recording_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Batch {batch_id}: merged {validated_df.count()} rows into {SILVER_STREAM_TABLE}")
```

### Step 6 — Start the Silver Streaming Query

```python
# Cell 7: start Silver stream using foreachBatch
silver_stream = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
    .withWatermark("event_timestamp", "5 minutes")
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"Silver stream started. Query ID: {silver_stream.id}")
```

---

## Section 7: Part 4 — Windowed Streaming Aggregation

### Step 7 — Compute Sliding Window Metrics

```python
# Cell 8: 5-minute tumbling window quality aggregation with watermark
AGG_TABLE       = "eeg_lakehouse.silver.eeg_stream_agg"
CHECKPOINT_AGG = "/mnt/checkpoints/streaming/agg"

agg_stream = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
        F.window(F.col("event_timestamp"), "5 minutes"),
        F.col("patient_id"),
    )
    .agg(
        F.count("recording_id").alias("recording_count"),
        F.avg("signal_quality").alias("avg_signal_quality"),
        F.avg("mean_amplitude").alias("avg_mean_amplitude"),
        F.sum(F.when(F.col("has_seizure") == 1, 1).otherwise(0)).alias("seizure_count"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("patient_id"),
        F.col("recording_count"),
        F.col("avg_signal_quality"),
        F.col("avg_mean_amplitude"),
        F.col("seizure_count"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_AGG)
    .trigger(processingTime="1 minute")
    .toTable(AGG_TABLE)
)

print(f"Aggregation stream started. Query ID: {agg_stream.id}")
```

---

## Section 8: Part 5 — Continuous Data Generation and Monitoring

### Step 8 — Generate Additional Batches

```python
# Cell 9: generate more data batches to feed the stream
for i in range(3, 8):
    path = generate_eeg_batch(i, n_records=50)
    print(f"Generated: {path}")
    time.sleep(5)

# Wait for streams to process
time.sleep(30)
```

### Step 9 — Inspect All Active Streaming Queries

```python
# Cell 10: inspect all active streaming queries
for q in spark.streams.active:
    print(f"Name    : {q.name}")
    print(f"ID      : {q.id}")
    print(f"Status  : {q.status}")
    if q.recentProgress:
        last = q.recentProgress[-1]
        print(f"  inputRowsPerSecond  : {last.get('inputRowsPerSecond', 0):.2f}")
        print(f"  processedRowsPerSecond: {last.get('processedRowsPerSecond', 0):.2f}")
        print(f"  batchId             : {last.get('batchId', 'N/A')}")
    print()
```

---

## Section 9: Part 6 — Available-Now Trigger (Batch Mode)

### Step 10 — Run a One-Time Incremental Load

```python
# Cell 11: available-now trigger for scheduled incremental loads
ONCE_CHECKPOINT = "/mnt/checkpoints/streaming/bronze_once"
ONCE_TABLE      = "eeg_lakehouse.bronze.eeg_bronze_once"

# Generate one more file to ensure there is new data
generate_eeg_batch(99, n_records=10)

bronze_once = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",         "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema/bronze_once")
    .schema(BRONZE_SCHEMA)
    .load(LANDING_PATH)
    .withColumn("event_timestamp",     F.to_timestamp("event_time"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .drop("event_time")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", ONCE_CHECKPOINT)
    .trigger(availableNow=True)        # Processes all pending files, then terminates
    .toTable(ONCE_TABLE)
)

bronze_once.awaitTermination()         # Blocks until the trigger completes
row_count = spark.table(ONCE_TABLE).count()
print(f"Available-now load complete. Rows written: {row_count:,}")
```

---

## Section 10: Part 7 — Stream Graceful Shutdown

### Step 11 — Stop All Active Streams

```python
# Cell 12: gracefully stop all active streaming queries
for q in spark.streams.active:
    q_id   = q.id
    q_name = q.name
    q.stop()
    print(f"Stopped stream: name={q_name}, id={q_id}")

print(f"Active streams remaining: {len(spark.streams.active)}")
```

---

## Section 11: Exam Reference Tables

### Structured Streaming Output Modes

| Mode | Delta support | When to use |
|---|---|---|
| `append` | Yes | Rows are written once and never updated; no aggregation state |
| `update` | Yes (with `foreachBatch`) | Only changed rows are written per micro-batch |
| `complete` | Requires bounded state (e.g., static aggregation) | Full result table rewritten each batch; high cost at scale |

### Auto Loader Configuration Options

| Option | Purpose | Recommended value |
|---|---|---|
| `cloudFiles.format` | File format of landing zone files | `json`, `parquet`, `csv` |
| `cloudFiles.schemaLocation` | Path to store the inferred schema | Unique path per stream |
| `cloudFiles.inferColumnTypes` | Cast inferred string columns to native types | `true` for non-binary formats |
| `cloudFiles.useNotifications` | Use cloud notification (SNS/Event Grid) instead of directory listing | `true` for high-scale production |
| `cloudFiles.maxFilesPerTrigger` | Maximum files processed per micro-batch | `1000` for backfill control |

### Certified Professional Exam Domain Mapping — Day 11 Topics

| Topic | Professional exam domain |
|---|---|
| Auto Loader `cloudFiles` source | Domain 3 — Incremental Data Processing |
| Trigger modes (`processingTime`, `availableNow`) | Domain 3 — Structured Streaming |
| Watermarking for late data | Domain 3 — Watermarks and state |
| `foreachBatch` with Delta merge | Domain 4 — Building Data Pipelines |
| Tumbling window aggregations | Domain 3 — Structured Streaming |
| Checkpoint management and restart | Domain 3 — Checkpoints |

---

## Section 12: Self-Check Questions

1. What is the difference between `trigger(processingTime="30 seconds")` and `trigger(availableNow=True)`?
2. Why must the `checkpointLocation` be unique per streaming query?
3. How does `.withWatermark("event_timestamp", "10 minutes")` affect memory usage in a long-running aggregation stream?
4. What is the purpose of `dropDuplicates(["recording_id"])` inside `foreachBatch`?
5. In which output mode must a Delta streaming write operate when using `foreachBatch` with a merge?

**Reference answers:**

1. `processingTime` triggers micro-batches on a fixed clock interval indefinitely. `availableNow` processes all files currently pending in the source (similar to the deprecated `once` trigger) and then terminates the query, making it suitable for scheduled Databricks Jobs.
2. The checkpoint stores the committed read offsets and operator state for a specific query. If two queries share a checkpoint, they will corrupt each other’s progress records, causing data loss or duplication.
3. The watermark bounds the state held in memory for the aggregation. Records older than `current_event_time - 10 minutes` are dropped, preventing unbounded state growth in long-running streams.
4. Auto Loader may deliver the same file more than once in error-recovery scenarios. Deduplicating within each micro-batch before merging into the Delta table prevents duplicate rows in the Silver table.
5. `foreachBatch` requires no output mode on the `writeStream` itself because the user controls the write logic inside the function. Internally, the function uses a `merge` operation which is neither append, update, nor complete — it is an arbitrary Delta write.

---

## Section 13: Day 11 Summary

| Artifact | Tool | Medallion layer | Exam domain |
|---|---|---|---|
| `eeg_bronze_stream` streaming table | Auto Loader `cloudFiles` + Delta | Bronze | Domain 3 |
| `upsert_to_silver` merge function | `foreachBatch` + Delta merge | Silver | Domain 4 |
| `eeg_silver_stream` upsert table | Delta `MERGE` | Silver | Domain 4 |
| 5-minute window aggregation table | Structured Streaming + watermark | Silver | Domain 3 |
| Available-now incremental load | `trigger(availableNow=True)` | Bronze | Domain 3 |

**Next**: Day 12 covers Delta Live Tables advanced features: pipeline orchestration, multi-hop dependencies, CDC with `APPLY CHANGES INTO`, and pipeline event log analysis.
