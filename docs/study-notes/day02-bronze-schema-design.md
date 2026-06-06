# Day 2 — Dataset Interface & Bronze Schema Design

**Exam Domain:** Auto Loader, Delta Lake schema enforcement, incremental ingestion 
**Pipeline Layer:** Bronze 
**Session Time:** ~2 hours

---

## Learning Objectives

- Design an explicit Bronze schema for binary EDF metadata
- Understand Auto Loader options: `cloudFiles.schemaLocation`, `mergeSchema`, `cloudFiles.useNotifications`
- Distinguish schema-on-read vs schema-on-write
- Explain why Bronze tables use `append` mode

---

## Core Concepts

### 1. Auto Loader (`format("cloudFiles")`)

Auto Loader is Databricks' optimised file ingestion mechanism for cloud object storage. It is the **primary incremental ingestion pattern** on the exam.

```python
# Minimal Auto Loader pattern
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")       # source file format
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load("/mnt/raw/incoming/")
)

df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/bronze")
    .outputMode("append")
    .trigger(availableNow=True)               # process all available, then stop
    .toTable("catalog.bronze.my_table")
```

**Key Auto Loader options:**

| Option | Purpose | Default |
|--------|---------|--------|
| `cloudFiles.format` | Source format (json, csv, parquet, binary) | required |
| `cloudFiles.schemaLocation` | Path to infer & store schema across runs | required for JSON/CSV |
| `cloudFiles.useNotifications` | Use cloud notifications vs directory listing | false (listing) |
| `cloudFiles.includeExistingFiles` | Include files that existed before stream started | true |
| `cloudFiles.maxFilesPerTrigger` | Rate-limit ingestion | unlimited |
| `cloudFiles.backfillInterval` | Periodic full directory scan to catch missed files | none |

**Listing mode vs Notification mode:**
- **Listing mode** (default): Databricks scans the directory on each trigger. Simple, no infra needed.
- **Notification mode**: Cloud storage sends event notifications (AWS SQS / Azure Event Grid). Scales to millions of files.

### 2. Schema Enforcement vs Inference

```python
# Schema inference (risky in prod)
df = spark.read.json("/path/")  # infers from sample

# Explicit schema (recommended for Bronze)
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("file_path",    StringType(),    nullable=False),
    StructField("subject_id",   StringType(),    nullable=True),
    StructField("sampling_rate",LongType(),      nullable=True),
    StructField("channel_count",LongType(),      nullable=True),
])

df = spark.read.schema(schema).json("/path/")
```

**Schema evolution options:**

```python
# mergeSchema: add new columns from new files, keep old data
df.write.option("mergeSchema", "true").mode("append").saveAsTable("bronze.table")

# overwriteSchema: completely replace schema (use with caution)
df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("bronze.table")
```

### 3. EDF File Schema Design

EDF (European Data Format) is the standard for physiological recordings. The Bronze table stores **metadata** (not the binary signal):

```python
BRONZE_EDF_SCHEMA = StructType([
    StructField("file_path",           StringType(),    nullable=False),
    StructField("file_name",           StringType(),    nullable=False),
    StructField("subject_id",          StringType(),    nullable=True),
    StructField("recording_type",      StringType(),    nullable=True),  # PSG or Hypnogram
    StructField("subject_age",         IntegerType(),   nullable=True),
    StructField("study_night",         IntegerType(),   nullable=True),
    StructField("is_hypnogram",        BooleanType(),   nullable=False),
    StructField("file_size_bytes",     LongType(),      nullable=True),
    StructField("file_modification_time", TimestampType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])
```

**Why not store the raw EDF binary in Delta?**
- Delta tables are columnar (Parquet). Binary blobs belong in object storage (ADLS/S3)
- Bronze stores the **metadata registry**: who, what, when, where of each file
- The actual signal processing happens in Silver using MNE-Python via Pandas UDFs

### 4. Delta Table Creation Patterns

```sql
-- CTAS: Create Table As Select
CREATE OR REPLACE TABLE catalog.bronze.eeg_files
USING DELTA
PARTITIONED BY (study_night)
COMMENT 'Bronze registry of EDF files ingested from PhysioNet'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT * FROM temp_view;
```

**Table properties to know:**
- `delta.autoOptimize.optimizeWrite`: coalesces small files on write
- `delta.autoOptimize.autoCompact`: triggers background compaction
- `delta.deletedFileRetentionDuration`: how long before VACUUM can remove files
- `delta.logRetentionDuration`: how long to keep transaction log entries

---

## Exam Focus Areas

### Exam Question Pattern: Auto Loader vs COPY INTO

| Feature | Auto Loader | COPY INTO |
|---------|-------------|----------|
| Mode | Streaming (readStream) | Batch SQL command |
| State tracking | Checkpoint (distributed) | Table metadata |
| Scale | Millions of files | Thousands of files |
| Schema inference | Yes, with `cloudFiles.schemaLocation` | Yes, with `COPY OPTIONS` |
| Use case | Continuous incremental | Scheduled batch loads |
| Duplicate prevention | Checkpoint prevents re-reads | State tracking prevents re-reads |

### Exam Question Pattern: Write Modes

| Mode | Behaviour | Delta support |
|------|-----------|---------------|
| `append` | Add new rows, keep old | Yes |
| `overwrite` | Replace entire table | Yes |
| `error` | Fail if table exists | Yes |
| `ignore` | Do nothing if table exists | Yes |

> ⚠️ `overwrite` in Delta does NOT delete the transaction log. You can still time-travel.

---

## Research Context

The Bronze layer for the EEG pipeline stores a **file registry** — not EEG signals. This is because:
1. EDF files are binary and cannot be stored directly in Parquet columnar format
2. Metadata (subject_id, sampling_rate, channel_count) is what drives partitioning and joins
3. Silver processing reads the file_path from Bronze to access the actual EDF binary

**EDF filename pattern:** `SC4001E0-PSG.edf`
- `SC` = Study Cassette
- `4001` = Subject 4001
- `E0` = First night (E0, E1 = nights 1, 2)
- `PSG` = Polysomnography recording (vs `Hypnogram`)

---

## Key Files Created Today

| File | Purpose |
|------|---------|
| `src/bronze/ingest_eeg_files.py` | Schema definition, `extract_subject_id()`, `load_raw_files()` skeleton |
| `src/bronze/ingest_metadata.py` | CSV subject metadata ingestion |
| `src/utils/config.py` | Extended with `DataPaths`, `CatalogConfig`, `SparkConfig` dataclasses |
| `notebooks/day02_bronze_schema_design.py` | Interactive schema exploration notebook |

---

## Self-Check Questions

1. What does `cloudFiles.schemaLocation` do, and why is it required for JSON?
2. What is the difference between listing mode and notification mode in Auto Loader?
3. When would you use `mergeSchema=true` vs `overwriteSchema=true`?
4. Why does the Bronze EDF table store metadata rather than raw binary?
5. How does Auto Loader prevent duplicate ingestion of the same file?
6. What is `availableNow=True` trigger and how does it differ from `once=True`?

---

## Further Reading

- [Auto Loader documentation](https://docs.databricks.com/ingestion/auto-loader/)
- [Delta Lake schema enforcement](https://docs.delta.io/latest/delta-batch.html#schema-validation)
- [EDF+ format specification](https://www.edfplus.info/specs/edf.html)
