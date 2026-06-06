# Day 3 — Bronze Ingestion with Databricks Patterns

**Exam Domain:** Auto Loader (cloudFiles), Delta DML, DESCRIBE HISTORY, incremental ingestion 
**Pipeline Layer:** Bronze 
**Session Time:** ~2-3 hours

---

## Learning Objectives

- Implement a full Auto Loader Bronze ingestion pipeline with checkpoint
- Use `DESCRIBE HISTORY` to audit Delta table operations
- Write idempotent ingestion: re-running produces no duplicates
- Write unit tests for Bronze ingestion logic using local SparkSession

---

## Core Concepts

### 1. Full Auto Loader Bronze Pipeline

```python
def create_bronze_table(
    spark: SparkSession,
    source_path: str,
    checkpoint_path: str,
    table_name: str,
) -> None:
    """Idempotent Bronze ingestion with Auto Loader."""
    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")  # EDF = binary
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("pathGlobFilter", "*.edf")           # only EDF files
        .load(source_path)
        .select(
            col("path").alias("file_path"),
            col("name").alias("file_name"),
            extract_subject_id(col("name")).alias("subject_id"),
            extract_study_night(col("name")).alias("study_night"),
            is_hypnogram_file(col("name")).alias("is_hypnogram"),
            col("size").alias("file_size_bytes"),
            col("modificationTime").alias("file_modification_time"),
            current_timestamp().alias("ingestion_timestamp"),
        )
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/bronze")
        .trigger(availableNow=True)
        .toTable(table_name)
    ).awaitTermination()
```

### 2. Delta DML Patterns

```sql
-- Inspect what was ingested
SELECT * FROM eeg_lakehouse.bronze.eeg_files LIMIT 10;

-- Describe the table schema
DESCRIBE TABLE EXTENDED eeg_lakehouse.bronze.eeg_files;

-- Show the full audit trail
DESCRIBE HISTORY eeg_lakehouse.bronze.eeg_files;

-- Count by subject
SELECT subject_id, COUNT(*) as file_count
FROM eeg_lakehouse.bronze.eeg_files
GROUP BY subject_id
ORDER BY subject_id;
```

**DESCRIBE HISTORY columns to know:**
- `version` — monotonically increasing version number
- `timestamp` — when the operation happened
- `operation` — WRITE, MERGE, DELETE, OPTIMIZE, VACUUM, etc.
- `operationParameters` — JSON with operation details
- `numOutputRows` — rows written
- `numOutputFiles` — Parquet files created

### 3. Delta Transaction Isolation

Delta uses **optimistic concurrency control**:

```
Write transaction attempts:
1. Read current version
2. Apply changes locally
3. Attempt to commit new version
4. If another writer committed since step 1 → conflict check
   - If changes are on different rows/partitions → both succeed (concurrent writes allowed)
   - If same rows modified → retry or fail
```

**Conflict matrix:**

| Operation A | Operation B | Conflict? |
|-------------|-------------|----------|
| INSERT | INSERT | No (different rows) |
| DELETE (partition A) | DELETE (partition B) | No |
| UPDATE (col x) | UPDATE (col x, same row) | Yes |
| OPTIMIZE | INSERT | No |

### 4. Checkpoint Directory Structure

```
checkpoints/
├── schema/           ← cloudFiles schema inference state
├── bronze/
│   ├── offsets/        ← stream progress (which files processed)
│   ├── commits/        ← confirmed completions
│   └── sources/        ← file list per micro-batch
```

> ⚠️ Never manually modify or delete checkpoints. If you need to reprocess, delete both the checkpoint AND the target Delta table, then re-run.

### 5. Testing Bronze Logic

```python
# tests/test_bronze.py
def test_extract_subject_id():
    assert extract_subject_id("SC4001E0-PSG.edf") == "SC4001"
    assert extract_subject_id("SC4002E1-Hypnogram.edf") == "SC4002"
    assert extract_subject_id("unknown.edf") is None

def test_bronze_schema(spark):
    """Schema must match BRONZE_EDF_SCHEMA exactly."""
    data = [("path/SC4001E0-PSG.edf", "SC4001E0-PSG.edf", 1000, None)]
    df = spark.createDataFrame(data, ["file_path", "file_name", "file_size_bytes", "subject_id"])
    # validate schema invariants
    assert "file_path" in df.columns
    assert df.filter(col("file_path").isNull()).count() == 0
```

---

## Exam Focus Areas

### Delta Table Operations

```sql
-- INSERT INTO (append rows)
INSERT INTO catalog.bronze.eeg_files
SELECT * FROM new_batch;

-- INSERT OVERWRITE (replace table, keep history)
INSERT OVERWRITE catalog.bronze.eeg_files
SELECT * FROM reprocessed_batch;

-- DELETE with predicate
DELETE FROM catalog.bronze.eeg_files
WHERE ingestion_timestamp < '2026-01-01';

-- UPDATE
UPDATE catalog.bronze.eeg_files
SET recording_type = 'PSG'
WHERE recording_type IS NULL AND is_hypnogram = false;
```

### MERGE INTO (Upsert)

```sql
MERGE INTO catalog.bronze.eeg_files AS target
USING new_records AS source
ON target.file_path = source.file_path
WHEN MATCHED THEN
  UPDATE SET target.file_size_bytes = source.file_size_bytes
WHEN NOT MATCHED THEN
  INSERT *;
```

> MERGE is the most important DML operation for the exam. Know all three clauses:
> `WHEN MATCHED`, `WHEN NOT MATCHED`, `WHEN NOT MATCHED BY SOURCE`

---

## Research Context

Bronze ingestion is **idempotent** — re-running the pipeline on the same source directory produces no duplicate rows. This is critical for the EEG corpus because:
- PhysioNet datasets are updated periodically with corrected files
- Re-ingestion must be safe and non-destructive
- Auto Loader checkpoint tracks exactly which files have been processed

**File naming conventions in Sleep-EDF Expanded:**
- PSG files: `SC4{subject}{night}-PSG.edf` (e.g., `SC40011E-PSG.edf`)
- Hypnogram files: `SC4{subject}{night}-Hypnogram.edf`
- Cassette studies: prefix `SC`, telemetry studies: prefix `ST`

---

## Key Files Created Today

| File | Purpose |
|------|---------|
| `src/bronze/ingest_eeg_files.py` | `create_bronze_table()` with full Auto Loader pattern |
| `src/bronze/ingest_metadata.py` | CSV subject metadata ingestion |
| `notebooks/day03_bronze_ingestion.py` | Run ingestion, DESCRIBE HISTORY, SELECT |
| `tests/test_bronze.py` | Unit tests: `extract_subject_id`, schema invariants, null checks |

---

## Self-Check Questions

1. What files are stored in the checkpoint directory and why are they critical?
2. How does Delta prevent concurrent write conflicts?
3. What is the difference between `INSERT INTO` and `INSERT OVERWRITE` in Delta?
4. Why is MERGE INTO preferred over DELETE + INSERT for upsert patterns?
5. What happens if you run Auto Loader twice on the same source path without clearing the checkpoint?
6. What does `DESCRIBE HISTORY` reveal that `DESCRIBE TABLE EXTENDED` does not?

---

## Further Reading

- [Delta Lake DML operations](https://docs.delta.io/latest/delta-update.html)
- [Structured Streaming checkpoints](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
- [MERGE INTO documentation](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
