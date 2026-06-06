# Day 09 – Structured Streaming: Watermarks, Triggers & Stateful Processing

## Overview
Spark Structured Streaming treats a stream as an unbounded table. New data appended
to the source is processed incrementally. Key exam topics: watermarks, trigger modes,
output sinks, stateful aggregations, and `foreachBatch`.

---

## Core Concepts

### 1. Streaming DataFrame Basics
```python
# Read from a streaming source (e.g., Kafka or Auto Loader)
stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(epoch_schema)
    .load("/mnt/bronze/eeg/")
)

# Write to Delta sink
query = (
    stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/eeg_bronze")
    .trigger(processingTime="1 minute")
    .toTable("eeg_catalog.bronze.raw_signals")
)
query.awaitTermination()
```

### 2. Trigger Modes
| Trigger | Behaviour |
|---|---|
| `processingTime="1 minute"` | Fixed micro-batch interval |
| `processingTime="0"` (default) | Run as fast as possible |
| `once=True` | Process all available data; then stop (deprecated) |
| `availableNow=True` | Like `once` but uses multiple micro-batches; preferred |
| `continuous="1 second"` | Experimental millisecond latency |

**Exam tip:** Prefer `availableNow=True` over `once=True` for scheduled batch-like
streaming jobs in Databricks workflows.

### 3. Watermarks for Late Data
```python
from pyspark.sql import functions as F

# Allow events up to 10 minutes late
windowed = (
    stream_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "5 minutes"),
        "subject_id"
    )
    .agg(F.count("*").alias("epoch_count"))
)
```
- **Watermark** = current max event time minus the late threshold.
- Data arriving after the watermark is **dropped**.
- Required for stateful aggregations with `append` output mode.

### 4. Output Modes
| Mode | Description | When to use |
|---|---|---|
| `append` | Only new rows written | Stateless transforms; watermarked aggregations |
| `complete` | Full result table rewritten | Small aggregation tables |
| `update` | Only changed rows written | Aggregations without watermark |

### 5. Stateful Operations
```python
# Streaming deduplication with watermark
deduped = (
    stream_df
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["epoch_id", "event_time"])
)

# Stream-stream join with watermark
joined = stream_epochs.join(
    stream_annotations,
    expr("""
        stream_epochs.subject_id = stream_annotations.subject_id AND
        stream_annotations.ann_time >= stream_epochs.epoch_start AND
        stream_annotations.ann_time <= stream_epochs.epoch_start + interval 30 seconds
    """),
    "leftOuter"
)
```

### 6. `foreachBatch` Pattern
```python
def process_batch(batch_df, batch_id):
    """Custom processing per micro-batch."""
    # Upsert (MERGE) instead of simple append
    batch_df.createOrReplaceTempView("batch_epochs")
    spark.sql("""
        MERGE INTO eeg_catalog.silver.epochs AS t
        USING batch_epochs AS s
        ON t.epoch_id = s.epoch_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

query = (
    stream_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/mnt/checkpoints/silver_epochs")
    .trigger(processingTime="2 minutes")
    .start()
)
```

---

## EEG / Neuroscience Context

### Real-Time EEG Use Cases
- **Live sleep staging:** Classify 30-s epochs as Wake/N1/N2/N3/REM in near real-time.
- **Artefact detection stream:** Flag channels with EMG or movement artefacts immediately.
- **Alert pipeline:** Trigger alert if spindle rate drops below clinical threshold.
- **Watermark relevance:** EEG data from remote monitoring devices may arrive minutes late
  due to network conditions; a 5-minute watermark accommodates this.

### EEG Streaming Architecture
```
EEG Device → Kafka/Event Hub → Auto Loader (Bronze) → Watermarked Aggregation → Silver
                                       ↓
                               foreachBatch MERGE → Delta table
```

---

## Exam-Focused Summary

| Topic | Key Fact |
|---|---|
| Watermark | `.withWatermark(col, delay)` drops late data; required for append aggregations |
| `availableNow` | Preferred trigger for scheduled streaming jobs |
| `foreachBatch` | Enables MERGE, custom writes; receives (df, batch_id) |
| Output modes | append/complete/update; append most common with watermark |
| Checkpoint | Required for fault-tolerance; stores offsets and state |
| Stream-stream join | Requires watermark on both sides |

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `src/streaming/eeg_stream_processor.py` | StreamProcessor class with watermark and foreachBatch |
| `notebooks/day09_streaming.py` | Interactive streaming notebook |
| `tests/test_streaming.py` | Streaming logic unit tests |

---

## Self-Check Questions
1. What happens to data that arrives after the watermark threshold?
2. What is the difference between `once=True` and `availableNow=True`?
3. When would you use `complete` output mode?
4. Why does a stream-stream join require watermarks on both sides?
5. What does `foreachBatch` enable that standard writeStream cannot do directly?
6. How would you implement real-time EEG artefact alerting with Structured Streaming?

---

## Further Reading
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks Streaming Best Practices](https://docs.databricks.com/en/structured-streaming/production.html)
