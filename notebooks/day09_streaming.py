# Databricks notebook source
# notebooks/day09_streaming.py
# =============================================================================
# DAY 9 - Structured Streaming: Simulated Live EEG Events
# =============================================================================
# EXAM DOMAINS: readStream/writeStream, trigger modes, watermarks, output modes,
#               checkpointing, Auto Loader streaming, windowed aggregations
# RESEARCH: Scaffold for real-time EEG monitoring (BCI / bedside applications)
# =============================================================================

# COMMAND ----------
# %md
# ## Day 9: Structured Streaming
#
# ### Key Exam Concepts
# | Concept | Description |
# |---------|-------------|
# | Trigger | Controls how often micro-batches run |
# | Output mode | Append / Complete / Update |
# | Watermark | Max lateness for late data |
# | Checkpoint | Exactly-once guarantees + recovery |
# | foreachBatch | Apply arbitrary DataFrame ops to each micro-batch |

# COMMAND ----------
# %md ### Setup

from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

# COMMAND ----------
# %md ### Step 1: Rate Source (Simulated EEG Events)
# EXAM NOTE: `rate` source generates rows at fixed rate for testing streaming.
# In production: use cloudFiles (Auto Loader), Kafka, or Event Hubs.

# Simulate EEG events arriving at 256 samples/sec
# Each row = one simulated EEG epoch event
df_stream_raw = (
    spark.readStream
    .format("rate")             # built-in test source
    .option("rowsPerSecond", 10)  # 10 events/sec for demo
    .load()
    .withColumn("subject_id",
        F.element_at(
            F.array(F.lit("SC4001"), F.lit("SC4002"), F.lit("SC4003")),
            (F.col("value") % 3 + 1).cast(IntegerType())
        ).cast(StringType())
    )
    .withColumn("channel",
        F.element_at(
            F.array(F.lit("Fz"), F.lit("Cz"), F.lit("Pz")),
            (F.col("value") % 3 + 1).cast(IntegerType())
        )
    )
    .withColumn("amplitude_uv",
        F.round(F.randn() * 30 + 10, 2)  # Gaussian noise around 10 uV
    )
    .withColumn("event_time", F.col("timestamp"))  # streaming event time
    .drop("value")
)

print("Streaming schema:")
df_stream_raw.printSchema()

# COMMAND ----------
# %md ### Step 2: Watermark + Window Aggregation
# EXAM NOTE:
# - withWatermark defines max delay for late data
# - window() creates time-based buckets
# - Output mode COMPLETE: rewrite entire result table each batch
# - Output mode APPEND: only write new rows (no updates to old windows)
# - Output mode UPDATE: update rows that changed
# RULE: windowed agg with watermark -> use Append or Update mode

df_windowed = (
    df_stream_raw
    .withWatermark("event_time", "10 seconds")  # tolerate 10s late data
    .groupBy(
        F.window("event_time", "30 seconds", "10 seconds"),  # 30s window, 10s slide
        F.col("subject_id"),
        F.col("channel"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.mean("amplitude_uv").alias("mean_amplitude"),
        F.max("amplitude_uv").alias("max_amplitude"),
        F.stddev("amplitude_uv").alias("std_amplitude"),
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

# COMMAND ----------
# %md ### Step 3: Write Stream to Delta (Append mode)
# EXAM NOTE:
# - checkpointLocation is REQUIRED for fault tolerance
# - Trigger.availableNow() = process all available data then stop (batch-like)
# - Trigger.processingTime('30 seconds') = run every 30s
# - Trigger.once() = deprecated; use availableNow() instead

CHECKPOINT_PATH = "/tmp/eeg_streaming_checkpoint"
OUTPUT_TABLE = "eeg_lakehouse.silver.streaming_eeg_windows"

query = (
    df_windowed
    .writeStream
    .format("delta")
    .outputMode("append")           # EXAM: append works with watermarked windows
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")  # micro-batch every 10s
    # .trigger(availableNow=True)    # uncomment for "run once" batch trigger
    .toTable(OUTPUT_TABLE)          # writes to UC-managed Delta table
)

print(f"Streaming query started: {query.name}")
print(f"Status: {query.status}")

# Run for 30 seconds then stop
time.sleep(30)
query.stop()
print("Query stopped.")

# COMMAND ----------
# %md ### Step 4: foreachBatch - Write to Multiple Sinks
# EXAM NOTE: foreachBatch lets you apply arbitrary DataFrame operations
# to each micro-batch. Useful for: dual writes, MERGE INTO, custom logic.

def process_batch(df_batch, batch_id):
    """
    Called for each micro-batch.
    EXAM PATTERN: Use foreachBatch for MERGE INTO (upsert) on Delta.
    """
    print(f"Batch {batch_id}: {df_batch.count()} rows")

    # Write to Delta with MERGE (upsert by subject+window)
    # In production: use DeltaTable.forName().merge()
    df_batch.write.format("delta").mode("append").saveAsTable(
        "eeg_lakehouse.silver.streaming_eeg_raw"
    )
    # Could also write to a second sink (e.g., alert table)
    high_amp = df_batch.filter(F.col("amplitude_uv") > 100)
    if high_amp.count() > 0:
        print(f"  ALERT: {high_amp.count()} high-amplitude events in batch {batch_id}")


query_foreach = (
    df_stream_raw
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "/tmp/eeg_foreach_checkpoint")
    .trigger(processingTime="10 seconds")
    .foreachBatch(process_batch)
    .start()
)

time.sleep(20)
query_foreach.stop()

# COMMAND ----------
# %md ### Step 5: Read Streaming Delta Table
# EXAM NOTE: Delta tables support BOTH batch and streaming reads.
# readStream on a Delta table = "change data feed" or append-only stream.

df_delta_stream = (
    spark.readStream
    .format("delta")
    .table("eeg_lakehouse.silver.streaming_eeg_windows")
)
print("Streaming from Delta table:")
df_delta_stream.printSchema()

# COMMAND ----------
# %md
# ## Streaming Exam Quick Reference
#
# | Trigger type | Behavior | Use case |
# |-------------|----------|----------|
# | `processingTime('30s')` | Micro-batch every 30s | Standard streaming |
# | `availableNow=True` | Process all available, stop | Incremental batch |
# | `once=True` | DEPRECATED - use availableNow | Legacy |
# | `continuous='1s'` | Millisecond latency (experimental) | Ultra-low latency |
#
# | Output mode | When to use |
# |------------|-------------|
# | Append | No updates to old rows (most common) |
# | Complete | Rewrite entire result (small aggregations) |
# | Update | Only rows that changed (Delta only) |
#
# ### EEG Research Application
# - Real-time spindle detection -> alert if density exceeds threshold
# - foreachBatch -> MERGE INTO Gold for incremental feature updates
# - Watermark -> handle late EEG packet arrivals (network jitter)
# - Checkpoint -> survive cluster restart without reprocessing data

print("Day 9 complete! Proceed to Day 10: MLflow training.")
