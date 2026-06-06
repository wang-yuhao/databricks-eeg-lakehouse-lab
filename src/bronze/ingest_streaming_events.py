"""Bronze layer: Structured Streaming ingestion of simulated live EEG events.

This module simulates a real-time EEG event stream (e.g., from a wearable EEG device
or a hospital PSG system) using Spark's built-in `rate` source for development.
In production, this would use Kafka, Azure Event Hubs, or Kinesis as the source.

Databricks Exam Relevance (Domain 3 - Incremental Data Processing):
- Demonstrates readStream / writeStream pattern
- Watermarks for handling late-arriving data
- Window aggregations on event time
- Output modes: append (default), update, complete
- Trigger types: processingTime, once, availableNow, continuous

Research Relevance:
- Future extension: ingest real-time spindle/SO detection results from
  online EEG processing (e.g., Dreem headband API or BrainVision RDA protocol)
- The streaming Bronze table feeds a DLT pipeline for live quality monitoring
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

from src.utils.config import AppConfig, DEFAULT_CONFIG
from src.utils.logging import get_logger

log = get_logger(__name__)


# Schema for simulated live EEG events
LIVE_EEG_EVENT_SCHEMA = StructType([
    StructField("event_id",      StringType(),   nullable=False),
    StructField("subject_id",    StringType(),   nullable=False),
    StructField("event_time",    TimestampType(),nullable=False),
    StructField("event_type",    StringType(),   nullable=False),  # spindle/SO/artefact
    StructField("channel",       StringType(),   nullable=False),
    StructField("amplitude_uv",  DoubleType(),   nullable=True),
    StructField("duration_sec",  DoubleType(),   nullable=True),
    StructField("confidence",    DoubleType(),   nullable=True),   # 0.0 - 1.0
    StructField("sleep_stage",   StringType(),   nullable=True),   # N1/N2/N3/R/W
])


def create_simulated_eeg_stream(
    spark: SparkSession,
    rows_per_second: int = 10,
) -> DataFrame:
    """Create a simulated EEG event stream using Spark's rate source.

    The `rate` source generates monotonically increasing (timestamp, value) pairs
    at the specified rate. We transform these into plausible EEG event records.

    Exam note: `rate` source is the standard way to test streaming pipelines
    without a real message broker. In the exam, expect questions about:
    - `readStream.format('rate').option('rowsPerSecond', N)`
    - Difference between `rate` (wall clock) and `rate-micro-batch` sources

    Args:
        spark: Active SparkSession.
        rows_per_second: Number of synthetic events to generate per second.

    Returns:
        Streaming DataFrame with EEG event schema.
    """
    # Event types and channels to cycle through
    event_types = F.array(
        F.lit("spindle"), F.lit("slow_oscillation"),
        F.lit("k_complex"), F.lit("artefact")
    )
    channels = F.array(F.lit("Fpz-Cz"), F.lit("Pz-Oz"))
    stages = F.array(F.lit("N2"), F.lit("N3"), F.lit("N1"))
    subjects = F.array(*[F.lit(f"SC{4000 + i:04d}") for i in range(10)])

    return (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", rows_per_second)
        .load()
        # Transform synthetic `value` column into EEG event fields
        .withColumn("event_id",     F.concat(F.lit("evt_"), F.col("value").cast("string")))
        .withColumn("subject_id",   subjects[(F.col("value") % 10).cast("int")])
        .withColumn("event_time",   F.col("timestamp"))
        .withColumn("event_type",   event_types[(F.col("value") % 4).cast("int")])
        .withColumn("channel",      channels[(F.col("value") % 2).cast("int")])
        .withColumn("amplitude_uv", (F.rand() * 80 + 20).cast("double"))    # 20-100 µV
        .withColumn("duration_sec", (F.rand() * 2.0 + 0.5).cast("double")) # 0.5-2.5 sec
        .withColumn("confidence",   F.rand().cast("double"))
        .withColumn("sleep_stage",  stages[(F.col("value") % 3).cast("int")])
        .drop("timestamp", "value")
    )


def write_streaming_bronze(
    df_stream: DataFrame,
    cfg: AppConfig = DEFAULT_CONFIG,
    checkpoint_path: str = "/tmp/checkpoints/streaming_eeg_bronze",
    await_termination: bool = False,
) -> object:
    """Write streaming EEG events to Bronze Delta table with watermark.

    Watermark exam notes:
    - `withWatermark('event_time', '10 minutes')` tells Spark to wait up to
      10 minutes for late data before finalizing a window.
    - State beyond the watermark threshold is dropped.
    - Required for stateful streaming ops (window aggs, stream-stream joins).

    Output modes:
    - `append`: only new rows added. Compatible with watermarked windows.
    - `update`: changed rows upserted. Requires Delta or foreach sink.
    - `complete`: entire result table rewritten. Only for aggregations.

    Args:
        df_stream: Streaming DataFrame from create_simulated_eeg_stream().
        cfg: Application configuration.
        checkpoint_path: Checkpoint directory (must be cloud-backed in prod).
        await_termination: If True, block until the stream stops.

    Returns:
        StreamingQuery object.
    """
    target = f"{cfg.catalog.catalog}.{cfg.catalog.bronze_schema}.streaming_eeg_events"

    # Add watermark before writing
    # Exam: watermark MUST be applied to the streaming source DF, not the write side
    df_with_watermark = df_stream.withWatermark("event_time", "10 minutes")

    query = (
        df_with_watermark.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .toTable(target)
    )

    log.info(f"Streaming query started → {target}")
    if await_termination:
        query.awaitTermination()
    return query
