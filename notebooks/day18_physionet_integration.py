# Databricks notebook source
# MAGIC %md
# MAGIC # Day 18: PhysioNet Sleep-EDF Dataset Integration
# MAGIC
# MAGIC ## Professional Certification - Advanced Data Engineering
# MAGIC
# MAGIC ### **Learning Objectives:**
# MAGIC - Integrate PhysioNet Sleep-EDF Expanded dataset into lakehouse architecture
# MAGIC - Implement EDF file parsing and metadata extraction
# MAGIC - Design efficient partitioning strategy for time-series EEG data
# MAGIC - Build automated data ingestion pipelines for large-scale datasets
# MAGIC - Implement data validation and quality checks for medical data
# MAGIC - Create subject-level and recording-level metadata management
# MAGIC - Optimize storage and query performance for neuroscience research

# COMMAND ----------

# MAGIC %md
# MAGIC ## **PhysioNet Sleep-EDF Expanded Dataset:**
# MAGIC
# MAGIC The PhysioNet Sleep-EDF Expanded database contains:
# MAGIC - **197 whole-night polysomnographic sleep recordings** (N=200 subjects)
# MAGIC - **EEG channels**: Fpz-Cz and Pz-Oz (100 Hz sampling rate)
# MAGIC - **EOG**: horizontal EOG
# MAGIC - **EMG**: submental chin EMG
# MAGIC - **Expert annotations**: Sleep stages (W, N1, N2, N3, REM) in 30-second epochs
# MAGIC - **Demographics**: Age, gender, medication status
# MAGIC - **Study groups**: Healthy subjects (sleep-cassette) and subjects with mild sleep
# MAGIC   disorders (sleep-telemetry)

# COMMAND ----------

# Setup and imports
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyedflib
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DateType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

from src.utils.physionet_downloader import download_full_dataset, build_full_manifest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 0: Download the Complete Dataset
# MAGIC
# MAGIC Downloads all ~394 EDF files from PhysioNet to the Unity Catalog Volume.
# MAGIC Files already present are skipped (idempotent).  Set n_jobs=8 for a
# MAGIC 4-core cluster; increase for larger clusters.

# COMMAND ----------

DEST_DIR = "/Volumes/eeg_lakehouse/bronze/raw_edf"

download_stats = download_full_dataset(
    dest_dir=DEST_DIR,
    n_jobs=8,
    overwrite=False,   # skip files that already exist
)
print("Download complete:", download_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Dataset Discovery & Metadata Extraction

# COMMAND ----------

# Build the full ingestion manifest from the real downloaded files
file_manifest = build_full_manifest(base_dir=DEST_DIR)
print(f"Manifest size: {len(file_manifest)} recordings")
print("Sample:", file_manifest[:3])

# COMMAND ----------

def parse_edf_header(edf_path: str) -> dict | None:
    """Extract header information from EDF file."""
    try:
        with pyedflib.EdfReader(edf_path) as f:
            return {
                'patient_id':        f.getPatientCode(),
                'recording_id':      f.getRecordingAdditional(),
                'start_time':        f.getStartdatetime(),
                'duration_seconds':  f.file_duration,
                'num_signals':       f.signals_in_file,
                'sample_frequency':  f.getSampleFrequency(0) if f.signals_in_file > 0 else None,
                'signal_labels':     [f.getLabel(i) for i in range(f.signals_in_file)],
                'physical_min':      [f.getPhysicalMinimum(i) for i in range(f.signals_in_file)],
                'physical_max':      [f.getPhysicalMaximum(i) for i in range(f.signals_in_file)],
                'digital_min':       [f.getDigitalMinimum(i) for i in range(f.signals_in_file)],
                'digital_max':       [f.getDigitalMaximum(i) for i in range(f.signals_in_file)],
            }
    except Exception as exc:
        print(f"Error parsing EDF header for {edf_path}: {exc}")
        return None

# COMMAND ----------

def extract_eeg_signals(edf_path: str, channels=None) -> pd.DataFrame | None:
    """Extract specific EEG channels from EDF file."""
    if channels is None:
        channels = ['EEG Fpz-Cz', 'EEG Pz-Oz']
    try:
        with pyedflib.EdfReader(edf_path) as f:
            signal_labels = [f.getLabel(i) for i in range(f.signals_in_file)]
            duration = f.file_duration
            eeg_data = {}
            for channel in channels:
                if channel in signal_labels:
                    idx = signal_labels.index(channel)
                    eeg_data[channel] = f.readSignal(idx)
            if not eeg_data:
                print(f"No matching channels in {edf_path}")
                return None
            n_samples = len(next(iter(eeg_data.values())))
            timestamps = np.linspace(0, duration, n_samples)
            return pd.DataFrame({'timestamp_offset': timestamps, **eeg_data})
    except Exception as exc:
        print(f"Error extracting EEG signals from {edf_path}: {exc}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Sleep Stage Annotation Extraction

# COMMAND ----------

def parse_hypnogram(hypnogram_edf_path: str) -> pd.DataFrame | None:
    """Extract sleep stage annotations from hypnogram EDF file."""
    stage_map = {
        'Sleep stage W': 'W',
        'Sleep stage 1': 'N1',
        'Sleep stage 2': 'N2',
        'Sleep stage 3': 'N3',
        'Sleep stage 4': 'N3',
        'Sleep stage R': 'REM',
        'Sleep stage ?': 'Unknown',
        'Movement time': 'MT',
    }
    try:
        with pyedflib.EdfReader(hypnogram_edf_path) as f:
            annotations = f.readAnnotations()
        epochs = [
            {
                'epoch_start_sec':    onset,
                'epoch_duration_sec': duration,
                'sleep_stage':        stage_map.get(label, label),
                'raw_annotation':     label,
            }
            for onset, duration, label in zip(*annotations)
        ]
        return pd.DataFrame(epochs)
    except Exception as exc:
        print(f"Error parsing hypnogram {hypnogram_edf_path}: {exc}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Automated Ingestion Pipeline

# COMMAND ----------

recording_metadata_schema = StructType([
    StructField("recording_id",        StringType(),        False),
    StructField("subject_id",          StringType(),        False),
    StructField("night_number",        IntegerType(),       False),
    StructField("study_group",         StringType(),        True),
    StructField("recording_date",      TimestampType(),     True),
    StructField("duration_seconds",    DoubleType(),        True),
    StructField("num_signals",         IntegerType(),       True),
    StructField("sample_frequency",    IntegerType(),       True),
    StructField("signal_labels",       ArrayType(StringType()), True),
    StructField("edf_file_path",       StringType(),        True),
    StructField("hypnogram_file_path", StringType(),        True),
    StructField("ingestion_timestamp", TimestampType(),     True),
    StructField("file_size_bytes",     LongType(),          True),
])


def ingest_recording_metadata(recording_files: list) -> object:
    """Process list of EDF file dicts and create metadata records as Spark DF."""
    records = []
    for fi in recording_files:
        header = parse_edf_header(fi.get('edf_path', ''))
        if not header:
            continue
        study_group = 'cassette' if fi['subject_id'].startswith('SC') else 'telemetry'
        records.append({
            'recording_id':        fi['recording_id'],
            'subject_id':          fi['subject_id'],
            'night_number':        fi['night'],
            'study_group':         study_group,
            'recording_date':      header['start_time'],
            'duration_seconds':    float(header['duration_seconds']),
            'num_signals':         header['num_signals'],
            'sample_frequency':    int(header['sample_frequency']) if header['sample_frequency'] else None,
            'signal_labels':       header['signal_labels'],
            'edf_file_path':       fi.get('edf_path'),
            'hypnogram_file_path': fi.get('hypnogram_path'),
            'ingestion_timestamp': datetime.now(),
            'file_size_bytes':     os.path.getsize(fi['edf_path']) if os.path.exists(fi.get('edf_path', '')) else None,
        })
    return spark.createDataFrame(records, schema=recording_metadata_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Signal Data Storage Strategy

# COMMAND ----------

eeg_signal_schema = StructType([
    StructField("recording_id",    StringType(),    False),
    StructField("subject_id",      StringType(),    False),
    StructField("channel_name",    StringType(),    False),
    StructField("sample_timestamp", TimestampType(), False),
    StructField("sample_value",    DoubleType(),    False),
    StructField("sample_index",    LongType(),      False),
    StructField("partition_date",  DateType(),      False),
])


def store_eeg_signals_delta(recording_id, subject_id, edf_path, recording_start_time, target_table):
    """Extract EEG signals and store in Delta Lake with efficient partitioning."""
    signals_df = extract_eeg_signals(edf_path)
    if signals_df is None or signals_df.empty:
        print(f"No signals extracted for {recording_id}")
        return

    base_ts = recording_start_time or datetime.now()
    records = []
    for idx, row in signals_df.iterrows():
        ts = base_ts + timedelta(seconds=float(row['timestamp_offset']))
        for channel in signals_df.columns:
            if channel != 'timestamp_offset':
                records.append({
                    'recording_id':    recording_id,
                    'subject_id':      subject_id,
                    'channel_name':    channel,
                    'sample_timestamp': ts,
                    'sample_value':    float(row[channel]),
                    'sample_index':    int(idx),
                    'partition_date':  ts.date(),
                })

    spark.createDataFrame(records, schema=eeg_signal_schema) \
        .write.format("delta").mode("append") \
        .partitionBy("partition_date", "subject_id") \
        .saveAsTable(target_table)
    print(f"✅ Stored {len(records)} signal samples for {recording_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Sleep Stage Annotation Storage

# COMMAND ----------

sleep_stage_schema = StructType([
    StructField("recording_id",      StringType(),    False),
    StructField("subject_id",        StringType(),    False),
    StructField("epoch_number",      IntegerType(),   False),
    StructField("epoch_start_time",  TimestampType(), False),
    StructField("epoch_duration_sec", DoubleType(),   False),
    StructField("sleep_stage",       StringType(),    False),
    StructField("stage_numeric",     IntegerType(),   True),
    StructField("raw_annotation",    StringType(),    True),
])

_STAGE_NUMERIC = {'W': -1, 'N1': 0, 'N2': 1, 'N3': 2, 'REM': 3, 'Unknown': None, 'MT': None}


def store_sleep_annotations(recording_id, subject_id, hypnogram_path, recording_start_time, target_table):
    """Extract and store sleep stage annotations."""
    annotations_df = parse_hypnogram(hypnogram_path)
    if annotations_df is None or annotations_df.empty:
        print(f"No annotations found for {recording_id}")
        return

    base_ts = recording_start_time or datetime.now()
    records = [
        {
            'recording_id':       recording_id,
            'subject_id':         subject_id,
            'epoch_number':       int(idx),
            'epoch_start_time':   base_ts + timedelta(seconds=float(row['epoch_start_sec'])),
            'epoch_duration_sec': float(row['epoch_duration_sec']),
            'sleep_stage':        row['sleep_stage'],
            'stage_numeric':      _STAGE_NUMERIC.get(row['sleep_stage']),
            'raw_annotation':     row['raw_annotation'],
        }
        for idx, row in annotations_df.iterrows()
    ]
    spark.createDataFrame(records, schema=sleep_stage_schema) \
        .write.format("delta").mode("append") \
        .partitionBy("subject_id") \
        .saveAsTable(target_table)
    print(f"✅ Stored {len(records)} annotations for {recording_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Subject Demographics & Metadata

# COMMAND ----------

def create_subject_demographics_table():
    """Create the subject demographics table if it does not exist."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS eeg_lakehouse.bronze.subject_demographics (
            subject_id          STRING,
            age_years           INT,
            gender              STRING,
            study_group         STRING,
            medication_status   STRING,
            sleep_disorder_type STRING,
            num_recordings      INT,
            first_recording_date TIMESTAMP,
            last_recording_date  TIMESTAMP,
            notes               STRING,
            created_at          TIMESTAMP,
            updated_at          TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (study_group)
    """)
    print("✅ Subject demographics table ready")

create_subject_demographics_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Data Quality Validation

# COMMAND ----------

def validate_eeg_recording(recording_id, signals_table, annotations_table):
    """Perform data quality checks on ingested recording."""
    print(f"\nValidating recording: {recording_id}")
    print("=" * 60)

    signal_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {signals_table}
        WHERE recording_id = '{recording_id}'
    """).collect()[0]['cnt']
    print(f"Total signal samples: {signal_count:,}")

    num_channels = spark.sql(f"""
        SELECT COUNT(DISTINCT channel_name) AS channels FROM {signals_table}
        WHERE recording_id = '{recording_id}'
    """).collect()[0]['channels']
    print(f"Number of channels: {num_channels}")

    annotation_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {annotations_table}
        WHERE recording_id = '{recording_id}'
    """).collect()[0]['cnt']
    expected_epochs = int((8 * 3600) / 30)
    print(f"Sleep stage epochs: {annotation_count}  (expected ~{expected_epochs} for 8-hour recording)")

    spark.sql(f"""
        SELECT channel_name,
               MIN(sample_value)   AS min_val,
               MAX(sample_value)   AS max_val,
               AVG(sample_value)   AS mean_val,
               STDDEV(sample_value) AS std_val
        FROM {signals_table}
        WHERE recording_id = '{recording_id}'
        GROUP BY channel_name
    """).show(truncate=False)

    spark.sql(f"""
        SELECT sleep_stage,
               COUNT(*) AS epoch_count,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM {annotations_table}
        WHERE recording_id = '{recording_id}'
        GROUP BY sleep_stage ORDER BY epoch_count DESC
    """).show()

    return {
        'recording_id':      recording_id,
        'signal_count':      signal_count,
        'annotation_count':  annotation_count,
        'num_channels':      num_channels,
        'validation_passed': signal_count > 0 and annotation_count > 0,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Batch Ingestion Orchestration — Full Dataset

# COMMAND ----------

METADATA_TABLE   = "eeg_lakehouse.bronze.recording_metadata"
SIGNALS_TABLE    = "eeg_lakehouse.bronze.eeg_signals"
ANNOTATIONS_TABLE = "eeg_lakehouse.bronze.sleep_annotations"


def orchestrate_full_dataset_ingestion(
    file_manifest,
    metadata_table=METADATA_TABLE,
    signals_table=SIGNALS_TABLE,
    annotations_table=ANNOTATIONS_TABLE,
):
    """Orchestrate ingestion of entire PhysioNet dataset.

    All previously commented-out steps are now ACTIVE.
    """
    print(f"Starting PhysioNet Sleep-EDF ingestion — {len(file_manifest)} recordings")
    stats = {'total': len(file_manifest), 'successful': 0, 'failed': 0}
    t0 = datetime.now()

    for file_info in file_manifest:
        recording_id = file_info['recording_id']
        try:
            print(f"\nProcessing: {recording_id}")

            # Step 1: ingest metadata
            meta_df = ingest_recording_metadata([file_info])
            meta_df.write.format("delta").mode("append").saveAsTable(metadata_table)

            # Back-fill recording_start_time from EDF header so signals/annotations
            # receive the correct absolute timestamps.
            header = parse_edf_header(file_info['edf_path'])
            recording_start = header['start_time'] if header else datetime.now()
            file_info['recording_start_time'] = recording_start

            # Step 2: ingest EEG signals
            store_eeg_signals_delta(
                recording_id=recording_id,
                subject_id=file_info['subject_id'],
                edf_path=file_info['edf_path'],
                recording_start_time=recording_start,
                target_table=signals_table,
            )

            # Step 3: ingest sleep stage annotations
            store_sleep_annotations(
                recording_id=recording_id,
                subject_id=file_info['subject_id'],
                hypnogram_path=file_info['hypnogram_path'],
                recording_start_time=recording_start,
                target_table=annotations_table,
            )

            # Step 4: validate
            result = validate_eeg_recording(recording_id, signals_table, annotations_table)
            if not result['validation_passed']:
                print(f"⚠️  Validation warning for {recording_id}")

            stats['successful'] += 1

        except Exception as exc:
            print(f"❌ Error processing {recording_id}: {exc}")
            stats['failed'] += 1

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n{'='*60}")
    print(f"Ingestion complete: {stats['successful']}/{stats['total']} successful, "
          f"{stats['failed']} failed, {elapsed:.0f}s elapsed")
    return stats


# Run full ingestion
ingestion_stats = orchestrate_full_dataset_ingestion(file_manifest)
print(ingestion_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Query Optimisation & Indexing

# COMMAND ----------

def optimize_physionet_tables():
    """Run OPTIMIZE + ZORDER + VACUUM on all PhysioNet tables."""
    tables = [
        METADATA_TABLE,
        SIGNALS_TABLE,
        ANNOTATIONS_TABLE,
        "eeg_lakehouse.bronze.subject_demographics",
    ]
    for table in tables:
        print(f"\nOptimizing {table}...")
        spark.sql(f"OPTIMIZE {table}")
        if "eeg_signals" in table:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (subject_id, channel_name)")
        elif "sleep_annotations" in table:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (subject_id, epoch_number)")
        elif "recording_metadata" in table:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (subject_id, study_group)")
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
        print(f"✅ {table} optimized")

optimize_physionet_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 11: Integration with Research Pipeline

# COMMAND ----------

def create_research_views():
    """Create curated Gold-layer views for neuroscience research."""
    spark.sql("""
        CREATE OR REPLACE VIEW eeg_lakehouse.gold.subject_sleep_metrics AS
        SELECT
            sa.subject_id,
            COUNT(DISTINCT sa.recording_id)                            AS total_recordings,
            SUM(sa.epoch_duration_sec) / 3600.0                        AS total_sleep_hours,
            SUM(CASE WHEN sa.sleep_stage = 'REM'  THEN sa.epoch_duration_sec ELSE 0 END)
                / NULLIF(SUM(sa.epoch_duration_sec), 0) * 100          AS pct_rem,
            SUM(CASE WHEN sa.sleep_stage = 'N3'   THEN sa.epoch_duration_sec ELSE 0 END)
                / NULLIF(SUM(sa.epoch_duration_sec), 0) * 100          AS pct_n3,
            SUM(CASE WHEN sa.sleep_stage = 'N2'   THEN sa.epoch_duration_sec ELSE 0 END)
                / NULLIF(SUM(sa.epoch_duration_sec), 0) * 100          AS pct_n2,
            SUM(CASE WHEN sa.sleep_stage = 'N1'   THEN sa.epoch_duration_sec ELSE 0 END)
                / NULLIF(SUM(sa.epoch_duration_sec), 0) * 100          AS pct_n1,
            SUM(CASE WHEN sa.sleep_stage = 'W'    THEN sa.epoch_duration_sec ELSE 0 END)
                / NULLIF(SUM(sa.epoch_duration_sec), 0) * 100          AS pct_wake
        FROM eeg_lakehouse.bronze.sleep_annotations sa
        GROUP BY sa.subject_id
    """)
    print("✅ Gold view eeg_lakehouse.gold.subject_sleep_metrics created")

create_research_views()
