# Databricks notebook source
# MAGIC %md
# MAGIC # Day 18: PhysioNet Sleep-EDF Dataset Integration
# MAGIC 
# MAGIC ## Professional Certification - Advanced Data Engineering
# MAGIC 
# MAGIC ## MAGIC
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
# MAGIC ## **PhysioNet Sleep-EDF Dataset:**
# MAGIC 
# MAGIC The PhysioNet Sleep-EDF Expanded database contains:
# MAGIC - **197 whole-night polysomnographic sleep recordings** (N=200 subjects)
# MAGIC - **EEG channels**: Fpz-Cz and Pz-Oz (100 Hz sampling rate)
# MAGIC - **EOG**: horizontal EOG
# MAGIC - **EMG**: submental chin EMG
# MAGIC - **Expert annotations**: Sleep stages (W, N1, N2, N3, REM) in 30-second epochs
# MAGIC - **Demographics**: Age, gender, medication status
# MAGIC - **Study groups**: Healthy subjects and subjects with mild sleep disorders

# COMMAND ----------

# Setup and imports
import requests
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pyedflib
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Dataset Discovery & Metadata Extraction

# COMMAND ----------

# PhysioNet Sleep-EDF dataset information
PHYSIONET_BASE_URL = "https://physionet.org/files/sleep-edfx/1.0.0/"

# Dataset structure
SLEEP_CASSETTE_DIR = "sleep-cassette/"  # Healthy subjects, hospital recordings
SLEEP_TELEMETRY_DIR = "sleep-telemetry/"  # Sleep disorder subjects, home recordings

def get_dataset_file_list():
    """
    Generate list of files in PhysioNet Sleep-EDF Expanded dataset
    In production, this would query the PhysioNet API or S3 bucket
    """
    # Sample file structure
    files = [
        # Sleep Cassette (SC) - Hospital recordings
        {'subject_id': 'SC4001', 'night': 1, 'type': 'PSG', 'file': 'SC4001E0-PSG.edf'},
        {'subject_id': 'SC4001', 'night': 1, 'type': 'Hypnogram', 'file': 'SC4001EC-Hypnogram.edf'},
        {'subject_id': 'SC4002', 'night': 1, 'type': 'PSG', 'file': 'SC4002E0-PSG.edf'},
        {'subject_id': 'SC4002', 'night': 1, 'type': 'Hypnogram', 'file': 'SC4002EC-Hypnogram.edf'},
        # Sleep Telemetry (ST) - Home recordings
        {'subject_id': 'ST7011', 'night': 1, 'type': 'PSG', 'file': 'ST7011J0-PSG.edf'},
        {'subject_id': 'ST7011', 'night': 1, 'type': 'Hypnogram', 'file': 'ST7011JP-Hypnogram.edf'},
    ]
    
    return spark.createDataFrame(files)

# file_list_df = get_dataset_file_list()
# file_list_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: EDF File Parsing & Signal Extraction

# COMMAND ----------

def parse_edf_header(edf_path):
    """
    Extract header information from EDF file
    """
    try:
        with pyedflib.EdfReader(edf_path) as f:
            header = {
                'patient_id': f.getPatientCode(),
                'recording_id': f.getRecordingAdditional(),
                'start_time': f.getStartdatetime(),
                'duration_seconds': f.file_duration,
                'num_signals': f.signals_in_file,
                'sample_frequency': f.getSampleFrequency(0) if f.signals_in_file > 0 else None,
                'signal_labels': [f.getLabel(i) for i in range(f.signals_in_file)],
                'physical_min': [f.getPhysicalMinimum(i) for i in range(f.signals_in_file)],
                'physical_max': [f.getPhysicalMaximum(i) for i in range(f.signals_in_file)],
                'digital_min': [f.getDigitalMinimum(i) for i in range(f.signals_in_file)],
                'digital_max': [f.getDigitalMaximum(i) for i in range(f.signals_in_file)],
            }
            return header
    except Exception as e:
        print(f"Error parsing EDF header: {e}")
        return None

# Example usage (would run on driver node with sample file)
# header_info = parse_edf_header('/dbfs/mnt/eeg_data/SC4001E0-PSG.edf')
# print(header_info)

# COMMAND ----------

def extract_eeg_signals(edf_path, channels=['EEG Fpz-Cz', 'EEG Pz-Oz']):
    """
    Extract specific EEG channels from EDF file
    Returns DataFrame with timestamp and signal values
    """
    try:
        with pyedflib.EdfReader(edf_path) as f:
            signal_labels = [f.getLabel(i) for i in range(f.signals_in_file)]
            sample_freq = f.getSampleFrequency(0)
            
            eeg_data = {}
            for channel in channels:
                if channel in signal_labels:
                    channel_idx = signal_labels.index(channel)
                    signal = f.readSignal(channel_idx)
                    eeg_data[channel] = signal
            
            # Create timestamps
            duration = f.file_duration
            num_samples = len(next(iter(eeg_data.values())))
            timestamps = np.linspace(0, duration, num_samples)
            
            # Convert to DataFrame
            df_data = {'timestamp_offset': timestamps}
            df_data.update(eeg_data)
            
            return pd.DataFrame(df_data)
    
    except Exception as e:
        print(f"Error extracting EEG signals: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Sleep Stage Annotation Extraction

# COMMAND ----------

def parse_hypnogram(hypnogram_edf_path):
    """
    Extract sleep stage annotations from hypnogram EDF file
    Returns DataFrame with epoch start time and sleep stage
    """
    try:
        with pyedflib.EdfReader(hypnogram_edf_path) as f:
            # Hypnogram files contain annotations
            annotations = f.readAnnotations()
            
            # Parse annotations into structured format
            epochs = []
            for onset, duration, label in zip(*annotations):
                # Sleep stages: Sleep stage W, Sleep stage 1, 2, 3, 4, R (REM)
                stage_map = {
                    'Sleep stage W': 'W',  # Wake
                    'Sleep stage 1': 'N1',
                    'Sleep stage 2': 'N2',
                    'Sleep stage 3': 'N3',
                    'Sleep stage 4': 'N3',  # N3 combines old stages 3 and 4
                    'Sleep stage R': 'REM',
                    'Sleep stage ?': 'Unknown',
                    'Movement time': 'MT'
                }
                
                stage = stage_map.get(label, label)
                
                epochs.append({
                    'epoch_start_sec': onset,
                    'epoch_duration_sec': duration,
                    'sleep_stage': stage,
                    'raw_annotation': label
                })
            
            return pd.DataFrame(epochs)
    
    except Exception as e:
        print(f"Error parsing hypnogram: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Automated Ingestion Pipeline

# COMMAND ----------

# Define schema for recording metadata
recording_metadata_schema = StructType([
    StructField("recording_id", StringType(), False),
    StructField("subject_id", StringType(), False),
    StructField("night_number", IntegerType(), False),
    StructField("study_group", StringType(), True),  # 'cassette' or 'telemetry'
    StructField("recording_date", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("num_signals", IntegerType(), True),
    StructField("sample_frequency", IntegerType(), True),
    StructField("signal_labels", ArrayType(StringType()), True),
    StructField("edf_file_path", StringType(), True),
    StructField("hypnogram_file_path", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("file_size_bytes", LongType(), True)
])

# COMMAND ----------

def ingest_recording_metadata(recording_files):
    """
    Process list of EDF files and create metadata records
    """
    metadata_records = []
    
    for file_info in recording_files:
        subject_id = file_info['subject_id']
        edf_path = file_info.get('edf_path')
        
        # Parse EDF header
        header = parse_edf_header(edf_path)
        
        if header:
            # Determine study group from subject ID
            study_group = 'cassette' if subject_id.startswith('SC') else 'telemetry'
            
            record = {
                'recording_id': f"{subject_id}_night{file_info['night']}",
                'subject_id': subject_id,
                'night_number': file_info['night'],
                'study_group': study_group,
                'recording_date': header['start_time'],
                'duration_seconds': header['duration_seconds'],
                'num_signals': header['num_signals'],
                'sample_frequency': int(header['sample_frequency']) if header['sample_frequency'] else None,
                'signal_labels': header['signal_labels'],
                'edf_file_path': edf_path,
                'hypnogram_file_path': file_info.get('hypnogram_path'),
                'ingestion_timestamp': datetime.now(),
                'file_size_bytes': os.path.getsize(edf_path) if os.path.exists(edf_path) else None
            }
            
            metadata_records.append(record)
    
    # Create DataFrame
    metadata_df = spark.createDataFrame(metadata_records, schema=recording_metadata_schema)
    
    return metadata_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Signal Data Storage Strategy

# COMMAND ----------

# Define schema for time-series EEG data
eeg_signal_schema = StructType([
    StructField("recording_id", StringType(), False),
    StructField("subject_id", StringType(), False),
    StructField("channel_name", StringType(), False),
    StructField("sample_timestamp", TimestampType(), False),
    StructField("sample_value", DoubleType(), False),
    StructField("sample_index", LongType(), False),
    StructField("partition_date", DateType(), False)  # For efficient partitioning
])

# COMMAND ----------

def store_eeg_signals_delta(recording_id, subject_id, edf_path, target_table):
    """
    Extract EEG signals and store in Delta Lake with efficient partitioning
    """
    # Extract signals
    signals_df = extract_eeg_signals(edf_path)
    
    if signals_df is None or signals_df.empty:
        print(f"No signals extracted for {recording_id}")
        return
    
    # Convert to Spark DataFrame with proper schema
    # Melt the DataFrame to long format for better Delta Lake performance
    signal_records = []
    
    base_timestamp = datetime.now()  # In production, use actual recording start time
    
    for idx, row in signals_df.iterrows():
        timestamp = base_timestamp + timedelta(seconds=row['timestamp_offset'])
        
        for channel in signals_df.columns:
            if channel != 'timestamp_offset':
                signal_records.append({
                    'recording_id': recording_id,
                    'subject_id': subject_id,
                    'channel_name': channel,
                    'sample_timestamp': timestamp,
                    'sample_value': float(row[channel]),
                    'sample_index': idx,
                    'partition_date': timestamp.date()
                })
    
    # Create Spark DataFrame
    spark_df = spark.createDataFrame(signal_records, schema=eeg_signal_schema)
    
    # Write to Delta Lake with partitioning
    spark_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("partition_date", "subject_id") \
        .saveAsTable(target_table)
    
    print(f"✅ Stored {len(signal_records)} signal samples for {recording_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Sleep Stage Annotation Storage

# COMMAND ----------

# Schema for sleep stage annotations
sleep_stage_schema = StructType([
    StructField("recording_id", StringType(), False),
    StructField("subject_id", StringType(), False),
    StructField("epoch_number", IntegerType(), False),
    StructField("epoch_start_time", TimestampType(), False),
    StructField("epoch_duration_sec", DoubleType(), False),
    StructField("sleep_stage", StringType(), False),
    StructField("stage_numeric", IntegerType(), True),  # -1=W, 0=N1, 1=N2, 2=N3, 3=REM
    StructField("raw_annotation", StringType(), True)
])

# COMMAND ----------

def store_sleep_annotations(recording_id, subject_id, hypnogram_path, recording_start_time, target_table):
    """
    Extract and store sleep stage annotations
    """
    # Parse hypnogram
    annotations_df = parse_hypnogram(hypnogram_path)
    
    if annotations_df is None or annotations_df.empty:
        print(f"No annotations found for {recording_id}")
        return
    
    # Convert to structured format
    stage_numeric_map = {
        'W': -1,
        'N1': 0,
        'N2': 1,
        'N3': 2,
        'REM': 3,
        'Unknown': None,
        'MT': None
    }
    
    annotation_records = []
    for idx, row in annotations_df.iterrows():
        epoch_start = recording_start_time + timedelta(seconds=row['epoch_start_sec'])
        
        annotation_records.append({
            'recording_id': recording_id,
            'subject_id': subject_id,
            'epoch_number': idx,
            'epoch_start_time': epoch_start,
            'epoch_duration_sec': row['epoch_duration_sec'],
            'sleep_stage': row['sleep_stage'],
            'stage_numeric': stage_numeric_map.get(row['sleep_stage']),
            'raw_annotation': row['raw_annotation']
        })
    
    # Create Spark DataFrame
    spark_df = spark.createDataFrame(annotation_records, schema=sleep_stage_schema)
    
    # Write to Delta table
    spark_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("subject_id") \
        .saveAsTable(target_table)
    
    print(f"✅ Stored {len(annotation_records)} sleep stage annotations for {recording_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Subject Demographics & Metadata

# COMMAND ----------

# Create subject demographics table
def create_subject_demographics_table():
    """
    Create and populate subject demographics table
    Based on PhysioNet Sleep-EDF documentation
    """
    spark.sql("""
    CREATE TABLE IF NOT EXISTS eeg_lakehouse.bronze.subject_demographics (
        subject_id STRING,
        age_years INT,
        gender STRING,
        study_group STRING,  -- 'cassette' or 'telemetry'
        medication_status STRING,
        sleep_disorder_type STRING,
        num_recordings INT,
        first_recording_date TIMESTAMP,
        last_recording_date TIMESTAMP,
        notes STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (study_group)
    """)
    
    print("✅ Subject demographics table created")

# create_subject_demographics_table()

# COMMAND ----------

# Sample demographics data (would come from PhysioNet documentation)
subject_demographics_sample = [
    {
        'subject_id': 'SC4001',
        'age_years': 35,
        'gender': 'F',
        'study_group': 'cassette',
        'medication_status': 'None',
        'sleep_disorder_type': None,
        'num_recordings': 2,
        'created_at': datetime.now()
    },
    {
        'subject_id': 'ST7011',
        'age_years': 42,
        'gender': 'M',
        'study_group': 'telemetry',
        'medication_status': 'Sleep medication',
        'sleep_disorder_type': 'Mild insomnia',
        'num_recordings': 2,
        'created_at': datetime.now()
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Data Quality Validation

# COMMAND ----------

def validate_eeg_recording(recording_id, signals_table, annotations_table):
    """
    Perform data quality checks on ingested recording
    """
    print(f"\nValidating recording: {recording_id}")
    print("="*60)
    
    # Check 1: Signal data completeness
    signal_count = spark.sql(f"""
        SELECT COUNT(*) as count
        FROM {signals_table}
        WHERE recording_id = '{recording_id}'
    """).collect()[0]['count']
    
    print(f"Total signal samples: {signal_count:,}")
    
    # Check 2: Expected sampling rate
    expected_samples_per_channel = 100 * 60 * 60 * 8  # 100Hz * 8 hours
    num_channels = spark.sql(f"""
        SELECT COUNT(DISTINCT channel_name) as channels
        FROM {signals_table}
        WHERE recording_id = '{recording_id}'
    """).collect()[0]['channels']
    
    print(f"Number of channels: {num_channels}")
    
    # Check 3: Annotations coverage
    annotation_count = spark.sql(f"""
        SELECT COUNT(*) as count
        FROM {annotations_table}
        WHERE recording_id = '{recording_id}'
    """).collect()[0]['count']
    
    print(f"Sleep stage epochs: {annotation_count}")
    expected_epochs = int((8 * 60 * 60) / 30)  # 8 hours / 30-second epochs
    print(f"Expected epochs (~8 hours): {expected_epochs}")
    
    # Check 4: Signal value ranges (detect anomalies)
    signal_stats = spark.sql(f"""
        SELECT 
            channel_name,
            MIN(sample_value) as min_value,
            MAX(sample_value) as max_value,
            AVG(sample_value) as mean_value,
            STDDEV(sample_value) as std_value
        FROM {signals_table}
        WHERE recording_id = '{recording_id}'
        GROUP BY channel_name
    """)
    
    print("\nSignal statistics:")
    signal_stats.show(truncate=False)
    
    # Check 5: Sleep stage distribution
    stage_distribution = spark.sql(f"""
        SELECT 
            sleep_stage,
            COUNT(*) as epoch_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM {annotations_table}
        WHERE recording_id = '{recording_id}'
        GROUP BY sleep_stage
        ORDER BY epoch_count DESC
    """)
    
    print("\nSleep stage distribution:")
    stage_distribution.show()
    
    return {
        'recording_id': recording_id,
        'signal_count': signal_count,
        'annotation_count': annotation_count,
        'num_channels': num_channels,
        'validation_passed': signal_count > 0 and annotation_count > 0
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Batch Ingestion Orchestration

# COMMAND ----------

def orchestrate_full_dataset_ingestion(
    file_manifest,
    metadata_table="eeg_lakehouse.bronze.recording_metadata",
    signals_table="eeg_lakehouse.bronze.eeg_signals",
    annotations_table="eeg_lakehouse.bronze.sleep_annotations"
):
    """
    Orchestrate ingestion of entire PhysioNet dataset
    """
    print("Starting PhysioNet Sleep-EDF dataset ingestion...")
    print(f"Total files to process: {len(file_manifest)}")
    
    ingestion_stats = {
        'total_files': len(file_manifest),
        'successful': 0,
        'failed': 0,
        'total_duration': 0
    }
    
    start_time = datetime.now()
    
    for file_info in file_manifest:
        try:
            recording_id = file_info['recording_id']
            print(f"\nProcessing: {recording_id}")
            
            # Step 1: Ingest metadata
            # metadata_record = ingest_recording_metadata([file_info])
            # metadata_record.write.format("delta").mode("append").saveAsTable(metadata_table)
            
            # Step 2: Ingest EEG signals
            # store_eeg_signals_delta(
            #     recording_id=recording_id,
            #     subject_id=file_info['subject_id'],
            #     edf_path=file_info['edf_path'],
            #     target_table=signals_table
            # )
            
            # Step 3: Ingest sleep annotations
            # store_sleep_annotations(
            #     recording_id=recording_id,
            #     subject_id=file_info['subject_id'],
            #     hypnogram_path=file_info['hypnogram_path'],
            #     recording_start_time=file_info['recording_start_time'],
            #     target_table=annotations_table
            # )
            
            # Step 4: Validate
            # validation_result = validate_eeg_recording(recording_id, signals_table, annotations_table)
            
            ingestion_stats['successful'] += 1
            
        except Exception as e:
            print(f"❌ Error processing {recording_id}: {e}")
            ingestion_stats['failed'] += 1
    
    end_time = datetime.now()
    ingestion_stats['total_duration'] = (end_time - start_time).total_seconds()
    
    print("\n" + "="*60)
    print("Ingestion Summary:")
    print(f"Total files: {ingestion_stats['total_files']}")
    print(f"Successful: {ingestion_stats['successful']}")
    print(f"Failed: {ingestion_stats['failed']}")
    print(f"Duration: {ingestion_stats['total_duration']:.2f} seconds")
    
    return ingestion_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Query Optimization & Indexing

# COMMAND ----------

# Optimize Delta tables for query performance
def optimize_physionet_tables():
    """
    Run optimization commands on PhysioNet tables
    """
    tables = [
        "eeg_lakehouse.bronze.recording_metadata",
        "eeg_lakehouse.bronze.eeg_signals",
        "eeg_lakehouse.bronze.sleep_annotations",
        "eeg_lakehouse.bronze.subject_demographics"
    ]
    
    for table in tables:
        print(f"\nOptimizing {table}...")
        
        # Optimize and Z-order
        spark.sql(f"OPTIMIZE {table}")
        
        # Z-order by commonly queried columns
        if "eeg_signals" in table:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (subject_id, channel_name)")
        elif "sleep_annotations" in table:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (subject_id, epoch_number)")
        elif "recording_metadata" in table:
            spark.sql(f"OPTIMIZE {table} ZORDER BY (subject_id, study_group)")
        
        # Vacuum old versions (retain 7 days)
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
        
        print(f"✅ {table} optimized")

# optimize_physionet_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 11: Integration with Research Pipeline

# COMMAND ----------

# Create Gold-layer views for research analysis
def create_research_views():
    """
    Create curated views for neuroscience research
    """
    # View 1: Subject-level sleep quality metrics
    spark.sql("""
    CREATE OR REPLACE VIEW eeg_lakehouse.gold.subject_sleep_metrics AS
    SELECT 
        s.subject_id,
        d.age_years,
        d.gender,
        d.study_group,
        COUNT(DISTINCT s.recording_id) as num_recordings,
        AVG(CASE WHEN s.sleep_stage = 'N3' THEN s.epoch_duration_sec ELSE 0 END) as avg_deep_sleep_sec,
        AVG(CASE WHEN s.sleep_stage = 'REM' THEN s.epoch_duration_sec ELSE 0 END) as avg_rem_sleep_sec,
        AVG(CASE WHEN s.sleep_stage = 'W' THEN s.epoch_duration_sec ELSE 0 END) as avg_wake_sec,
        SUM(s.epoch_duration_sec) / 3600.0 as total_sleep_hours
    FROM eeg_lakehouse.bronze.sleep_annotations s
    JOIN eeg_lakehouse.bronze.subject_demographics d ON s.subject_id = d.subject_id
    GROUP BY s.subject_id, d.age_years, d.gender, d.study_group
    """)
    
    # View 2: Recording-level summary
    spark.sql("""
    CREATE OR REPLACE VIEW eeg_lakehouse.gold.recording_summary AS
    SELECT 
        m.recording_id,
        m.subject_id,
        m.night_number,
        m.recording_date,
        m.duration_seconds / 3600.0 as duration_hours,
        COUNT(DISTINCT a.sleep_stage) as distinct_sleep_stages,
        MAX(CASE WHEN a.sleep_stage = 'N3' THEN 1 ELSE 0 END) as has_deep_sleep,
        MAX(CASE WHEN a.sleep_stage = 'REM' THEN 1 ELSE 0 END) as has_rem_sleep
    FROM eeg_lakehouse.bronze.recording_metadata m
    LEFT JOIN eeg_lakehouse.bronze.sleep_annotations a ON m.recording_id = a.recording_id
    GROUP BY m.recording_id, m.subject_id, m.night_number, m.recording_date, m.duration_seconds
    """)
    
    print("✅ Research views created")

# create_research_views()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 12: Hands-On Exercises
# MAGIC 
# MAGIC ### Exercise 1: Custom EDF Parser
# MAGIC - Write a UDF to parse EDF files in parallel across cluster
# MAGIC - Handle different EDF+ variants
# MAGIC - Extract custom annotations
# MAGIC 
# MAGIC ### Exercise 2: Data Quality Dashboard
# MAGIC - Build monitoring dashboard for ingestion pipeline
# MAGIC - Track data completeness by subject
# MAGIC - Identify recordings with anomalies
# MAGIC 
# MAGIC ### Exercise 3: Advanced Queries
# MAGIC - Find subjects with longest REM sleep duration
# MAGIC - Correlate sleep stages with age and gender
# MAGIC - Identify sleep architecture patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this notebook, you learned:
# MAGIC 
# MAGIC ✅ PhysioNet Sleep-EDF dataset structure and contents  
# MAGIC ✅ EDF file parsing and signal extraction techniques  
# MAGIC ✅ Efficient partitioning strategies for time-series data  
# MAGIC ✅ Automated ingestion pipelines for large-scale datasets  
# MAGIC ✅ Data quality validation for medical research data  
# MAGIC ✅ Subject and recording metadata management  
# MAGIC ✅ Storage optimization for neuroscience research  
# MAGIC ✅ Integration with lakehouse architecture (Bronze → Silver → Gold)  
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC - Day 19: Security & Compliance Patterns
# MAGIC - Day 20: Advanced TDA Algorithms (Persistent Homology)
# MAGIC - Day 21: End-to-End Pipeline Integration
