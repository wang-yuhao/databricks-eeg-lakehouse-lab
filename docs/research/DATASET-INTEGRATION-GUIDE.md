# 📊 PhysioNet Sleep-EDF Dataset Integration Guide

> **Transform Real Sleep EEG Data into Production Delta Lake Pipeline**
>
> **Dataset**: PhysioNet Sleep-EDF Database Expanded v1.0.0  
> **Size**: 8.1 GB | **Subjects**: 197 | **Format**: European Data Format (EDF)  
> **URL**: https://physionet.org/files/sleep-edfx/1.0.0/

---

## 🎯 Quick Start (5 Minutes)

### Step 1: Download Sample Data (1 Subject)
```bash
# Create download script
python scripts/download_sample_data.py --subjects 1 --output /dbfs/mnt/raw-data/

# Expected output:
# Downloaded: SC4001E0-PSG.edf (23.7 MB)
# Downloaded: SC4001EC-Hypnogram.edf (8.5 KB)
```

### Step 2: Ingest to Bronze
```python
# Run Bronze ingestion notebook
%run ./notebooks/day18_edf_to_delta_pipeline

# Check results
spark.table("sleep_eeg_catalog.bronze.raw_eeg_files").display()
```

### Step 3: Verify Pipeline
```sql
SELECT 
  subject_id,
  recording_date,
  duration_hours,
  n_channels,
  sampling_freq_hz
FROM sleep_eeg_catalog.bronze.eeg_metadata
LIMIT 5;
```

---

## 📚 Dataset Details

### PhysioNet Sleep-EDF Expanded

**Source**: https://physionet.org/content/sleep-edfx/1.0.0/

**Citation**:
```
Kemp B, Zwinderman AH, Tuk B, Kamphuisen HAC, Oberye JJL. 
Analysis of a sleep-dependent neuronal feedback loop: the slow-wave 
microcontinuity of the EEG. IEEE-BME 47(9):1185-1194 (2000).

Goldberger AL, et al. PhysioBank, PhysioToolkit, and PhysioNet: 
Components of a New Research Resource for Complex Physiologic Signals.
Circulation 101(23):e215-e220 (2000).
```

**Contents**:
- **197 whole-night polysomnography recordings**
- **Two study cohorts**:
  1. Sleep cassette study (SC): 153 recordings from 78 healthy subjects
  2. Sleep telemetry study (ST): 44 recordings from 22 subjects with sleep disorders
- **Signals**: 
  - EEG: Fpz-Cz, Pz-Oz (100 Hz sampling)
  - EOG: Horizontal eye movements
  - EMG: Submental chin muscle activity
  - Event markers
  - Some recordings: respiration, body temperature
- **Annotations**: Expert-scored sleep stages (R&K scoring)
  - W: Wake
  - N1, N2, N3: NREM sleep stages
  - REM: Rapid eye movement sleep
  - Movement, Unknown

---

## 📥 Complete Dataset Download

### Option 1: Direct Download (Recommended for Testing)
```bash
# Download full dataset (8.1 GB) - takes ~30 min
mkdir -p /dbfs/mnt/raw-data/physionet-sleep-edf
cd /dbfs/mnt/raw-data/physionet-sleep-edf

wget -r -N -c -np https://physionet.org/files/sleep-edfx/1.0.0/

# Directory structure:
# sleep-edfx/1.0.0/
# ├── sleep-cassette/  (SC studies)
# └── sleep-telemetry/ (ST studies)
```

### Option 2: AWS S3 (Faster)
```bash
# No authentication required
aws s3 sync --no-sign-request \
  s3://physionet-open/sleep-edfx/1.0.0/ \
  /dbfs/mnt/raw-data/physionet-sleep-edf/
```

### Option 3: Azure Blob Storage (For Databricks)
```python
# Mount Azure storage
dbutils.fs.mount(
  source="wasbs://physionet@yourstorageaccount.blob.core.windows.net",
  mount_point="/mnt/physionet-data",
  extra_configs={"fs.azure.account.key.yourstorageaccount.blob.core.windows.net": dbutils.secrets.get(scope="azure", key="storage-key")}
)

# Download to mounted storage
# ... (use wget or Azure CLI)
```

---

## 🛠️ Data Pipeline Implementation

### Bronze Layer: Raw EDF Ingestion

**File**: `src/bronze/ingest_edf_files.py`

```python
"""
PhysioNet EDF to Delta Lake Bronze Layer.

Reads European Data Format (EDF) files and converts to structured Delta tables.
Preserves raw signal data with minimal transformation.
"""

import mne  # MNE-Python: https://mne.tools
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List, Dict
import pandas as pd

class EDFToDeltaIngestion:
    
    def __init__(self, catalog: str = "sleep_eeg_catalog", schema: str = "bronze"):
        self.catalog = catalog
        self.schema = schema
        self.spark = SparkSession.builder.getOrCreate()
        
    def ingest_psg_files(self, edf_directory: str):
        """
        Ingest all PSG (polysomnography) .edf files.
        
        Args:
            edf_directory: Path to downloaded PhysioNet data
                          e.g., "/dbfs/mnt/raw-data/physionet-sleep-edf/1.0.0/"
        """
        # List all PSG files
        psg_files = dbutils.fs.ls(f"{edf_directory}/sleep-cassette/")
        psg_files += dbutils.fs.ls(f"{edf_directory}/sleep-telemetry/")
        
        # Filter for PSG files (not hypnograms)
        psg_files = [f for f in psg_files if f.name.endswith("-PSG.edf")]
        
        print(f"Found {len(psg_files)} PSG files to ingest")
        
        # Process each file
        for file_info in psg_files:
            self._process_single_edf(file_info.path)
            
    def _process_single_edf(self, edf_path: str):
        """
        Process a single EDF file into Bronze Delta tables.
        
        Creates two tables:
        1. eeg_metadata: Recording-level information
        2. eeg_raw_signals: Time-series data
        """
        # Read EDF file with MNE
        raw = mne.io.read_raw_edf(edf_path, preload=True, verbose=False)
        
        # Extract subject ID from filename
        # Format: SC4001E0-PSG.edf -> SC4001E0
        subject_id = edf_path.split("/")[-1].replace("-PSG.edf", "")
        
        # Metadata
        metadata = {
            "subject_id": subject_id,
            "file_path": edf_path,
            "sampling_freq_hz": raw.info['sfreq'],
            "n_channels": len(raw.ch_names),
            "duration_sec": raw.times[-1],
            "duration_hours": raw.times[-1] / 3600,
            "channel_names": raw.ch_names,
            "recording_date": raw.info['meas_date'],
            "ingestion_timestamp": current_timestamp()
        }
        
        # Write metadata
        metadata_df = self.spark.createDataFrame([metadata])
        metadata_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{self.catalog}.{self.schema}.eeg_metadata")
        
        # Extract signals
        # Convert to Pandas for easier manipulation
        data_pd = raw.get_data().T  # Shape: (n_timepoints, n_channels)
        times = raw.times
        
        # Create DataFrame with time index
        signal_df = pd.DataFrame(data_pd, columns=raw.ch_names)
        signal_df['time_sec'] = times
        signal_df['subject_id'] = subject_id
        
        # Convert to Spark DataFrame
        signal_spark = self.spark.createDataFrame(signal_df)
        
        # Write signals (partitioned by subject)
        signal_spark.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("subject_id") \
            .saveAsTable(f"{self.catalog}.{self.schema}.eeg_raw_signals")
        
        print(f"Ingested {subject_id}: {metadata['n_channels']} channels, {metadata['duration_hours']:.2f} hours")

# Usage
ingestion = EDFToDeltaIngestion()
ingestion.ingest_psg_files("/dbfs/mnt/raw-data/physionet-sleep-edf/1.0.0/")
```

### Hypnogram (Sleep Stage Labels)

**File**: `src/bronze/ingest_hypnogram.py`

```python
import mne

def ingest_hypnograms(edf_directory: str):
    """
    Ingest sleep stage annotations from -Hypnogram.edf files.
    
    Returns DataFrame schema:
    - subject_id: String
    - epoch_number: Integer (30-second epochs)
    - onset_sec: Float
    - duration_sec: Float
    - sleep_stage: String (W, N1, N2, N3, REM)
    - description: String (original annotation)
    """
    hypno_files = dbutils.fs.ls(f"{edf_directory}/sleep-cassette/")
    hypno_files += dbutils.fs.ls(f"{edf_directory}/sleep-telemetry/")
    
    hypno_files = [f for f in hypno_files if f.name.endswith("-Hypnogram.edf")]
    
    all_annotations = []
    
    for file_info in hypno_files:
        subject_id = file_info.name.replace("-Hypnogram.edf", "")
        
        # Read annotations
        annotations = mne.read_annotations(file_info.path)
        
        # Convert to structured format
        for i, ann in enumerate(annotations):
            all_annotations.append({
                "subject_id": subject_id,
                "epoch_number": i,
                "onset_sec": ann['onset'],
                "duration_sec": ann['duration'],
                "sleep_stage": _parse_sleep_stage(ann['description']),
                "description": ann['description'],
                "ingestion_timestamp": current_timestamp()
            })
    
    # Create Spark DataFrame
    df = spark.createDataFrame(all_annotations)
    
    # Write to Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("subject_id") \
        .saveAsTable("sleep_eeg_catalog.bronze.sleep_stages")
    
    return df

def _parse_sleep_stage(description: str) -> str:
    """
    Parse sleep stage from annotation description.
    
    Mappings:
    - 'Sleep stage W' -> 'W' (Wake)
    - 'Sleep stage 1' or 'Sleep stage N1' -> 'N1'
    - 'Sleep stage 2' or 'Sleep stage N2' -> 'N2'
    - 'Sleep stage 3' or 'Sleep stage N3' -> 'N3'
    - 'Sleep stage 4' -> 'N3' (R&K Stage 4 = AASM N3)
    - 'Sleep stage R' or 'Sleep stage REM' -> 'REM'
    - 'Movement' -> 'M'
    - 'Sleep stage ?' -> 'Unknown'
    """
    stage_map = {
        'Sleep stage W': 'W',
        'Sleep stage 1': 'N1',
        'Sleep stage N1': 'N1',
        'Sleep stage 2': 'N2',
        'Sleep stage N2': 'N2',
        'Sleep stage 3': 'N3',
        'Sleep stage N3': 'N3',
        'Sleep stage 4': 'N3',
        'Sleep stage R': 'REM',
        'Sleep stage REM': 'REM',
        'Movement': 'M',
        'Sleep stage ?': 'Unknown'
    }
    return stage_map.get(description, description)
```

---

## 📊 Data Quality Checks

### Validation Queries

```sql
-- Check data completeness
SELECT 
  COUNT(DISTINCT subject_id) as n_subjects,
  COUNT(*) as n_recordings,
  AVG(duration_hours) as avg_duration_hours,
  MIN(recording_date) as earliest_recording,
  MAX(recording_date) as latest_recording
FROM sleep_eeg_catalog.bronze.eeg_metadata;

-- Expected: 197 subjects

-- Verify channel consistency
SELECT 
  n_channels,
  COUNT(*) as n_recordings
FROM sleep_eeg_catalog.bronze.eeg_metadata
GROUP BY n_channels
ORDER BY n_channels;

-- Check sleep stage distribution
SELECT 
  sleep_stage,
  COUNT(*) as n_epochs,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM sleep_eeg_catalog.bronze.sleep_stages
GROUP BY sleep_stage
ORDER BY n_epochs DESC;

-- Typical distribution:
-- N2: ~45-55%
-- REM: ~20-25%
-- N3: ~15-20%
-- N1: ~5-10%
-- W: ~5%
```

---

## 🛡️ Troubleshooting

### Common Issues

**1. MNE-Python not installed**
```bash
# Install in Databricks cluster
%pip install mne
dbutils.library.restartPython()
```

**2. Memory errors with large files**
```python
# Process files in batches
for batch in chunk_list(psg_files, batch_size=10):
    process_batch(batch)
    spark.catalog.clearCache()  # Free memory
```

**3. Slow downloads**
- Use AWS S3 instead of direct HTTP
- Download in parallel with `aria2c`
- Use Google Cloud Storage mirror if available

**4. File format errors**
```python
# Some files may have encoding issues
raw = mne.io.read_raw_edf(path, preload=True, encoding='latin1')
```

---

## 📈 Expected Outputs

### Bronze Layer Tables

**1. `eeg_metadata`**
| subject_id | duration_hours | n_channels | sampling_freq_hz | recording_date |
|------------|----------------|------------|------------------|----------------|
| SC4001E0   | 9.92           | 5          | 100.0            | 1989-07-19     |
| SC4002E0   | 8.15           | 5          | 100.0            | 1989-08-02     |

**2. `eeg_raw_signals`** (partitioned by subject_id)
| subject_id | time_sec | EEG Fpz-Cz | EEG Pz-Oz | EOG horizontal | ... |
|------------|----------|------------|-----------|----------------|-----|
| SC4001E0   | 0.0      | 2.34       | -1.23     | 0.56           | ... |
| SC4001E0   | 0.01     | 2.41       | -1.19     | 0.58           | ... |

**3. `sleep_stages`**
| subject_id | epoch_number | onset_sec | duration_sec | sleep_stage |
|------------|--------------|-----------|--------------|-------------|
| SC4001E0   | 0            | 0.0       | 30.0         | W           |
| SC4001E0   | 1            | 30.0      | 30.0         | W           |
| SC4001E0   | 2            | 60.0      | 30.0         | N1          |

---

## ✅ Next Steps

1. **Silver Layer**: Feature extraction (spindles, slow oscillations)
2. **Gold Layer**: Subject-level aggregates, sleep quality metrics
3. **ML Models**: Sleep stage classification, anomaly detection
4. **TDA Analysis**: Persistent homology for memory consolidation

**See**: `docs/IMPLEMENTATION-GUIDE.md` for complete pipeline

---

## 📚 References

- **PhysioNet**: https://physionet.org/content/sleep-edfx/1.0.0/
- **MNE-Python Documentation**: https://mne.tools/stable/auto_tutorials/clinical/60_sleep.html
- **Sleep Staging Tutorial**: https://raphaelvallat.com/yasa/build/html/quickstart.html
- **EDF Specification**: https://www.edfplus.info/specs/edf.html

**Alternative Datasets**:
- **MASS** (Montreal Archive): http://ceams-carsm.ca/en/MASS
- **TUH EEG Corpus**: https://isip.piconepress.com/projects/tuh_eeg/
- **SHHS** (Sleep Heart Health Study): https://sleepdata.org/datasets/shhs
