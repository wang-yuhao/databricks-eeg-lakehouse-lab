# EEG Dataset Notes

## Primary Dataset: Sleep-EDF Expanded (PhysioNet)

**Citation:** Kemp et al. (2000), Goldberger et al. (2000)  
**URL:** https://physionet.org/content/sleep-edfx/1.0.0/  
**License:** PhysioNet Restricted Health Data License 1.5.0 (free for research)

### Structure
```
sleep-edf-expanded/
  sleep-cassette/         # 153 subjects, 2 nights each (SC4*)
    SC4001E0-PSG.edf      # PSG (polysomnography) recording
    SC4001EC-Hypnogram.edf  # Sleep stage annotations
  sleep-telemetry/        # 44 subjects, 1-2 nights (ST7*)
    ST7011J0-PSG.edf
    ST7011JP-Hypnogram.edf
```

### Key EDF Fields
| Field | Value | Notes |
|-------|-------|-------|
| EEG channels | Fpz-Cz, Pz-Oz | 2 EEG channels (standard in cassette subset) |
| EOG channel | ROC-LOC | Used for artifact detection |
| EMG channel | EMG submental | Chin EMG for sleep staging |
| Sampling rate | 100 Hz | All EEG/EOG channels |
| EMG sample rate | 1 Hz | Submental only |
| Recording duration | ~8 hours per night | Variable |
| Annotation format | EDF+C | Sleep stages: W, 1, 2, 3, 4, R, M, ? |

### Subject Metadata (from header)
| Field | Example | Notes |
|-------|---------|-------|
| subject_id | SC4001 | 6-char code: prefix + 4-digit ID |
| night | E0, E1 | Night index (0=first, 1=second) |
| recording_date | 1989-04-03 | From EDF header |
| age | 25 | From ST subset only (not SC) |

### Volume Estimate
- Total subjects: 197 (153 SC + 44 ST)
- Files per subject: 2 (PSG + Hypnogram) × 1-2 nights = ~2-4 EDF files
- Single EDF size: ~130 MB (8h × 2 channels × 100 Hz × 2 bytes)
- **Total estimated size: ~60 GB** for full corpus
- Pilot cohort (5 subjects, 10 EDF files): ~650 MB

### Download Commands
```bash
# Requires PhysioNet credentialing + wget
wget -r -N -c -np https://physionet.org/files/sleep-edfx/1.0.0/ -P /tmp/sleep-edf

# Or use wfdb Python package
pip install wfdb
# python -c "import wfdb; wfdb.dl_database('sleep-edfx', '/tmp/sleep-edf')"

# For pilot: download only SC4001 and SC4002 (2 subjects)
wget https://physionet.org/files/sleep-edfx/1.0.0/sleep-cassette/SC4001E0-PSG.edf
wget https://physionet.org/files/sleep-edfx/1.0.0/sleep-cassette/SC4001EC-Hypnogram.edf
```

---

## Validation Dataset: CAP Sleep Database (PhysioNet)

**URL:** https://physionet.org/content/capslpdb/1.0.0/  
**N:** 108 subjects (16 normal + 92 with various sleep disorders)  
**EEG channels:** 3-19 (variable per subject)  
**Sampling rate:** 512 Hz → must downsample to 100 Hz for consistency with Sleep-EDF

---

## Bronze Schema Design

### Table: `eeg_lakehouse.bronze.raw_eeg_files`
Registers every EDF file ingested. One row per file.

```python
BRONZE_EDF_SCHEMA = StructType([
    StructField("subject_id",    StringType(),    nullable=False),  # e.g. "SC4001"
    StructField("night_index",   IntegerType(),   nullable=True),   # 0 or 1
    StructField("file_type",     StringType(),    nullable=False),  # "PSG" or "Hypnogram"
    StructField("file_path",     StringType(),    nullable=False),  # source path
    StructField("file_name",     StringType(),    nullable=False),  # basename
    StructField("file_size_bytes",LongType(),     nullable=True),
    StructField("ingestion_ts",  TimestampType(), nullable=False),  # Auto Loader time
    StructField("source",        StringType(),    nullable=True),   # "sleep-edf" | "cap"
    # Auto Loader metadata columns (injected by cloudFiles)
    StructField("_metadata",     StructType([
        StructField("file_path",         StringType(), True),
        StructField("file_name",         StringType(), True),
        StructField("file_size",         LongType(),   True),
        StructField("file_modification_time", TimestampType(), True),
    ]), nullable=True),
])
```

### Table: `eeg_lakehouse.bronze.subject_metadata`
One row per subject (parsed from file names + any external CSV).

```python
BRONZE_METADATA_SCHEMA = StructType([
    StructField("subject_id",   StringType(),  nullable=False),
    StructField("dataset",      StringType(),  nullable=False),  # "SC" | "ST"
    StructField("n_nights",     IntegerType(), nullable=True),
    StructField("has_psg",      BooleanType(), nullable=True),
    StructField("has_hypnogram",BooleanType(), nullable=True),
    StructField("notes",        StringType(),  nullable=True),
])
```

---

## EEG Signal Characteristics (for UDF design)

| Oscillation | Frequency Band | Duration | Amplitude |
|------------|----------------|----------|-----------|
| Slow Oscillation (SO) | 0.5–1.0 Hz | 0.5–2 sec | >75 µV peak-to-peak |
| Sleep Spindle | 11–16 Hz | 0.5–3 sec | varies |
| K-complex | <3.5 Hz | >0.5 sec | >75 µV |
| Ripple (hippocampal) | 80–120 Hz | 40–100 ms | low amplitude |

## Auto Loader Notes (Exam Relevant)

- **`format("cloudFiles")`** is the Databricks Auto Loader format string
- **`cloudFiles.format`** = inner file format (here: `binaryFile` for raw EDF, or `csv` for metadata)
- **Schema location** must be a persistent cloud path (DBFS or Volume) — stores inferred schema across runs
- **Checkpoint location** tracks which files have been processed — guarantees exactly-once ingestion
- EDF files are binary → use `binaryFile` source; extract metadata from file names via UDF
