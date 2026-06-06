# Dataset Notes — Sleep EEG Pipeline

## Primary Dataset: Sleep-EDF Expanded (PhysioNet)

- **URL:** https://physionet.org/content/sleep-edfx/1.0.0/
- **N subjects:** 197 healthy adults (20 Cassette recordings + 153 Telemetry recordings)
- **Nights per subject:** 1–2 (some subjects have only 1 recording night)
- **Format:** EDF+ (European Data Format)
- **EEG channels (Cassette):** `EEG Fpz-Cz`, `EEG Pz-Oz` (2 channels)
- **EOG channel:** `EOG horizontal`
- **EMG channel:** `EMG submental`
- **Sampling rate:** 100 Hz (EEG/EOG), 1 Hz (body temp/event markers)
- **Sleep stage annotations:** Hypnogram in separate EDF annotation file (`.edf` + `-Hypnogram.edf`)
- **Stage labels:** W (wake), 1, 2, 3, 4 (R&K), R (REM), ? (movement/unscored)
- **Recording duration:** 8–10 hours per night
- **Size estimate:** ~20 GB for full corpus (EDF files)

### File Naming Convention
```
SC4001E0-PSG.edf    <- subject SC, age 40 (01), study night 1, PSG recording
SC4001EC-Hypnogram.edf  <- corresponding hypnogram
```

Subject ID extraction regex: `r'(SC|ST)(\d{4})'
- `SC` = Cassette recording (home)
- `ST` = Telemetry recording (hospital)

---

## Validation Dataset: CAP Sleep Database (PhysioNet)

- **URL:** https://physionet.org/content/capslpdb/1.0.0/
- **N subjects:** 108
- **EEG channels:** 3–19 (varies per recording)
- **Sampling rate:** 512 Hz → must downsample to 100 Hz for consistency
- **Special feature:** Cyclic Alternating Pattern (CAP) annotations
- **Size estimate:** ~5 GB

---

## Bronze Schema Design

The Bronze layer stores a **registry** of EDF files — not the raw signal arrays (too large for columnar storage). Signal arrays are processed via Pandas UDFs in the Silver layer.

### Table: `eeg_lakehouse.bronze.raw_eeg_files`

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `file_path` | STRING | Auto Loader `_metadata.file_path` | Full DBFS/Volume path |
| `file_name` | STRING | Derived | Basename of EDF file |
| `subject_id` | STRING | Regex on filename | `SC4001`, `ST7011`, etc. |
| `recording_type` | STRING | Regex | `SC` or `ST` |
| `subject_age` | INTEGER | Regex on filename | Age encoded in filename |
| `study_night` | INTEGER | Regex | 1 or 2 |
| `is_hypnogram` | BOOLEAN | Filename suffix | TRUE for `*Hypnogram.edf` |
| `file_size_bytes` | LONG | Auto Loader `_metadata.file_size` | For monitoring |
| `file_modification_time` | TIMESTAMP | Auto Loader `_metadata.file_modification_time` | For incremental detection |
| `ingestion_timestamp` | TIMESTAMP | `current_timestamp()` | Audit column |
| `dataset_source` | STRING | Hardcoded | `sleep-edf-expanded` or `cap-sleep` |

### Table: `eeg_lakehouse.bronze.subject_metadata`

| Column | Type | Notes |
|--------|------|-------|
| `subject_id` | STRING | Primary key |
| `recording_type` | STRING | SC or ST |
| `age` | INTEGER | |
| `sex` | STRING | M/F (from companion CSV) |
| `lightsoff_time` | STRING | Bedtime annotation |
| `lightson_time` | STRING | Wake annotation |
| `dataset_source` | STRING | |

---

## Volume Estimates

| Layer | Format | Est. Size | Rows |
|-------|--------|-----------|------|
| Bronze (file registry) | Delta | ~1 MB | ~400 (197 subjects × 2 nights) |
| Silver (30-sec epochs) | Delta | ~500 MB | ~200,000 epochs |
| Silver (spindle events) | Delta | ~50 MB | ~500,000 spindle events |
| Silver (SO events) | Delta | ~20 MB | ~200,000 SO events |
| Gold (TDA features) | Delta | ~100 MB | ~200,000 rows (1 per epoch) |

---

## Download Instructions (Local Dev)

```bash
# Install PhysioNet client
pip install wfdb

# Download 5-subject pilot cohort (subjects SC4001, SC4002, SC4011, SC4012, SC4021)
python -c "
import wfdb
for subj in ['SC4001E0-PSG', 'SC4001EC-Hypnogram',
             'SC4002E0-PSG', 'SC4002EC-Hypnogram',
             'SC4011E0-PSG', 'SC4011EH-Hypnogram']:
    wfdb.dl_database('sleep-edfx/1.0.0/', dl_dir='/tmp/sleep-edf', records=[subj])
"
```

## PhysioNet Access on Databricks

```python
# Use dbutils to download to a Unity Catalog Volume
import subprocess
subprocess.run([
    'wget', '-r', '-N', '-c', '-np',
    '--directory-prefix=/Volumes/eeg_lakehouse/bronze/raw_edf/',
    'https://physionet.org/files/sleep-edfx/1.0.0/sleep-cassette/'
])
```
