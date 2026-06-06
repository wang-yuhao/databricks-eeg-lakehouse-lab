# Dataset Notes — Sleep EEG Corpora

## Primary Dataset: Sleep-EDF Expanded (PhysioNet)

- **URL:** https://physionet.org/content/sleep-edfx/1.0.0/
- **Subjects:** 197 (SC: 153 healthy, ST: 44 with temazepam)
- **Nights:** 1–2 per subject → ~350 PSG recordings total
- **Format:** EDF (European Data Format) — binary, header + signal records
- **EEG channels:** `EEG Fpz-Cz`, `EEG Pz-Oz` (2 channels, 100 Hz)
- **Additional signals:** EOG horizontal (100 Hz), EMG submental (1 Hz), rectal body temperature (1 Hz)
- **Annotations:** EDF+ annotation file (.edf-Hypnogram) — 30-sec epochs, labels: W/1/2/3/4/R/M (wake/NREM stages/REM/movement)
- **Total uncompressed size:** ~8 GB

### Key EDF Header Fields (mapped to Bronze schema)
```
local_patient_id   → subject_id (extract SC/ST prefix + numeric ID)
startdate          → recording_date
starttime          → recording_start_time
number_of_signals  → channel_count
signal_labels      → channels (list)
sampling_frequencies → sample_rates (per channel)
```

### File Naming Convention
```
SC4001E0-PSG.edf   → SC (Study Cassette), subject 4001, E0 (evening 0)
SC4001EC-Hypnogram.edf  → corresponding annotation file
```

### Download Commands
```bash
# Install PhysioNet downloader
pip install wfdb

# Download first 5 subjects for pilot (SC40xx)
wget -r -N -c -np https://physionet.org/files/sleep-edfx/1.0.0/ \
  --accept "SC40*" -P /tmp/sleep-edf/

# Or use wfdb Python API
python -c "
import wfdb
wfdb.dl_database('sleep-edfx', '/tmp/sleep-edf/', records=['SC4001E0-PSG'])
"
```

---

## Validation Dataset: CAP Sleep Database (PhysioNet)

- **URL:** https://physionet.org/content/capslpdb/1.0.0/
- **Subjects:** 108 (16 healthy, 22 REM Behavior Disorder, 10 periodic leg movements, ...)
- **Format:** EDF + EDF+ annotations
- **EEG channels:** 3–19 (varies per subject — requires channel harmonization)
- **Sampling rate:** 512 Hz → must downsample to 100 Hz for consistency with Sleep-EDF
- **Size:** ~3 GB

---

## Volume Estimates for Databricks Planning

| Layer | Table | Format | Estimated Size | Notes |
|-------|-------|--------|----------------|-------|
| Bronze | `raw_eeg_files` | Delta (binary metadata) | ~50 MB | File paths + headers, not raw signals |
| Bronze | Actual EDF binaries | Volume (unstructured) | ~8 GB | Stored in UC Volume |
| Silver | `cleaned_epochs` | Delta (float arrays) | ~15 GB | 30-sec epochs, 100 Hz, 2 channels |
| Silver | `spindle_events` | Delta (structs) | ~200 MB | ~10–30 spindles/subject/night |
| Silver | `slow_oscillation_events` | Delta (structs) | ~100 MB | ~100–300 SOs/subject/night |
| Silver | `pac_windows` | Delta (float arrays) | ~2 GB | 5-sec windows around events |
| Gold | `tda_features` | Delta (float columns) | ~500 MB | Betti curves, landscapes, TMCI per window |
| Gold | `ml_ready_features` | Delta (wide table) | ~50 MB | One row per subject/night |

**Cluster sizing recommendation:**
- Pilot (5 subjects): Single-node `Standard_DS3_v2` (14 GB RAM) is sufficient
- Full cohort (197 subjects): 2–4 worker `Standard_DS4_v2` cluster; Ripser TDA computation is the bottleneck

---

## Memory Proxy Derivation

Since behavioral memory task scores are unavailable in Sleep-EDF, we use four EEG-validated proxies:

```python
# 1. Spindle density (spindles/minute in N2+N3)
spindle_density = spindle_count_n2_n3 / total_n2_n3_minutes

# 2. SO-spindle PAC modulation index (Tort et al. 2010)
pac_mi = tort_modulation_index(so_phase_signal, spindle_amplitude_signal)

# 3. Sigma band power (11-16 Hz during N2)
sigma_power = bandpower(eeg_signal, sf=100, band=(11, 16), hypno=hypno, include_stage=2)

# 4. Sleep quality composite
quality_score = (
    0.4 * (n3_percent / 25)       # N3 target: 25% of TST
  + 0.3 * (sleep_efficiency / 100) # SE target: 100%
  + 0.3 * (1 - waso_minutes / 60)  # WASO target: 0 min
)
```
