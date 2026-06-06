# Research Project Overview

## Persistent Homology Reveals Topological Dynamics of Sleep EEG Networks During Memory Consolidation

**Principal Investigator:** Yuhao Wang  
**Affiliation:** Ludwig-Maximilians-Universität München (M.Sc. Computer Science)  
**Status:** Research prototype → production pipeline in this repo

---

## Abstract (Short)

Sleep-dependent memory consolidation depends on precisely timed SO→spindle→ripple coupling during NREM sleep. Classical methods (spectral power, PLV, coherence) capture only pairwise, threshold-dependent, static snapshots. This project applies **dynamic persistent homology** — tracking Betti numbers, persistence landscapes, and Wasserstein distances across sliding EEG windows — to characterize the multi-scale topological structure of sleep EEG networks and predict EEG-validated memory proxies.

---

## Core Hypotheses

| ID | Hypothesis | Operationalization |
|---|---|---|
| H1 | β₁ (loops) ↑ during SO-spindle coupling | Pearson r > 0.3, PAC MI vs summed H₁ lifetimes, LME model p < 0.05 |
| H2 | Spindle-dense subjects show higher topological stability | Lower Wasserstein distance between consecutive persistence diagrams |
| H3 | TDA features outperform spectral baseline | ΔR² > 0.10 in LOSO cross-validation |
| H4 | Topological structure evolves across sleep cycles | β₀ ↓ (integration), β₁ ↑ (loop formation) across NREM cycles |

---

## Datasets

| Dataset | N | Channels | Fs | Access |
|---|---|---|---|---|
| Sleep-EDF Expanded | 197 subjects, 2 nights | 19 EEG | 100 Hz | PhysioNet (public) |
| CAP Sleep Database | 108 subjects | variable | 512 Hz | PhysioNet (public) |

No new IRB required — exclusively publicly available, de-identified data.

---

## Pipeline Stages

### Stage 1: Bronze — Raw Ingestion
- EDF files ingested via Databricks Auto Loader into Delta Bronze table
- Fields: `subject_id`, `night`, `file_path`, `file_size_bytes`, `ingestion_timestamp`, `raw_bytes`
- Schema enforced at write; file-level deduplication via `_rescued_data` column

### Stage 2: Silver — Preprocessing & Event Detection
- **Preprocessing (MNE-Python):** Bandpass filter 0.5–40 Hz (zero-phase FIR), 50 Hz notch, ICA artifact rejection (EOG/EMG), epoching into 30-second segments
- **Sleep staging (YASA):** Automated NREM/REM/Wake labeling (>80% agreement with expert)
- **Event detection (YASA):** Spindle catalog (11–16 Hz), SO identification (0.5–1 Hz), K-complex marking, PAC modulation index (Tort 2010)
- **TDA point clouds:** Three representations per 5-second sliding window (50% overlap):
  1. Time-delay (Takens) embeddings of spindle-band signals
  2. Cross-channel correlation matrix → distance matrix D = 1 – |R|
  3. Phase-space reconstruction of filtered EEG
- **Ripser filtrations:** Vietoris-Rips on each window → persistence diagrams (H₀, H₁, H₂)

### Stage 3: Gold — Features & ML
- Feature engineering: Betti curves, persistence landscapes (L¹/L² norms), persistent entropy, total persistence, Wasserstein distances, Topological Volatility Index (TVI), **Topological Memory Consolidation Index (TMCI)**
- ML: Random Forest + XGBoost predicting binary memory-proxy outcomes (above/below median spindle density; high/low PAC)
- Evaluation: LOSO cross-validation, SHAP feature importance, permutation testing
- Baseline: spectral features (band power, spectral entropy, PLV, coherence)

---

## Connection to Prior Work

- **Ngo et al. 2020 (J. Neurosci.):** SO-targeted auditory cueing modulates spindle coupling — our TDA features can characterize the network geometry of such cueing-induced changes.
- **Staresina et al. 2015 (Nat. Neurosci.):** Nested hippocampal ripples–spindles–SOs during NREM — the nested hierarchy is exactly what β₁ loops in the Vietoris-Rips filtration would capture.
- **Fernandez-Sanjurjo et al. 2026 (PLOS Biol.):** PAC predicts successor-representation memory transformation — directly motivates our PAC modulation index as a Gold-layer prediction target.

---

## Novelty Statement

As of 2025, no published work combines TDA, sleep EEG, and memory consolidation (confirmed by exhaustive PubMed/ArXiv/Scholar search, 100+ papers, 2020–2025). This is a confirmed literature gap and a first-of-kind methodological contribution.

---

## Open Science Commitments

- All code released MIT license on this GitHub repo
- Processed feature matrices archived on Zenodo (DOI assigned)
- Environment fully reproducible via `requirements.txt` + `conda export`
- FAIR data principles throughout
