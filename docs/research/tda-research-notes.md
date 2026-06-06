# TDA Research Notes
## Persistent Homology on Sleep EEG — Research Foundation

> Research proposal: *Persistent Homology Reveals Topological Dynamics of Sleep EEG Networks
> During Memory Consolidation* (Yuhao Wang, LMU Munich)

---

## Motivation

Sleep plays a critical role in memory consolidation. Standard sleep staging
(Wake/N1/N2/N3/REM) captures temporal sequences of brain states, but misses
**topological structure** within each stage: how brain regions coordinate,
form transient coalitions, and dissolve.

Topological Data Analysis (TDA) — specifically persistent homology — offers
a principled way to quantify this structure without requiring predefined
network parcellations.

---

## Key Concepts

### Persistent Homology

| Concept | Meaning | EEG application |
|---|---|---|
| **Simplicial complex** | Generalisation of a graph to higher dimensions | Correlation matrix of EEG channels forms a Rips complex |
| **Betti number β0** | Number of connected components | How many independent EEG subnetworks exist? |
| **Betti number β1** | Number of 1-cycles (loops) | Are there oscillatory feedback loops in EEG? |
| **Persistence diagram** | Birth/death pairs of topological features | Which features are robust vs noise? |
| **Persistence entropy** | Shannon entropy of the diagram | Summary scalar for ML feature engineering |
| **Wasserstein distance** | Distance between two diagrams | Similarity between two sleep stage topologies |

### Why Persistent Homology for EEG?

1. **Scale-free**: Captures structure at multiple spatial/temporal resolutions
2. **Network-agnostic**: No a-priori graph construction needed
3. **Memory-sensitive**: N2/N3 sleep spindles and slow oscillations have distinct topological signatures
4. **Interpretable**: Betti numbers have direct neuroscientific meaning

---

## Pipeline Design

```
PhysioNet Sleep-EDF Expanded (N=200 subjects)
    ↓
[Bronze] Auto Loader → Delta table
    ↓ EDF binary + metadata
[Silver] MNE-Python preprocessing
    - Band-pass filter: 0.5–45 Hz
    - Epoch extraction: 30 s windows
    - Artefact rejection (peak-to-peak amplitude > 150 μV)
    - ICA (optional, for ocular/muscle artefacts)
    ↓ Clean epochs + hypnogram labels
[Gold] TDA feature extraction
    - Pearson correlation matrix → distance matrix
    - Vietoris-Rips filtration (Ripser)
    - Persistence diagrams (H0, H1, H2)
    - Wasserstein distances between stages
    - Persistence entropy per epoch
    ↓ Feature DataFrame
[ML] XGBoost / Random Forest
    - Target: memory score (Word Pair Associative task proxy)
    - Features: TDA features + classic spectral features
    - Evaluation: 5-fold CV, AUROC
    - SHAP explanation: which TDA features drive memory prediction?
```

---

## Current Status (Mock Implementation)

The current pipeline uses **mock Pandas UDFs** that simulate TDA features
(random Betti numbers, persistence entropy drawn from distributions matching
the literature). This is sufficient for:

- Demonstrating the full pipeline architecture
- Testing data flow: Bronze → Silver → Gold → MLflow
- Exam practice (UDFs, DLT, Unity Catalog, streaming)
- Portfolio demonstration

### Known Gap
Real MNE + Ripser integration is commented out due to CI build time.
Activate by uncommenting in `requirements.txt`:
```
# mne>=1.6
# yasa>=0.6
# ripser>=0.6
# giotto-tda>=0.6
```

---

## Literature

| Paper | Key finding | Relevance |
|---|---|---|
| Reimann et al. (2017) *Neuron* | Cliques and cavities in cortical networks | Foundation for using higher Betti numbers |
| Stolz et al. (2021) *PNAS* | Persistent homology of functional brain networks | Direct methodological inspiration |
| Helfrich et al. (2018) *Nature Comms* | Sleep spindles during N2 aid hippocampal replay | Memory consolidation mechanism |
| Diekelmann & Born (2010) *Nat Rev Neurosci* | Sleep and memory consolidation review | Background motivation |
| Chung et al. (2019) *Neuroimage* | TDA of EEG during cognitive tasks | EEG-specific TDA methodology |

---

## Exam Connection

This research pipeline exercises every Databricks DE 2026 exam domain:

| Exam domain | Pipeline component |
|---|---|
| Auto Loader / incremental ingestion | Bronze EDF file ingestion |
| Delta Lake, schema evolution | Bronze/Silver/Gold tables |
| Pandas UDFs | Silver MNE preprocessing, Gold TDA features |
| DLT + expectations | Full pipeline as DLT graph |
| Unity Catalog | Governance: row-level filters by study cohort |
| Structured Streaming | Real-time EEG epoch processor |
| MLflow | XGBoost training, SHAP, Model Registry |
| Performance tuning | OPTIMIZE/ZORDER on subject_id + study_night |
| CI/CD | GitHub Actions, pytest with local SparkSession |

---

## Next Steps

1. **Replace mock UDFs with real MNE**: `preprocess_eeg.py` → actual band-pass, epoching
2. **Integrate Ripser**: `build_features.py` → actual persistence diagrams
3. **Add Giotto-TDA**: Wasserstein distance between sleep stage diagrams
4. **Real dataset**: Download PhysioNet Sleep-EDF Expanded to ADLS Gen2
5. **Publish results**: Academic poster or short paper (LMU Munich Neuro group)
