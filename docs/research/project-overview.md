# Research Project Overview

## Persistent Homology Reveals Topological Dynamics of Sleep EEG Networks During Memory Consolidation

**Author:** Yuhao Wang | LMU Munich · Serviceplan Group (Mediaplus)  
**Status:** Pipeline implementation (this repo) | PhD application target: Stockholm University DSV

---

## 1. The Core Question

Does the *geometry* of sleep EEG network activity — captured by topological features (Betti numbers, persistence landscapes) — predict memory consolidation better than traditional spectral measures (spindle density, σ-power, PAC modulation index)?

---

## 2. Neuroscience Background

### The Memory Consolidation Mechanism
During NREM sleep, memory consolidation depends on three nested oscillatory events:

```
[Slow Oscillation: 0.5–1 Hz]  <-- envelope
    └── [Sleep Spindle: 11–16 Hz]  <-- nested within SO up-state
            └── [Hippocampal Ripple: 80–120 Hz]  <-- nested within spindle trough
```

This triple coupling enables **hippocampal → neocortical information transfer**. The 2024 PLoS Biology paper (Fernandez-Sanjurjo et al.) showed that the *coupling geometry* — not just event counts — predicts which memory representations get transformed (successor representation learning).

### What Current Methods Miss
Existing approaches (PLV, coherence, PAC) capture only **pairwise** channel relationships. They miss:
- Higher-order (3-way, k-way) interactions between EEG channels
- Time-varying topological structure (topology changes *across* sleep cycles)
- Scale-free, threshold-independent characterization of network geometry

### Why Persistent Homology Solves This
Persistent homology tracks the **birth and death of topological features** across filtration scales:
- **β₀ (connected components):** How many disconnected EEG channel clusters exist?
- **β₁ (loops/cycles):** Are there closed loops in the functional connectivity graph?
- **β₂ (voids):** Higher-dimensional cavities in the phase-space reconstruction?

Key property: coordinate-invariant, noise-robust, multi-scale → ideal for neural data.

---

## 3. Research Hypotheses

| Hypothesis | Claim | Operationalization |
|------------|-------|--------------------|
| **H1** (Topological Coupling Signature) | β₁ persistence is higher during SO-spindle coupling windows | Pearson r > 0.3 between PAC modulation index and summed H₁ lifetimes |
| **H2** (Consolidation Stability) | High spindle density subjects show lower Wasserstein distance between consecutive persistence diagrams | Wasserstein distance comparison: top vs bottom spindle density tertile |
| **H3** (Predictive Superiority) | TDA features improve memory proxy prediction over spectral features | ΔR² > 0.10 in LOSO cross-validation |
| **H4** (Sleep-Cycle Dynamics) | β₀ decreases and β₁ increases across successive NREM periods | Longitudinal mixed-effects model on topological features per sleep cycle |

---

## 4. Datasets

### Primary: Sleep-EDF Expanded (PhysioNet)
- **N:** 197 subjects, 2 nights each
- **EEG channels:** 2 (Fpz-Cz, Pz-Oz) in original; expanded to 19 channels in later versions
- **Sampling rate:** 100 Hz
- **Sleep stages:** Expert-annotated (Rechtschaffen & Kales)
- **Access:** `https://physionet.org/content/sleep-edfx/1.0.0/`
- **Size estimate:** ~20 GB for full EDF corpus

### Validation: CAP Sleep Database (PhysioNet)
- **N:** 108 subjects
- **EEG channels:** 3–19 (varies)
- **Sampling rate:** 512 Hz (needs downsampling to 100 Hz for consistency)
- **Access:** `https://physionet.org/content/capslpdb/1.0.0/`

### Memory Proxies (since behavioral data unavailable)
1. **Spindle density** — spindles/min in N2+N3, validated r=0.3–0.6 with next-day recall
2. **SO-spindle PAC modulation index** — Tort et al. 2010 method
3. **Sigma power** — 11–16 Hz power during N2
4. **Sleep architecture quality score** — composite (N3%, REM%, sleep efficiency)

---

## 5. Technical Pipeline

```
[EDF Files] → Bronze (raw binary/metadata)
      ↓ MNE-Python preprocessing (UDF)
[Bronze] → Silver (cleaned epochs, sleep stages)
      ↓ YASA event detection (UDF)
[Silver events] → Silver (spindle/SO catalogs, PAC windows)
      ↓ Feature engineering
[Silver] → Gold (TDA features: Betti curves, landscapes, TMCI)
      ↓ MLflow
[Gold] → ML Models (RF/XGBoost) → Memory proxy predictions
      ↓ SHAP
[Interpretability] → Which topological features matter most?
```

**TDA Tools:** Ripser (Vietoris-Rips filtration), Giotto-TDA (persistence diagrams), persim (Wasserstein distance)

---

## 6. Novel Contribution

This is the **first application of dynamic persistent homology to sleep EEG for memory research** — confirmed by exhaustive literature search (PubMed, ArXiv, Google Scholar, 2020–2025, 100+ papers reviewed).

The open-source `SleepTDA` toolkit produced by this project will enable the broader sleep community to apply these methods.

---

## 7. Connection to Ngo 2020 / Staresina Work

- **Ngo et al. (2020)** demonstrated that targeted memory reactivation (TMR) during slow oscillation up-states enhances memory. Our pipeline's SO detection and PAC computation directly enables TMR-style analysis.
- **Staresina et al.** established that spindle-ripple coupling strength (not just count) correlates with memory benefit — our β₁ persistence hypothesis is a topological operationalization of this coupling geometry.

---

## 8. Target Outputs

1. **Processed feature matrices** archived on Zenodo with DOI
2. **Open-source Python toolkit** (`SleepTDA`) on GitHub under MIT license
3. **Preprint** on bioRxiv
4. **Journal submission** — primary: NeuroImage (IF 5.7), PLOS Computational Biology (IF 4.3)
