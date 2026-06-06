# 🧠 Databricks EEG Lakehouse Lab

> **Three purposes, one repo:** Databricks Data Engineer Exam Practice Lab · Sleep EEG TDA Research Pipeline · Senior DE/DS Portfolio

[![CI](https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab/actions/workflows/ci.yml)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange)
![License: MIT](https://img.shields.io/badge/License-MIT-green)

---

## 🎯 What This Repo Does

This repository is simultaneously:

1. **A Databricks Certified Data Engineer Practice Lab (2026 exam)** — every file maps to an explicit exam domain (Auto Loader, Delta, DLT, Unity Catalog, performance, streaming, MLflow, CI/CD). Exam notes live in `docs/exam/`.

2. **A production-style Sleep EEG Research Pipeline** — implements the *Persistent Homology Reveals Topological Dynamics of Sleep EEG Networks During Memory Consolidation* research proposal (Yuhao Wang, LMU Munich). The pipeline processes PhysioNet Sleep-EDF Expanded data (N≈200 subjects) through Bronze → Silver → Gold Delta tables, culminating in TDA feature extraction and ML-based memory proxy prediction.

3. **A Senior DE/DS Portfolio Project** — structured with real engineering discipline: type hints, docstrings, Pytest, GitHub Actions CI/CD, Unity Catalog governance, and interview-ready STAR stories in the docs.

---

## 🏗️ Architecture

```
PhysioNet EDF Files
       │
       ▼  (Auto Loader / cloudFiles)
 ┌─────────────┐
 │  🥉 BRONZE  │  Raw EEG binary metadata, file registry, subject catalog
 │  Delta Lake │  ingest_eeg_files.py · ingest_metadata.py
 └─────┬───────┘
       │  PySpark UDFs / Pandas UDFs
       ▼
 ┌─────────────┐
 │  🥈 SILVER  │  Cleaned signals, spindle events, SO events, PAC windows
 │  Delta Lake │  preprocess_eeg.py · detect_events.py
 └─────┬───────┘
       │  Feature engineering
       ▼
 ┌─────────────┐
 │  🥇 GOLD    │  TDA feature table, ML-ready wide table, TMCI index
 │  Delta Lake │  build_features.py · train_ml_model.py
 └─────┬───────┘
       │  MLflow
       ▼
 ┌─────────────┐
 │  📊 MLflow  │  RF/XGBoost models predicting spindle density & PAC
 │  Model Reg. │  SHAP explanations · persistence landscapes
 └─────────────┘
```

**Unity Catalog:** `eeg_lakehouse.bronze / .silver / .gold`  
**DLT Pipeline:** `resources/eeg_dlt_pipeline.yml`  
**Streaming:** Simulated live EEG events via Structured Streaming

---

## 🗓️ 14-Day Program

See [`docs/daily-plan.md`](docs/daily-plan.md) for the full day-by-day checklist.

| Week | Days | Focus |
|------|------|-------|
| 1 | 1–3 | Repo setup, Bronze ingestion, Auto Loader patterns |
| 1 | 4–6 | Silver preprocessing, event detection, Gold features |
| 1 | 7 | DLT pipeline skeleton |
| 2 | 8–9 | Unity Catalog governance, Streaming |
| 2 | 10–11 | MLflow, CI/CD & tests |
| 2 | 12–13 | Performance tuning, exam mini-labs |
| 2 | 14 | Portfolio polish & interview narratives |

---

## 📁 Repository Structure

```
databricks-eeg-lakehouse-lab/
├── README.md
├── requirements.txt
├── databricks.yml              # Databricks Asset Bundle skeleton
├── docs/
│   ├── daily-plan.md           # 14-day checklist
│   ├── exam/
│   │   ├── domains-overview.md
│   │   ├── delta-patterns.md
│   │   ├── dlt-cheatsheet.md
│   │   ├── uc-governance.md
│   │   ├── performance-tuning.md
│   │   ├── ci-cd-notes.md
│   │   └── patterns-lab.md
│   └── research/
│       ├── project-overview.md
│       ├── dataset-notes.md
│       └── analysis-questions.md
├── src/
│   ├── bronze/
│   │   ├── ingest_eeg_files.py
│   │   ├── ingest_metadata.py
│   │   └── ingest_streaming_events.py
│   ├── silver/
│   │   ├── preprocess_eeg.py
│   │   └── detect_events.py
│   ├── gold/
│   │   ├── build_features.py
│   │   └── train_ml_model.py
│   └── utils/
│       ├── config.py
│       └── logging.py
├── notebooks/
│   ├── day01_intro_and_setup.py
│   ├── day02_bronze_schema_design.py
│   └── ...  (added per day)
├── resources/
│   └── eeg_dlt_pipeline.yml
├── tests/
│   ├── conftest.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
└── .github/
    └── workflows/
        └── ci.yml
```

---

## 🔬 Research Context

The neuroscience pipeline implements **Persistent Homology** applied to sleep EEG for the first time, targeting three key findings:

- **H1:** SO-spindle coupling windows show significantly higher β₁ (loop) persistence than non-coupling windows
- **H2:** High spindle density subjects show lower Wasserstein distance between consecutive persistence diagrams (topological stability)
- **H3:** TDA features improve memory proxy prediction by ΔR² > 0.10 over spectral baselines

Datasets: [Sleep-EDF Expanded](https://physionet.org/content/sleep-edfx/1.0.0/) (N=197), [CAP Sleep Database](https://physionet.org/content/capslpdb/1.0.0/) (N=108)

---

## 🚀 Quick Start

```bash
git clone https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git
cd databricks-eeg-lakehouse-lab
pip install -r requirements.txt
pytest tests/ -v
```

For Databricks: import notebooks from `notebooks/` into your workspace and set `CATALOG`, `SCHEMA`, `VOLUME_PATH` in your cluster environment.

---

## 📊 Exam Domain Coverage

| Domain | Coverage | Key Files |
|--------|----------|----------|
| Lakehouse & Delta Lake | ✅ Bronze/Silver/Gold tables | `src/bronze/`, `src/silver/`, `src/gold/` |
| Auto Loader | ✅ cloudFiles ingestion | `src/bronze/ingest_eeg_files.py` |
| Delta Live Tables (DLT) | ✅ Full DLT pipeline | `resources/eeg_dlt_pipeline.yml` |
| Unity Catalog | ✅ 3-layer catalog | `notebooks/day08_unity_catalog_setup.py` |
| Streaming | ✅ Structured Streaming | `src/bronze/ingest_streaming_events.py` |
| Performance & Cost | ✅ OPTIMIZE/ZORDER/AQE | `docs/exam/performance-tuning.md` |
| CI/CD & Git | ✅ GitHub Actions | `.github/workflows/ci.yml` |
| MLflow | ✅ Training + registry | `src/gold/train_ml_model.py` |

---

## 💼 Interview-Ready Stories

See [`docs/daily-plan.md`](docs/daily-plan.md) → Day 14 for full STAR stories. Quick summary:

- **Pipeline Story:** "I designed a medallion lakehouse pipeline for multichannel EEG data, handling binary EDF ingestion with Auto Loader, incremental Bronze→Silver→Gold processing, and DLT for data quality governance."
- **Streaming Story:** "I implemented a Structured Streaming path for simulated real-time EEG events, using watermarks and micro-batch mode to handle late-arriving sensor data."
- **Governance Story:** "I configured Unity Catalog with GRANT-based access control across research and analytics roles, with row-level security on subject demographics."

---

## 📜 License

MIT License — Yuhao Wang, 2026
