# databricks-eeg-lakehouse-lab

> **Three purposes, one codebase:** Databricks Data Engineer 2026 exam lab · Production-grade sleep EEG/TDA research pipeline · Senior DE/DS portfolio.

---

## 🎯 Goals

| Dimension | Goal |
|---|---|
| **Exam** | Pass Databricks Certified Data Engineer Associate 2026 — every domain covered with a live example in this repo |
| **Research** | Reproduce & extend the *Persistent Homology Reveals Topological Dynamics of Sleep EEG Networks During Memory Consolidation* pipeline (TDA + YASA + MNE on Sleep-EDF Expanded N≈200) |
| **Portfolio** | Demonstrate senior ownership to hiring managers at Siemens, Allianz, BMW, Deutsche Bank in Germany |

---

## 🏗 Architecture

```
Raw EDF Files (ADLS Volume)
       │
       ▼  Auto Loader (cloudFiles)
  ┌─────────────┐
  │   BRONZE    │  Raw EEG bytes + metadata → Delta table
  │  (ingest)   │  Schema enforcement, file-level deduplication
  └──────┬──────┘
         │  DLT @dlt.table + @dlt.expect
         ▼
  ┌─────────────┐
  │   SILVER    │  MNE preprocessing → spindle/SO detection (YASA)
  │  (process)  │  TDA point clouds → Vietoris-Rips → persistence diagrams
  └──────┬──────┘
         │  DLT + Unity Catalog
         ▼
  ┌─────────────┐
  │    GOLD     │  Feature table: Betti numbers, TMCI, spindle density, PAC
  │  (features) │  MLflow: RF / XGBoost trained on topological features
  └─────────────┘
```

**Platform:** Databricks on Azure · Unity Catalog · Delta Live Tables · MLflow · Auto Loader · GitHub Actions CI/CD

---

## 📁 Repository Structure

```
databricks-eeg-lakehouse-lab/
├── README.md
├── requirements.txt
├── databricks.yml               # Asset Bundle skeleton (Week 2)
├── docs/
│   ├── daily-plan.md            # 14-day checklist
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
│   └── ...
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

## 🗓 14-Day Roadmap

See [`docs/daily-plan.md`](docs/daily-plan.md) for the full checklist.

| Week | Days | Theme |
|---|---|---|
| **1** | 1–7 | Lakehouse pipeline (Bronze → Silver → Gold) + DLT |
| **2** | 8–14 | Unity Catalog, Streaming, MLflow, CI/CD, Performance, Portfolio polish |

---

## 🧠 Research Summary

This project implements the first dynamic persistent homology pipeline for sleep EEG, testing whether topological network features (Betti numbers β₀, β₁, β₂; persistence landscapes; Wasserstein distances) predict memory-consolidation proxies (spindle density, SO-spindle PAC modulation index) better than classical spectral baselines.

**Dataset:** Sleep-EDF Expanded (N≈197, PhysioNet) + CAP Sleep Database (N≈108)  
**Key tools:** MNE-Python · YASA · Ripser · Giotto-TDA · scikit-learn · XGBoost · SHAP  
**Hypotheses:** H1 (β₁ ↑ during SO-spindle coupling) · H2 (topological stability predicts spindle density) · H3 (TDA ΔR²>0.10 over spectral baseline) · H4 (sleep-cycle β₀↓ β₁↑ trajectory)

---

## 🔗 How This Repo Maps to the Databricks Exam

| Exam Domain | Covered By |
|---|---|
| Databricks Lakehouse Platform | `src/utils/config.py`, `docs/exam/domains-overview.md` |
| ELT with Spark SQL & Python | `src/silver/preprocess_eeg.py`, notebooks |
| Incremental Data Processing | Auto Loader in `src/bronze/ingest_eeg_files.py` |
| Production Pipelines (DLT) | DLT decorators in all `src/` layers, `docs/exam/dlt-cheatsheet.md` |
| Data Governance (Unity Catalog) | `notebooks/day08_unity_catalog_setup.py`, `docs/exam/uc-governance.md` |
| Security | `docs/exam/uc-governance.md` (GRANT / RLS patterns) |
| Performance Optimization | `docs/exam/performance-tuning.md`, `notebooks/day12_performance_experiments.py` |

---

## ⚡ Quick Start

```bash
git clone https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git
cd databricks-eeg-lakehouse-lab
pip install -r requirements.txt
pytest tests/ -v
```

> **Note:** Full pipeline requires Databricks workspace + ADLS Gen2. Local mode uses PySpark with Delta Lake OSS.

---

## 📜 License

MIT License · © 2025 Yuhao Wang
