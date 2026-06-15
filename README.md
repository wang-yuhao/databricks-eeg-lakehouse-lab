# Databricks EEG Lakehouse Lab

> **Three purposes, one repo:** Associate + Professional Data Engineer Exam Prep (21-Day Program) · Sleep EEG Research Pipeline · Senior DE/DS Portfolio

![CI](https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab/actions/workflows/ci.yml/badge.svg)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange)
![License: MIT](https://img.shields.io/badge/License-MIT-green)

---

## What This Repo Does

1. **Associate + Professional Data Engineer Certification Prep (21-day program)** — Days 1-14 cover Associate-level fundamentals (Auto Loader, Delta, DLT, Unity Catalog basics). Days 15-21 advance to Professional topics (Delta Sharing, Monitoring, Security, CLI/API, Advanced Data Modeling). 21 daily study notes live in `docs/study-notes/`.
   
2. **Production-style Sleep EEG Research Pipeline** — processes PhysioNet Sleep-EDF Expanded data (N=200 subjects) through Bronze → Silver → Gold Delta tables, culminating in sleep feature extraction, ML-based sleep staging, and Topological Data Analysis (persistent homology) for memory consolidation research.
   
3. **Senior DE/DS Portfolio Project** — structured with real engineering discipline: type hints, docstrings, pytest, GitHub Actions CI/CD, Unity Catalog governance, and interview-ready STAR stories in the docs.

---

## Architecture

```
PhysioNet EDF Files
        |
        ▼  (Auto Loader / cloudFiles)
┌──────────────┐
│  BRONZE       │  Raw EEG binary metadata, file registry, subject catalog
│  Delta Lake   │  ingest_eeg_files.py · ingest_metadata.py
└──────────────┘
        |
        ▼  PySpark UDFs / Pandas UDFs
┌──────────────┐
│  SILVER       │  Cleaned signals, spindle events, SO events, PAC windows
│  Delta Lake   │  preprocess_eeg.py · detect_events.py
└──────────────┘
        |
        ▼  Feature engineering
┌──────────────┐
│  GOLD         │  Subject-level features, sleep quality metrics
│  Delta Lake   │  build_features.py
└──────────────┘
        |
        ▼  MLflow
┌──────────────┐
│  ML Models    │  Random Forest / LSTM sleep stage classifier
│  Model Reg.   │  Experiment tracking, registry, batch scoring
└──────────────┘

        ▼  TDA Research Component
┌──────────────┐
│  Persistent   │  Topological Data Analysis: persistent homology
│  Homology     │  Memory consolidation patterns during sleep
└──────────────┘

Governance: Unity Catalog (catalog.schema.table) + column masking + row filters
Sharing:    Delta Sharing (cross-workspace federation, BI tool integration)
Streaming:  Structured Streaming with watermarks + foreachBatch MERGE
CI/CD:      GitHub Actions (pytest) + Databricks Asset Bundles
```

---

## Repo Structure

```
databricks-eeg-lakehouse-lab/
├── README.md                      # This file
├── databricks.yml                 # Databricks Asset Bundle
├── pyproject.toml                 # Python packaging & tool config
├── CHANGELOG.md                   # Version history
├── PROFESSIONAL-EXAM-PREP-SUMMARY.md  # Professional certification roadmap
├── .github/workflows/ci.yml       # GitHub Actions CI
├── src/                           # Production Python source
│   ├── bronze/                    # Auto Loader ingestion
│   ├── silver/                    # UDFs: event detection, preprocessing
│   ├── gold/                      # Feature engineering
│   ├── dlt/                       # Delta Live Tables pipeline
│   ├── streaming/                 # Structured Streaming processor
│   ├── ml/                        # MLflow model classes
│   └── utils/                     # Shared helpers
├── notebooks/                     # Day-by-day Databricks notebooks (Days 1-14)
├── tests/                         # pytest unit tests
├── data/                          # Local data for development
└── docs/
    ├── project-overview.md        # Architecture map & 14-day schedule
    ├── IMPLEMENTATION-GUIDE.md    # Comprehensive 21-day roadmap
    ├── daily-plan.md              # Daily task tracker
    ├── interview-star-stories.md  # Portfolio STAR interview stories
    ├── study-notes/               # 21 daily exam study notes
    │   ├── day01-repo-bootstrap.md through day14-portfolio-capstone.md (Associate)
    │   └── day15-data-source-connectors.md through day21-data-modeling.md (Professional)
    ├── exam/                      # Deep-dive exam reference docs
    └── research/                  # EEG neuroscience background
        └── DATASET-INTEGRATION-GUIDE.md  # PhysioNet Sleep-EDF dataset integration guide
```

---

## 21-Day Program Structure

### **Phase 1: Associate Certification (Days 1-14)**

| Day | Topic | Key Exam Domain |
|---|---|---|
| [01](docs/study-notes/day01-repo-bootstrap.md) | Lakehouse fundamentals & repo bootstrap | Lakehouse Platform, Delta Lake |
| [02](docs/study-notes/day02-bronze-schema-design.md) | Bronze schema & Auto Loader | Auto Loader, schema inference |
| [03](docs/study-notes/day03-bronze-ingestion.md) | Bronze ingestion & DML | MERGE INTO, COPY INTO, DML |
| [04](docs/study-notes/day04-silver-preprocessing.md) | Silver preprocessing & Pandas UDFs | UDFs, transformations |
| [05](docs/study-notes/day05-silver-event-detection.md) | Silver event detection | Nested structs, explode, arrays |
| [06](docs/study-notes/day06-gold-aggregations.md) | Gold aggregations & window functions | Window functions, broadcast joins |
| [07](docs/study-notes/day07-delta-live-tables.md) | Delta Live Tables & data quality | DLT, expectations, CDC |
| [08](docs/study-notes/day08-unity-catalog.md) | Unity Catalog governance | Governance, column/row security |
| [09](docs/study-notes/day09-structured-streaming.md) | Structured Streaming | Watermarks, triggers, foreachBatch |
| [10](docs/study-notes/day10-mlflow.md) | MLflow & experiment tracking | Model Registry, batch scoring |
| [11](docs/study-notes/day11-cicd-dabs.md) | CI/CD & Databricks Asset Bundles | DABs, GitHub Actions, pytest |
| [12](docs/study-notes/day12-performance-tuning.md) | Performance tuning | OPTIMIZE, ZORDER, AQE |
| [13](docs/study-notes/day13-exam-mini-labs.md) | Exam mini-labs (6 scenarios) | All domains — revision |
| [14](docs/study-notes/day14-portfolio-capstone.md) | Portfolio & STAR stories | German-market positioning |

### **Phase 2: Professional Certification (Days 15-21)**

| Day | Topic | Key Exam Domain |
|---|---|---|
| [15](docs/study-notes/day15-data-source-connectors.md) | Data Source Connectors | JDBC, Cloud Storage, Streaming, Delta Sharing |
| [16](docs/study-notes/day16-delta-sharing.md) | Delta Sharing & Federation | Cross-workspace sharing, BI integration |
| [17](docs/study-notes/day17-monitoring-observability.md) | Monitoring & Observability | Query profiling, skew, memory optimization |
| [18](docs/study-notes/day18-dataset-integration.md) | PhysioNet Dataset Integration | Real EDF pipeline, production ingestion |
| [19](docs/study-notes/day19-security-compliance.md) | Security & Compliance | Row-level security, audit logging |
| [20](docs/study-notes/day20-databricks-cli-api.md) | Databricks CLI & API Automation | CLI commands, REST API, deployment |
| [21](docs/study-notes/day21-data-modeling.md) | Advanced Data Modeling & TDA | Dimensional modeling, Persistent Homology |

---

## Quick Start

```bash
git clone https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git
cd databricks-eeg-lakehouse-lab
pip install -e ".[dev]"
pytest tests/ -v
```

**Start learning:** open [`docs/study-notes/day01-repo-bootstrap.md`](docs/study-notes/day01-repo-bootstrap.md)

**Project overview:** [`docs/project-overview.md`](docs/project-overview.md)

**Implementation roadmap:** [`docs/IMPLEMENTATION-GUIDE.md`](docs/IMPLEMENTATION-GUIDE.md)

**Dataset integration:** [`docs/research/DATASET-INTEGRATION-GUIDE.md`](docs/research/DATASET-INTEGRATION-GUIDE.md)

**Professional exam prep:** [`PROFESSIONAL-EXAM-PREP-SUMMARY.md`](PROFESSIONAL-EXAM-PREP-SUMMARY.md)

---

## Technology Stack

Azure Databricks · Delta Lake · Auto Loader · Delta Live Tables · Delta Sharing · Unity Catalog · Structured Streaming · MLflow · PySpark · Pandas UDFs · GitHub Actions · Databricks Asset Bundles · Databricks CLI · pytest · Python 3.11

---

## Exam Coverage

### Associate Certification (Databricks DE Associate 2026)

| Domain | Weight | Status |
|---|---|---|
| Databricks Lakehouse Platform | ~24% | ✅ Day 1 |
| ELT with Spark SQL and Python | ~29% | ✅ Days 3–6, 13 |
| Incremental Data Processing | ~22% | ✅ Days 3, 7, 9 |
| Production Pipelines | ~16% | ✅ Days 7, 11, 12 |
| Data Governance | ~9% | ✅ Day 8 |

### Professional Certification (Databricks DE Professional)

| Domain | Weight | Status |
|---|---|---|
| Data Processing & Transformation | ~25% | ✅ Days 1-6, 15 |
| Data Governance & Security | ~20% | ✅ Days 8, 19 |
| Data Sharing & Collaboration | ~15% | ✅ Day 16 |
| Production & Operations | ~20% | ✅ Days 11, 17, 20 |
| Advanced Analytics & ML | ~20% | ✅ Days 10, 21 |

---

## Research Dataset

This project uses the **PhysioNet Sleep-EDF Expanded** dataset, a comprehensive collection of polysomnographic sleep recordings. The dataset includes EEG, EOG, and EMG signals from 200 subjects, making it ideal for developing production-scale data pipelines and ML-based sleep analysis.

**Key features:**
- 200 subjects with full-night sleep recordings
- Multiple EEG channels (Fpz-Cz, Pz-Oz)
- Expert-annotated sleep stages (W, N1, N2, N3, REM)
- Sampling rate: 100 Hz
- Format: European Data Format (EDF)

For detailed information on dataset download, schema design, and integration into the Databricks lakehouse, see [`docs/research/DATASET-INTEGRATION-GUIDE.md`](docs/research/DATASET-INTEGRATION-GUIDE.md).

---

## Research Component: Topological Data Analysis

Beyond standard sleep staging, this project explores **Topological Data Analysis (TDA)** using persistent homology to identify memory consolidation patterns during sleep. This advanced research component demonstrates expertise in:

- Persistent homology computation on time-series EEG data
- Dimensionality reduction and feature extraction from topological features
- Integration of computational neuroscience with production data engineering

See [Day 21: Data Modeling & TDA](docs/study-notes/day21-data-modeling.md) for details.

---

## License

MIT License - see LICENSE file for details
