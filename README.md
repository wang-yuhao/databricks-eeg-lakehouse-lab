# Databricks EEG Lakehouse Lab

> **Three purposes, one repo:** Databricks Data Engineer Exam Practice Lab · Sleep EEG Research Pipeline · Senior DE/DS Portfolio

[![CI](https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)]()
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-green)]()

---

## What This Repo Does

1. **Databricks Certified Data Engineer Practice Lab (2026 exam)** — every file maps to an explicit exam domain (Auto Loader, Delta, DLT, Unity Catalog, performance, streaming, MLflow, CI/CD). 14 daily study notes live in `docs/study-notes/`.

2. **Production-style Sleep EEG Research Pipeline** — processes PhysioNet Sleep-EDF Expanded data (N=200 subjects) through Bronze → Silver → Gold Delta tables, culminating in sleep feature extraction and ML-based sleep staging.

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

  Governance: Unity Catalog (catalog.schema.table) + column masking + row filters
  Streaming:  Structured Streaming with watermarks + foreachBatch MERGE
  CI/CD:      GitHub Actions (pytest) + Databricks Asset Bundles
```

---

## Repo Structure

```
databricks-eeg-lakehouse-lab/
├── README.md                 # This file
├── databricks.yml            # Databricks Asset Bundle
├── pyproject.toml            # Python packaging & tool config
├── CHANGELOG.md              # Version history
├── .github/workflows/ci.yml  # GitHub Actions CI
├── src/                      # Production Python source
│   ├── bronze/               # Auto Loader ingestion
│   ├── silver/               # UDFs: event detection, preprocessing
│   ├── gold/                 # Feature engineering
│   ├── dlt/                  # Delta Live Tables pipeline
│   ├── streaming/            # Structured Streaming processor
│   ├── ml/                   # MLflow model classes
│   └── utils/                # Shared helpers
├── notebooks/                # Day-by-day Databricks notebooks
├── tests/                    # pytest unit tests
└── docs/
    ├── project-overview.md   # Architecture map & 14-day schedule
    ├── daily-plan.md         # Daily task tracker
    ├── study-notes/          # 14 daily exam study notes
    ├── exam/                 # Deep-dive exam reference docs
    └── research/             # EEG neuroscience background
```

---

## 14-Day Program

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

---

## Technology Stack

Azure Databricks · Delta Lake · Auto Loader · Delta Live Tables · Unity Catalog ·
Structured Streaming · MLflow · PySpark · Pandas UDFs · GitHub Actions ·
Databricks Asset Bundles · pytest · Python 3.11

---

## Exam Coverage (Databricks DE Associate 2026)

| Domain | Weight | Status |
|---|---|---|
| Databricks Lakehouse Platform | ~24% | ✅ Day 1 |
| ELT with Spark SQL and Python | ~29% | ✅ Days 3–6, 13 |
| Incremental Data Processing | ~22% | ✅ Days 3, 7, 9 |
| Production Pipelines | ~16% | ✅ Days 7, 11, 12 |
| Data Governance | ~9% | ✅ Day 8 |

---

*Built as a 14-day intensive study programme combining exam preparation with
production data engineering on real sleep EEG research data.*
