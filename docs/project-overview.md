# Project Overview: Databricks EEG Lakehouse Lab

> **A 14-day intensive lab combining Databricks Data Engineer exam preparation
> with a production-grade EEG sleep science pipeline — built as a professional
> portfolio for the German data engineering market.**

---

## Three Purposes of This Repo

```
┌───────────────────────────────────────────────────────────────────┐
│  1. EXAM PREP          Databricks Data Engineer Associate (2026)  │
│  2. RESEARCH PIPELINE  Production EEG sleep science data platform  │
│  3. PORTFOLIO PROJECT  German-market senior DE/DS positioning      │
└───────────────────────────────────────────────────────────────────┘
```

---

## Architecture Overview

```
                        EEG DATA FLOW

EDF Files (ADLS) ──[Auto Loader]──► BRONZE ──[DLT Pipeline]──► SILVER ──[Aggregations]──► GOLD
                                     raw_signals    epochs, events    subject_features
                                                         │
                                              [MLflow Models]
                                                         │
                                              sleep_stage predictions

                        GOVERNANCE LAYER
       Unity Catalog (catalog.schema.table) + Column Masking + Row Filters + Lineage

                        STREAMING PATH
  EEG Device ─► Kafka/Event Hub ─► Structured Streaming (watermark) ─► Silver MERGE

                        CI/CD
  GitHub Actions (pytest) + Databricks Asset Bundles (dev/prod deploy)
```

---

## Repo Structure

```
databricks-eeg-lakehouse-lab/
├── README.md                      # Entry point; badges; quick start
├── databricks.yml                 # Databricks Asset Bundle definition
├── pyproject.toml                 # Python deps, Black, pytest config
├── CHANGELOG.md                   # Version history
├── .github/
│   └── workflows/ci.yml            # GitHub Actions CI pipeline
├── src/
│   ├── bronze/                    # Auto Loader ingestion
│   ├── silver/                    # UDFs: detect_events, preprocess
│   ├── gold/                      # Feature engineering
│   ├── dlt/                       # Delta Live Tables pipeline
│   ├── streaming/                 # Structured streaming processor
│   ├── ml/                        # MLflow models
│   └── utils/                     # Shared helpers
├── notebooks/
│   ├── day01_lakehouse_intro.py
│   ├── day02_bronze_schema.py
│   ├── day03_bronze_ingestion.py
│   ├── day04_silver_preprocessing.py
│   ├── day05_silver_events.py
│   ├── day06_gold_features.py
│   ├── day07_dlt_pipeline.py
│   ├── day08_unity_catalog.py
│   ├── day09_streaming.py
│   ├── day10_mlflow.py
│   ├── day11_cicd.py
│   ├── day12_performance.py
│   ├── day13_exam_mini_labs.py
│   └── day14_portfolio.py
├── tests/
│   ├── conftest.py                # SparkSession fixture
│   ├── test_bronze.py
│   ├── test_silver.py
│   ├── test_gold.py
│   └── ...
└── docs/
    ├── project-overview.md        # THIS FILE
    ├── study-notes/               # 14 daily study notes (exam prep)
    │   ├── day01-repo-bootstrap.md
    │   ├── day02-bronze-schema-design.md
    │   ├── ...
    │   └── day14-portfolio-capstone.md
    ├── exam/                      # Deep-dive exam reference docs
    │   └── uc-governance.md
    └── research/                  # EEG/neuroscience background
```

---

## 14-Day Schedule

| Day | Topic | Exam Domain | EEG Context |
|---|---|---|---|
| 1 | Repo bootstrap & Lakehouse | Lakehouse Platform, Delta Lake | Project setup, EDF file format |
| 2 | Bronze schema design | Auto Loader, schema inference | EEG metadata, channel layout |
| 3 | Bronze ingestion | MERGE INTO, DML operations | Multi-subject batch ingestion |
| 4 | Silver preprocessing | Pandas UDFs, transformations | Artefact rejection, bandpass filter |
| 5 | Silver event detection | Nested structs, explode | Spindle & slow oscillation detection |
| 6 | Gold aggregations | Window functions, broadcast joins | Sleep feature engineering |
| 7 | Delta Live Tables | DLT, expectations, CDC | Quality-gated EEG pipeline |
| 8 | Unity Catalog | Governance, column/row security | GDPR-compliant subject data |
| 9 | Structured Streaming | Watermarks, triggers, foreachBatch | Real-time sleep staging |
| 10 | MLflow | Experiment tracking, Model Registry | Sleep stage classifier |
| 11 | CI/CD & DABs | Asset Bundles, GitHub Actions | Clinical pipeline reliability |
| 12 | Performance tuning | OPTIMIZE, ZORDER, AQE | EEG data layout optimisation |
| 13 | Exam mini-labs | All domains (revision) | End-to-end scenario labs |
| 14 | Portfolio & capstone | — | STAR stories, German market prep |

---

## Technology Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Azure Databricks, ADLS Gen2 |
| Data Format | Delta Lake (Parquet + transaction log) |
| Ingestion | Auto Loader (`cloudFiles`), Kafka |
| Transformation | PySpark, Pandas UDFs, SQL |
| Orchestration | Delta Live Tables, Databricks Workflows |
| Governance | Unity Catalog |
| ML Platform | MLflow |
| Streaming | Structured Streaming |
| CI/CD | GitHub Actions, Databricks Asset Bundles |
| Testing | pytest, local SparkSession |
| Language | Python 3.11, SQL |

---

## Databricks DE Associate Exam Coverage

| Exam Domain (% weight) | Covered | Notes |
|---|---|---|
| Databricks Lakehouse Platform (~24%) | ✅ | Day 1, README |
| ELT with Spark SQL and Python (~29%) | ✅ | Days 3-6, 13 |
| Incremental Data Processing (~22%) | ✅ | Days 3, 7, 9 |
| Production Pipelines (~16%) | ✅ | Days 7, 11, 12 |
| Data Governance (~9%) | ✅ | Day 8 |

---

## Quick Start

```bash
# Clone the repo
git clone https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git
cd databricks-eeg-lakehouse-lab

# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Start with Day 1 study notes
open docs/study-notes/day01-repo-bootstrap.md
```

---

## Navigation

- **Start here:** `docs/study-notes/day01-repo-bootstrap.md`
- **Pipeline code:** `src/bronze/`, `src/silver/`, `src/gold/`
- **Notebooks:** `notebooks/day01_lakehouse_intro.py` → `day14_portfolio.py`
- **Tests:** `tests/`
- **Exam reference:** `docs/exam/uc-governance.md`
- **Portfolio stories:** `docs/study-notes/day14-portfolio-capstone.md`
