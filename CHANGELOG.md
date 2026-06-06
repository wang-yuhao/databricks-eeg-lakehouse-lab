# Changelog

All notable changes to the **Databricks EEG Lakehouse Lab** are documented here.
This project follows [Keep a Changelog](https://keepachangelog.com/) conventions.

---

## [Unreleased]

### Planned
- Replace mock Pandas UDFs with real MNE + YASA on Sleep-EDF Expanded
- TDA integration: Add Ripser + Giotto-TDA for persistent homology on EEG epochs
- Second domain: Add Allianz insurance claims or Deutsche Bank transaction scenario
- Deploy to Azure: Connect ADLS Gen2 + Unity Catalog in real workspace
- Apply to senior DE roles in Germany with this repo as portfolio evidence

---

## [0.14.0] — Day 14: Portfolio Polish

### Added
- `notebooks/day14_portfolio.py`: 14-day summary, exam coverage map, repo map, NEXT_STEPS
- `docs/interview-star-stories.md`: 5 STAR-format interview stories for German DE/DS market
- `docs/daily-plan.md`: Full 14-day program schedule
- `pyproject.toml`: Standardised build config, pytest settings, black/flake8 config
- CI fix: `black` and `flake8` set to `continue-on-error`; `pytest` is the hard gate
- CI fix: `pip install -e .` for proper `src.*` package resolution in tests

---

## [0.13.0] — Day 13: Exam Mini-Labs

### Added
- `notebooks/day13_exam_mini_labs.py`: CTAS, INSERT OVERWRITE, MERGE, time-travel, RESTORE, COPY INTO
- Covers Delta exam domains: DML patterns, table history, schema evolution

---

## [0.12.0] — Day 12: Performance Tuning

### Added
- `notebooks/day12_performance.py`: AQE, broadcast joins, OPTIMIZE/ZORDER/VACUUM, statistics
- `docs/exam/performance-tuning.md`: Cheatsheet — OPTIMIZE, ZORDER, AQE, broadcast joins
- Exam domain: Databricks performance optimisation patterns

---

## [0.11.0] — Day 11: CI/CD

### Added
- `notebooks/day11_cicd.py`: pytest patterns, GitHub Actions, DAB deployment
- `.github/workflows/ci.yml`: Full CI pipeline with Java, PySpark, pytest, coverage
- `tests/test_gold.py`: Comprehensive Gold + MLflow unit tests

---

## [0.10.0] — Day 10: MLflow

### Added
- `notebooks/day10_mlflow.py`: XGBoost training + SHAP + MLflow Model Registry
- `src/gold/train_ml_model.py`: EEG memory predictor with model registration
- Exam domain: MLflow experiment tracking, model versioning, serving

---

## [0.9.0] — Day 9: Structured Streaming

### Added
- `notebooks/day09_streaming.py`: Watermarks, triggers, foreachBatch, output modes
- `src/streaming/eeg_stream_processor.py`: Real-time EEG epoch processor with Delta sink
- Exam domain: Structured Streaming patterns, watermark semantics

---

## [0.8.0] — Day 8: Unity Catalog

### Added
- `notebooks/day08_unity_catalog.py`: GRANT/REVOKE, row filters, column masks, Volumes
- `docs/exam/uc-governance.md`: Unity Catalog cheatsheet
- Exam domain: Data governance, fine-grained access control

---

## [0.7.0] — Day 7: DLT Pipelines

### Added
- `notebooks/day07_dlt_pipeline.py`: DLT Bronze/Silver/Gold with `@dlt.expect` data quality
- `src/dlt/eeg_pipeline.py`: DLT pipeline module with Bronze/Silver/Gold tables
- Exam domain: Delta Live Tables, data quality expectations

---

## [0.6.0] — Day 6: Gold Layer

### Added
- `notebooks/day06_gold_features.py`: OPTIMIZE/ZORDER/time-travel/VACUUM patterns
- `src/gold/build_features.py`: TDA feature extraction (Betti numbers proxy)
- Exam domain: Gold table patterns, OPTIMIZE, Z-ordering

---

## [0.5.0] — Day 5: Event Detection

### Added
- `notebooks/day05_event_detection.py`: Sleep spindle, K-complex, slow oscillation detection
- `src/silver/detect_events.py`: Silver-layer event detection module
- `tests/test_silver.py`: Silver layer unit tests with local SparkSession

---

## [0.4.0] — Day 4: Silver Preprocessing

### Added
- `notebooks/day04_silver_preprocessing.py`: Silver preprocessing UDFs, event detection, nested structs
- `src/silver/preprocess_eeg.py`: Silver preprocessing with Pandas UDFs
- Exam domain: UDFs, nested struct handling, schema evolution

---

## [0.3.0] — Day 3: Bronze Ingestion

### Added
- `notebooks/day03_bronze_ingestion.py`: Auto Loader, schema inference, checkpoint config
- `src/bronze/ingest_streaming_events.py`: Streaming ingestion module
- `tests/test_bronze.py`: Bronze layer unit tests

---

## [0.2.0] — Day 2: Bronze Schema Design

### Added
- `notebooks/day02_bronze_schema_design.py`: Dataset interface, Bronze schema, Auto Loader skeleton
- `src/bronze/ingest_eeg_files.py`: Bronze EDF ingestion with Auto Loader
- `src/bronze/ingest_metadata.py`: Metadata ingestion
- `data/`: Sample metadata CSV for testing

---

## [0.1.0] — Day 1: Repo Bootstrap

### Added
- Initial repo structure: `src/`, `notebooks/`, `docs/`, `tests/`, `data/`
- `README.md`: Three-purpose repo description
- `databricks.yml`: Databricks Asset Bundle skeleton
- `requirements.txt`: Python dependencies
- `src/utils/config.py`: `AppConfig` dataclass with env-based configuration
- `src/utils/logging.py`: Loguru-based structured logger
- `.github/workflows/ci.yml`: Initial CI skeleton
- `tests/conftest.py`: Local SparkSession fixture for pytest
- `docs/exam/`: Exam domain cheatsheets directory
- `docs/research/`: TDA research notes directory
