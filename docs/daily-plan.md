# 14-Day Program: Databricks EEG Lakehouse Lab

> **Format:** Each day = 2–3 focused tasks · ~2–3 hours · Commit at end of session.

---

## Week 1 — Lakehouse Pipeline

### Day 1 — Repo Bootstrap & Goals
- [x] Create repo, README, requirements.txt, databricks.yml skeleton
- [x] Write `docs/daily-plan.md` (this file)
- [x] Write `docs/exam/domains-overview.md`
- [x] Write `docs/research/project-overview.md`
- [x] Write `src/utils/config.py` skeleton

**Deliverable:** Runnable `python -c "from src.utils.config import PipelineConfig; print(PipelineConfig())"` succeeds.

---

### Day 2 — Dataset Interface & Bronze Schema Design
- [ ] Write `docs/research/dataset-notes.md` (Sleep-EDF field descriptions, file layout, approximate volume)
- [ ] Extend `src/utils/config.py` with Bronze/Silver/Gold table names and UC paths
- [ ] Sketch `src/bronze/ingest_eeg_files.py` — schema definition + `load_raw_files()` stub
- [ ] Add notebook `notebooks/day02_bronze_schema_design.py`

**Deliverable:** `spark.read.format("binaryFile")` on a local EDF loads without error; Bronze schema printed.

---

### Day 3 — Bronze Ingestion with Auto Loader Patterns
- [ ] Add `create_bronze_table()` with Auto Loader (`.format("cloudFiles")`) to `src/bronze/ingest_eeg_files.py`
- [ ] Add notebook `notebooks/day03_bronze_ingestion.py` (DESCRIBE HISTORY + SELECT)
- [ ] Add `tests/test_bronze.py` with subject-ID extraction and schema invariant tests

**Exam focus:** Auto Loader, schema evolution, Delta basics.  
**Deliverable:** `pytest tests/test_bronze.py` passes.

---

### Day 4 — Silver Preprocessing (EEG Cleaning) in PySpark
- [ ] Write `src/silver/preprocess_eeg.py` with `preprocess_eeg(df_bronze)` using Pandas UDF (mock MNE)
- [ ] Add notebook `notebooks/day04_silver_preprocessing.py`
- [ ] Add `tests/test_silver.py` — schema + null-row tests

**Exam focus:** Transformations domain, UDF vs Pandas UDF, `withColumn`, `filter`.  
**Research link:** Scaffold for real MNE bandpass + ICA pipeline in Phase 2.

---

### Day 5 — Silver Event Detection (Spindles & SOs)
- [ ] Write `src/silver/detect_events.py` with `detect_spindles()` returning ArrayType of event structs
- [ ] Add notebook `notebooks/day05_event_detection.py` (`explode`, window aggregations)
- [ ] Update `docs/exam/delta-patterns.md` with Silver partitioning notes

**Exam focus:** Complex transformations, nested data types, `explode`, `struct`.  
**Research link:** YASA spindle/SO catalogs → TDA point clouds in Day 6.

---

### Day 6 — Gold Feature Table & Delta Optimization
- [ ] Write `src/gold/build_features.py` with `build_feature_table()` join + TMCI proxy column
- [ ] Add notebook `notebooks/day06_gold_features.py` (OPTIMIZE, ZORDER, DESCRIBE DETAIL)
- [ ] Add `tests/test_gold.py` — feature column existence + computed value sanity checks

**Exam focus:** Data modeling, OPTIMIZE/ZORDER, Delta history, feature store patterns.

---

### Day 7 — DLT Pipeline Skeleton
- [ ] Annotate `src/bronze/*.py`, `src/silver/*.py`, `src/gold/*.py` with `@dlt.table` + `@dlt.expect`
- [ ] Create `resources/eeg_dlt_pipeline.yml`
- [ ] Write `docs/exam/dlt-cheatsheet.md`

**Exam focus:** DLT domain — live vs streaming tables, expectations, `dlt.read` vs `dlt.read_stream`.  
**Deliverable:** DLT pipeline JSON deployable via Databricks CLI.

---

## Week 2 — Governance, Streaming, MLflow, CI/CD

### Day 8 — Unity Catalog & Governance
- [ ] Add notebook `notebooks/day08_unity_catalog_setup.py` (CREATE CATALOG / SCHEMA / VOLUME)
- [ ] Extend `src/utils/config.py` with UC helpers
- [ ] Write `docs/exam/uc-governance.md` (GRANT patterns, row-level security)

---

### Day 9 — Streaming Path (Simulated Live EEG)
- [ ] Write `src/bronze/ingest_streaming_events.py` (rate source → structured streaming → Bronze append)
- [ ] Add notebook `notebooks/day09_streaming_ingestion.py` (watermarks, window aggregations)
- [ ] Update `docs/exam/delta-patterns.md` with streaming vs batch notes

---

### Day 10 — MLflow Integration
- [ ] Write `src/gold/train_ml_model.py` (load Gold, train XGBoost on TMCI, log to MLflow)
- [ ] Add notebook `notebooks/day10_mlflow_training.py`
- [ ] Write `docs/research/analysis-questions.md`

---

### Day 11 — CI/CD & Tests
- [ ] Add `.github/workflows/ci.yml` (pytest + black + flake8)
- [ ] Ensure test coverage across Bronze/Silver/Gold
- [ ] Write `docs/exam/ci-cd-notes.md`

---

### Day 12 — Performance & Cost
- [ ] Write `docs/exam/performance-tuning.md` (partitioning vs ZORDER, AQE, broadcast joins)
- [ ] Add notebook `notebooks/day12_performance_experiments.py`

---

### Day 13 — Exam Pattern Mini-Labs
- [ ] Write `docs/exam/patterns-lab.md` (CTAS, MERGE INTO, time travel, VACUUM)
- [ ] Add notebook `notebooks/day13_exam_mini_labs.py`

---

### Day 14 — Portfolio Polish
- [ ] Update README with exam-domain mapping table and STAR interview stories
- [ ] Finalize `docs/daily-plan.md` (tick off everything, add "After 14 Days" section)
- [ ] Update `docs/research/project-overview.md` with Ngo 2020 / Staresina links

---

## After 14 Days

- Extend pipeline with full real MNE preprocessing (bandpass, ICA, YASA sleep staging)
- Implement Vietoris-Rips filtrations with Ripser on real EDF pilot subjects
- Add second domain notebook: supply-chain analytics on synthetic dataset
- Submit Databricks Certified Data Engineer Associate exam registration
