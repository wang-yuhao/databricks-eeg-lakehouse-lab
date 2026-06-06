# 14-Day EEG Lakehouse Lab — Daily Plan

> **Goal:** Complete Databricks Data Engineer exam prep + production EEG pipeline + portfolio, in 2-hour daily sessions.

---

## WEEK 1 — Build the Lakehouse Around the EEG Experiment

### ✅ Day 1 — Repo Bootstrap & Goal Alignment
**Session time: ~2 hours**

#### Tasks:
- [x] **T1.1** Create repository `databricks-eeg-lakehouse-lab` on GitHub
- [x] **T1.2** Write `README.md` with full architecture diagram, goals, exam domain table
- [x] **T1.3** Create `docs/daily-plan.md` (this file) with 14-day checklist
- [x] **T1.4** Create `docs/exam/domains-overview.md` — map all exam domains to repo files
- [x] **T1.5** Create `docs/research/project-overview.md` — describe the sleep EEG TDA experiment
- [x] **T1.6** Create `src/utils/config.py` skeleton
- [x] **T1.7** Create `requirements.txt`
- [x] **T1.8** Create `databricks.yml` bundle skeleton

**Why it matters:**
- 🎓 **Exam:** Forces you to survey all domains up front and identify gaps before coding begins.
- 🔬 **Research:** Documents the experimental design (TDA pipeline, datasets, hypotheses) before implementation so you never lose the "why."
- 💼 **Interview:** Hiring managers read READMEs first. A clear README with architecture diagrams signals senior ownership immediately.

---

### Day 2 — Dataset Interface & Bronze Schema Design
**Session time: ~2 hours**

#### Tasks:
- [ ] **T2.1** Create `docs/research/dataset-notes.md` — EDF format, PhysioNet paths, channel layout, volume estimate
- [ ] **T2.2** Extend `src/utils/config.py` with full dataclasses (paths, catalog, schema, Spark config)
- [ ] **T2.3** Create `src/bronze/ingest_eeg_files.py` — schema definition + `load_raw_files()` skeleton
- [ ] **T2.4** Create `notebooks/day02_bronze_schema_design.py` — interactive exploration notebook

**Why it matters:**
- 🎓 **Exam:** Bronze schema design maps directly to the Auto Loader & Delta schema enforcement domain. Understanding `mergeSchema`, `cloudFiles.schemaLocation` is testable.
- 🔬 **Research:** Defining the data contract for EDF files (subject_id, session, channel_count, sampling_rate) prevents schema drift as you scale to 200 subjects.
- 💼 **Interview:** Being able to explain "I defined explicit Bronze schemas and used Auto Loader's schema evolution controls" signals production maturity.

---

### Day 3 — Bronze Ingestion with Databricks Patterns
**Session time: ~2–3 hours**

#### Tasks:
- [ ] **T3.1** Extend `src/bronze/ingest_eeg_files.py` — add `create_bronze_table()` with Auto Loader / cloudFiles pattern
- [ ] **T3.2** Add `src/bronze/ingest_metadata.py` — CSV subject metadata ingestion
- [ ] **T3.3** Create `notebooks/day03_bronze_ingestion.py` — run ingestion, show `DESCRIBE HISTORY`, `SELECT` from Bronze
- [ ] **T3.4** Create `tests/test_bronze.py` — unit tests: subject_id extraction, schema invariants, null checks

**Why it matters:**
- 🎓 **Exam:** Auto Loader (`format("cloudFiles")`) is the primary ingestion pattern tested. The `DESCRIBE HISTORY` command verifies Delta audit logging.
- 🔬 **Research:** Idempotent ingestion (Auto Loader checkpointing) means you can re-run without re-ingesting files — critical for 197-subject EDF corpus.
- 💼 **Interview:** Demonstrates you know production ingestion patterns, not just `spark.read.csv()`.

---

### Day 4 — Silver Preprocessing (EEG Cleaning) in PySpark
**Session time: ~2–3 hours**

#### Tasks:
- [ ] **T4.1** Create `src/silver/preprocess_eeg.py` — Pandas UDF skeleton for bandpass filter + artifact flagging
- [ ] **T4.2** Create `notebooks/day04_silver_preprocessing.py` — load Bronze, apply UDF, write Silver Delta table
- [ ] **T4.3** Extend `tests/test_silver.py` — schema validation, null flagging, output column existence
- [ ] **T4.4** Update `docs/exam/delta-patterns.md` — note on Pandas UDF vs Arrow UDF, when to use each

**Why it matters:**
- 🎓 **Exam:** Pandas UDFs (Arrow-based) are a major data processing topic — knowing when to use `@pandas_udf` vs `mapInPandas` vs regular Python UDFs is testable.
- 🔬 **Research:** This is where real MNE preprocessing (bandpass 0.5–40 Hz, notch 50 Hz, ICA artifact rejection) gets plugged in later. The scaffold works with mock data first.
- 💼 **Interview:** Shows you can apply domain-specific libraries (MNE) within a distributed Spark context using vectorized UDFs.

---

### Day 5 — Silver Event Detection + Feature Hints
**Session time: ~2 hours**

#### Tasks:
- [ ] **T5.1** Create `src/silver/detect_events.py` — `detect_spindles()` and `detect_slow_oscillations()` returning nested structs
- [ ] **T5.2** Create `notebooks/day05_event_detection.py` — run detection, `explode()` events, aggregate per subject
- [ ] **T5.3** Update `docs/exam/delta-patterns.md` — nested struct types, `explode()`, `flatten()`, array aggregations
- [ ] **T5.4** Add tests to `tests/test_silver.py` — event schema shape, event count sanity checks

**Why it matters:**
- 🎓 **Exam:** Nested data types (StructType, ArrayType), `explode()`, `transform()`, `aggregate()` are commonly tested transformation topics.
- 🔬 **Research:** Spindle and SO event catalogs are the core data products of the sleep pipeline — all downstream TDA analysis depends on these timestamps.
- 💼 **Interview:** Demonstrates you can model domain-specific event data (start_time, duration, channel, amplitude) in Spark-native types.

---

### Day 6 — Gold Feature Table + ML-Ready Design
**Session time: ~2–3 hours**

#### Tasks:
- [ ] **T6.1** Create `src/gold/build_features.py` — `build_feature_table()` joining Silver tables, computing spindle density, PAC proxy, σ-power
- [ ] **T6.2** Create `notebooks/day06_gold_features.py` — write Gold Delta, run `OPTIMIZE`, `ZORDER BY(subject_id, spindle_density)`, `DESCRIBE DETAIL`
- [ ] **T6.3** Create `tests/test_gold.py` — feature column validation on small test dataset
- [ ] **T6.4** Update `docs/exam/delta-patterns.md` — OPTIMIZE, ZORDER, data skipping, liquid clustering

**Why it matters:**
- 🎓 **Exam:** `OPTIMIZE`, `ZORDER`, `VACUUM`, file compaction, and liquid clustering are core Delta optimization topics.
- 🔬 **Research:** Gold features (spindle density, PAC modulation index, σ-power) are the inputs to TDA and ML model training.
- 💼 **Interview:** Shows you design ML feature tables with downstream consumers in mind — wide table design, proper partitioning strategy.

---

### Day 7 — DLT Pipeline Skeleton
**Session time: ~2–3 hours**

#### Tasks:
- [ ] **T7.1** Create `resources/eeg_dlt_pipeline.yml` — full DLT pipeline YAML configuration
- [ ] **T7.2** Add `@dlt.table` decorators and `@dlt.expect` quality rules to Bronze/Silver/Gold source files
- [ ] **T7.3** Create `notebooks/day07_dlt_pipeline.py` — DLT notebook with Bronze + Silver + Gold tables
- [ ] **T7.4** Create `docs/exam/dlt-cheatsheet.md` — `dlt.read` vs `dlt.read_stream`, expect types, pipeline modes

**Why it matters:**
- 🎓 **Exam:** DLT is a primary domain in the 2026 exam — `@dlt.table`, `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`, LIVE tables vs streaming tables.
- 🔬 **Research:** DLT enforces data quality expectations (e.g., `expect("valid_subject", "subject_id IS NOT NULL")`) automatically across all EEG ingestion runs.
- 💼 **Interview:** DLT knowledge differentiates Databricks practitioners from generic Spark users — be prepared to compare DLT vs manual pipeline orchestration.

---

## WEEK 2 — Governance, Streaming, MLflow, CI/CD, Exam Practice

### Day 8 — Unity Catalog & Governance
**Session time: ~2 hours**

#### Tasks:
- [ ] **T8.1** Create `notebooks/day08_unity_catalog_setup.py` — SQL to create catalog, schemas, volumes
- [ ] **T8.2** Extend `src/utils/config.py` — UC-aware FQN (fully qualified name) helpers
- [ ] **T8.3** Create `docs/exam/uc-governance.md` — GRANT syntax, row-level security, lineage, data discovery

**Why it matters:**
- 🎓 **Exam:** Unity Catalog (catalog/schema/table/volume hierarchy, GRANT/REVOKE, row filters, column masks) is a dedicated exam domain.
- 🔬 **Research:** UC enables proper access governance for EEG subject data — researchers can query Gold features without accessing raw EDF binaries.
- 💼 **Interview:** Governance is a top concern at large German enterprises (GDPR compliance, data lineage for audit). UC knowledge is highly valued.

---

### Day 9 — Streaming Path (Simulated Live EEG)
**Session time: ~2 hours**

#### Tasks:
- [ ] **T9.1** Create `src/bronze/ingest_streaming_events.py` — Structured Streaming from rate source → Bronze streaming table
- [ ] **T9.2** Create `notebooks/day09_streaming_ingestion.py` — start streaming query, watermarks, window aggregations
- [ ] **T9.3** Update `docs/exam/delta-patterns.md` — streaming vs batch ingestion, watermarks, `outputMode("append")`

**Why it matters:**
- 🎓 **Exam:** Structured Streaming — trigger types, watermarks, output modes, `readStream`/`writeStream` — is a distinct exam domain.
- 🔬 **Research:** Future extension: real-time EEG from wearables (Dreem, Muse) would use this streaming path. The scaffold is ready.
- 💼 **Interview:** "I built a streaming pipeline for real-time sensor events" maps directly to BMW telematics, Deutsche Bank trading events, Siemens IoT use cases.

---

### Day 10 — MLflow Integration
**Session time: ~2–3 hours**

#### Tasks:
- [ ] **T10.1** Create `src/gold/train_ml_model.py` — RF/XGBoost on Gold TDA features, MLflow logging
- [ ] **T10.2** Create `notebooks/day10_mlflow_training.py` — end-to-end training run, MLflow UI walkthrough
- [ ] **T10.3** Create `docs/research/analysis-questions.md` — research questions the model addresses

**Why it matters:**
- 🎓 **Exam:** MLflow (experiments, runs, model registry, model serving) is covered in the Databricks exam as part of the ML lifecycle.
- 🔬 **Research:** This directly tests H3 (TDA features improve prediction of memory proxies over spectral baselines by ΔR² > 0.10).
- 💼 **Interview:** MLflow logging with SHAP explanations shows you produce reproducible, interpretable models — senior-level MLOps thinking.

---

### Day 11 — CI/CD & Tests
**Session time: ~2 hours**

#### Tasks:
- [ ] **T11.1** Create `.github/workflows/ci.yml` — pytest + black --check + flake8 on every push
- [ ] **T11.2** Create `tests/conftest.py` — shared Spark fixtures for all test modules
- [ ] **T11.3** Ensure test coverage across Bronze, Silver, Gold modules
- [ ] **T11.4** Create `docs/exam/ci-cd-notes.md` — Databricks CLI, bundle deploy, Repos, SDLC patterns

**Why it matters:**
- 🎓 **Exam:** The 2026 exam includes Databricks CLI, Git integration, bundle deploy, and CI/CD concepts.
- 🔬 **Research:** Reproducibility requires automated testing — every new EEG processing function must pass schema and logic tests before deployment.
- 💼 **Interview:** A repo with working CI/CD signals that you operate at senior engineering standards, not data analyst level.

---

### Day 12 — Performance & Cost Optimization
**Session time: ~2 hours**

#### Tasks:
- [ ] **T12.1** Create `docs/exam/performance-tuning.md` — partitioning, ZORDER, AQE configs, broadcast joins, caching
- [ ] **T12.2** Create `notebooks/day12_performance_experiments.py` — benchmark before/after OPTIMIZE/ZORDER, show query plans
- [ ] **T12.3** Add AQE and broadcast join configs to `src/utils/config.py`

**Why it matters:**
- 🎓 **Exam:** Query optimization (AQE, broadcast joins, predicate pushdown, Z-ordering) is a dedicated performance domain.
- 🔬 **Research:** TDA computation on 197 subjects × 2 nights of 19-channel EEG generates large intermediate DataFrames — broadcast joining small lookup tables (subject demographics) avoids shuffle.
- 💼 **Interview:** "I optimized a 500GB Delta table query from 8 minutes to 45 seconds using ZORDER and AQE" is a memorable interview story.

---

### Day 13 — Exam Pattern Mini-Labs
**Session time: ~2 hours**

#### Tasks:
- [ ] **T13.1** Create `docs/exam/patterns-lab.md` — CTAS, MERGE INTO, time travel, VACUUM worked examples
- [ ] **T13.2** Create `notebooks/day13_exam_mini_labs.py` — implement all patterns against EEG Delta tables

**Why it matters:**
- 🎓 **Exam:** CTAS, `MERGE INTO` (upserts), `VERSION AS OF`, `RESTORE TO`, `VACUUM` with retention period are very frequently tested.
- 🔬 **Research:** MERGE INTO enables incremental updates to the Gold feature table when new subjects are added.
- 💼 **Interview:** Recalling concrete examples from your own pipeline ("I used MERGE INTO on the subject Gold table to handle reprocessed subjects") is far more convincing than reciting docs.

---

### Day 14 — Documentation Polish & Interview Narratives
**Session time: ~2 hours**

#### Tasks:
- [ ] **T14.1** Update `README.md` — add exam domain coverage table, STAR interview stories
- [ ] **T14.2** Finalize `docs/daily-plan.md` — check off everything, add "After 14 Days" suggestions
- [ ] **T14.3** Update `docs/research/project-overview.md` — link EEG pipeline to Ngo 2020 / Staresina work

**Why it matters:**
- 🎓 **Exam:** Review pass — the repo now covers all major exam domains with concrete, memorable examples.
- 🔬 **Research:** A polished research overview makes this submittable as a supplementary portfolio for PhD / postdoc applications.
- 💼 **Interview:** Concrete STAR stories about this pipeline (scale, decisions, outcomes) are your most powerful interview tool.

---

## After 14 Days — Suggested Extensions

- **Domain extension:** Reproduce Ngo et al. 2020 (targeted memory reactivation with auditory cues) using the MASSDB dataset
- **Advanced TDA:** Implement zigzag persistence for formal topological trajectory tracking across sleep cycles
- **Production deployment:** Deploy Gold feature table as a Databricks SQL endpoint + Grafana dashboard
- **Certification:** Schedule the Databricks Certified Data Engineer Associate exam
- **Second domain:** Finance data engineering use case (supply chain, trading events) to broaden portfolio
