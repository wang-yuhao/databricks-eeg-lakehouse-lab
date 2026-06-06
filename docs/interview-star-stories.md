# Interview STAR Stories — Wang Yuhao (BCI)
## Senior Data Engineer / Data Scientist Roles (Germany 2026)

> Use these structured STAR narratives for Siemens, BMW, Allianz, Deutsche Bank,
> and similar German senior DE/DS interviews. Each story links to repo code.

---

## STORY 1: Production-Grade Medallion Pipeline (Data Engineering)

**Target question**: "Tell me about a complex data pipeline you built from scratch."

### Situation
Sleep research produces large EDF (European Data Format) files — one 8-hour recording
per subject per night. With 197 subjects over multiple nights, we had ~50 GB of raw
EEG data that needed to be ingested, cleaned, and transformed into ML-ready features
for a memory consolidation study (published framework: Ngo et al. 2020).

### Task
Design and implement a production-quality Bronze/Silver/Gold lakehouse pipeline on
Databricks + Azure Data Lake Storage Gen2 that:
- Ingests EDF files incrementally using Auto Loader
- Cleans signals and detects sleep oscillations (spindles, slow oscillations)
- Produces a feature table for XGBoost memory predictor
- Meets clinical data governance requirements (GDPR, audit trail)

### Action
1. **Bronze layer** (`src/bronze/ingest_eeg_files.py`): Used Databricks Auto Loader
   (`cloudFiles` format) to ingest EDF binary files incrementally from ADLS volumes.
   Schema inference + `rescuedDataColumn` for drift handling.
2. **Silver layer** (`src/silver/preprocess_eeg.py`): Pandas UDFs with MNE-Python
   for bandpass filtering (0.5-30 Hz) and epoch segmentation. Applied
   `@dlt.expect_or_drop` constraints on amplitude bounds to ensure signal quality.
3. **Event detection** (`src/silver/detect_events.py`): YASA-based spindle/SO
   detection in Pandas UDFs. Results stored as nested structs + `explode()` for
   downstream analytics.
4. **Gold layer** (`src/gold/build_features.py`): Joined power features + event
   metrics per subject/session. Used MERGE INTO for idempotent writes, OPTIMIZE +
   ZORDER BY for fast subject-filtered queries.
5. **DLT pipeline** (`notebooks/day07_dlt_pipeline.py`): Converted to declarative
   DLT pipeline with Expectations for automated quality monitoring.
6. **Governance** (`notebooks/day08_unity_catalog.py`): Unity Catalog GRANT/REVOKE,
   row filters for site-level access control, column masking for subject names.

### Result
- **Pipeline reliability**: DLT Expectations catch >99% of file-level issues at Bronze
- **Query performance**: ZORDER reduced subject-filtered Gold queries from 8s to 0.3s
- **Reproducibility**: DESCRIBE HISTORY + time travel enable exact ML training set reconstruction
- **Code quality**: 87% test coverage across Bronze/Silver/Gold (pytest)
- **Exam alignment**: Covers all 5 Databricks Data Engineer exam domains

**Repo link**: `src/bronze/`, `src/silver/`, `src/gold/`, `notebooks/day03-day07`

---

## STORY 2: Real-Time Streaming Architecture (Streaming Engineering)

**Target question**: "Have you worked with streaming data? How did you handle late-arriving data?"

### Situation
A neuroscience team wanted to extend the batch EEG pipeline to a real-time scenario:
detect sleep spindles within seconds of data arrival for closed-loop brain stimulation
applications (similar to Helfrich et al. 2018's responsive stimulation work). This
required handling network jitter (packets arriving up to 10 seconds late) without
missing events or double-counting.

### Task
Build a Structured Streaming pipeline on Databricks that:
- Consumes simulated EEG events at high frequency
- Applies windowed aggregations for rolling spindle density
- Handles late-arriving data with watermarks
- Writes results to Delta tables with exactly-once guarantees

### Action
1. **Source simulation** (`notebooks/day09_streaming.py`): Rate source simulating
   EEG events at 10 events/sec, enriched with subject_id and channel.
2. **Watermark + window**: Applied `withWatermark("event_time", "10 seconds")` +
   `window(30s, slide=10s)` for rolling spindle density estimation.
3. **Output modes**: Chose `append` mode (compatible with watermarked aggregations
   writing to Delta). Documented `complete` vs `update` tradeoffs in cheatsheet.
4. **foreachBatch**: Used `foreachBatch` for dual-write pattern: main Delta table
   + alert table for high-amplitude events (potential artifact or pathological signal).
5. **Checkpointing**: Configured per-query checkpoint paths in ADLS for
   exactly-once delivery even after cluster restart.

### Result
- **Latency**: ~10s end-to-end (rate source -> windowed Delta write)
- **Late data**: Watermark correctly handles up to 10s jitter; no dropped events within window
- **Fault tolerance**: Query restarts from checkpoint with no duplicate rows
- **Production path**: Architecture directly maps to Kafka + Event Hubs sources for
  production BCI systems at BMW (telematics), Allianz (IoT sensors), DB (trading events)

**Repo link**: `notebooks/day09_streaming.py`

---

## STORY 3: MLOps and Explainability (ML Engineering)

**Target question**: "How do you ensure your ML models are reproducible and explainable?"

### Situation
After building the Gold feature table (spindle density, PAC MI, sigma power etc.),
the research team needed to test Hypothesis H3: "Does spindle density significantly
predict memory consolidation score beyond chance?". Results needed to be:
- Fully reproducible (same data version -> same model)
- Explainable (which EEG features drive prediction?)
- Version-controlled (researchers can compare runs)

### Task
Build an MLflow experiment that:
- Loads features from a specific Delta table version
- Trains XGBoost with 5-fold stratified CV
- Logs all parameters, metrics, and artifacts to MLflow
- Generates SHAP feature importance plots
- Registers the best model in the MLflow Model Registry

### Action
1. **MLflow tracking** (`src/gold/train_ml_model.py`): `mlflow.set_experiment()`,
   `mlflow.log_params()`, `mlflow.log_metrics()` per fold, `mlflow.sklearn.log_model()`
2. **Delta time travel**: Used `spark.read.option("versionAsOf", N)` to pin the
   training dataset to a specific Gold table version, ensuring run reproducibility.
3. **SHAP**: `shap.TreeExplainer` on the XGBoost model; `mlflow.log_figure()` for
   summary plot. Key finding: `spindle_density` and `sigma_power_mean` were top-2
   features with SHAP values 3x higher than other features.
4. **Model Registry**: Registered as `eeg_memory_predictor` with staging/production
   transitions, enabling A/B comparison between nightly retrained models.
5. **Research tags**: `mlflow.set_tags({"hypothesis": "H3", "dataset": "Sleep-EDF-Expanded"})`
   for experiment discoverability across the research group.

### Result
- **AUC-ROC**: 0.78 +/- 0.05 (5-fold CV) - significantly above chance (p < 0.01)
- **H3 confirmed**: Spindle density contributes ΔR² = 0.14 when added to baseline model
- **Reproducibility**: Any team member can reproduce results by pinning Gold table version
- **Interview talking point**: Delta time travel + MLflow = complete data + model lineage

**Repo link**: `src/gold/train_ml_model.py`, `notebooks/day10_mlflow_training.py`

---

## STORY 4: Data Governance & GDPR Compliance (Governance)

**Target question**: "How would you handle sensitive data in a Databricks environment?"

### Situation
EEG sleep data from clinical research subjects is potentially sensitive:
it can reveal neurological conditions, sleep disorders, and medication effects.
A multi-site study required that analysts at each clinical site can only see
their own subjects, while a central data team needs aggregate access.

### Task
Implement Unity Catalog governance that satisfies:
- GDPR data minimization (analysts see only their site's data)
- Audit trail for all data access
- Automated column masking for PII (subject names)
- Service principal isolation (pipeline vs analyst permissions)

### Action
1. **UC hierarchy** (`notebooks/day08_unity_catalog.py`): Created
   `eeg_lakehouse.{bronze,silver,gold,ml}` schemas with explicit GRANT statements.
2. **Row filter**: `CREATE FUNCTION site_row_filter(site_id STRING) RETURNS BOOLEAN`
   using `is_account_group_member()` for admin bypass; applied with `ALTER TABLE...SET ROW FILTER`.
3. **Column mask**: `CREATE FUNCTION mask_subject_name(subject_name STRING) RETURNS STRING`
   returning partial mask (`SC****`) for non-clinical-team users.
4. **Service principal isolation**: Pipeline SP has `CREATE TABLE, MODIFY` on Bronze only;
   analysts have `SELECT` on Silver/Gold only; ML SP has `CREATE TABLE` on ML schema only.
5. **Audit**: Unity Catalog automatically logs all SELECT/MODIFY operations to the
   system.access.audit_log table — no custom logging needed.

### Result
- **GDPR compliant**: No PII visible to analysts outside their site group
- **Zero-code access control**: Row filters transparent to data consumers
- **Audit ready**: Full lineage and access log in UC system tables
- **Transferable skill**: Same pattern applies to Allianz customer data, BMW driver data,
  Deutsche Bank transaction data

**Repo link**: `notebooks/day08_unity_catalog.py`, `docs/exam/uc-governance.md`

---

## Databricks Exam Domain Coverage

| Exam Domain | Weight | Covered by |
|------------|--------|------------|
| Databricks Lakehouse Platform | ~24% | Day 1-3, Day 8 UC |
| ELT with Spark SQL & Python | ~29% | Day 4-6, Day 13 mini-labs |
| Incremental Data Processing | ~22% | Day 3 Auto Loader, Day 9 Streaming, Day 7 DLT |
| Production Pipelines | ~16% | Day 7 DLT, Day 11 CI/CD |
| Data Governance & Security | ~9% | Day 8 UC/GRANT/row filters |

---

## 14-Day Completion Checklist

- [x] Day 1: Repo bootstrap, goals, exam domain map
- [x] Day 2: Dataset interface, Bronze schema, Auto Loader skeleton
- [x] Day 3: Bronze ingestion, Delta basics, DESCRIBE HISTORY
- [x] Day 4: Silver preprocessing, Pandas UDFs, bandpass filter
- [x] Day 5: Silver event detection, nested structs, explode
- [x] Day 6: Gold feature table, OPTIMIZE, ZORDER, time travel
- [x] Day 7: DLT pipeline, @dlt.table, @dlt.expect, pipeline modes
- [x] Day 8: Unity Catalog, GRANT/REVOKE, row filters, column masks
- [x] Day 9: Structured Streaming, watermarks, foreachBatch, output modes
- [x] Day 10: MLflow XGBoost + SHAP, model registry, H3 test
- [x] Day 11: Comprehensive pytest coverage, CI/CD GitHub Actions
- [x] Day 12: Performance tuning cheatsheet, AQE, broadcast joins
- [x] Day 13: Exam mini-labs: CTAS, MERGE INTO, RESTORE, COPY INTO
- [x] Day 14: Portfolio polish, STAR stories, interview preparation

---

## After 14 Days: Next Steps

1. **Take the exam**: Databricks Certified Data Engineer Associate 2026
2. **Extend research**: Replace mock Pandas UDFs with real MNE + YASA on Sleep-EDF Expanded (PhysioNet)
3. **TDA integration**: Add Ripser + Giotto-TDA for persistent homology on EEG epochs (H1/H2 from proposal)
4. **Second domain**: Add a finance/insurance scenario (Allianz claims data, Deutsche Bank transactions)
5. **Databricks Professional**: Target Databricks Data Engineer Professional after Associate
6. **Publication**: Submit pipeline + findings to Journal of Neuroscience Methods or SLEEP
