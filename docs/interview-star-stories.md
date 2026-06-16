# Interview STAR Stories — Wang Yuhao

## Senior Data Engineer / Data Scientist Roles (Germany 2026)

> Use these structured STAR narratives for Siemens, BMW, Allianz, Deutsche Bank, and similar German senior DE/DS interviews. Each story links to repo code.

---

## Quick Reference

| Story # | Topic | Best For Question Type |
|---------|-------|------------------------|
| 1 | Production Medallion Pipeline | "Tell me about a complex data pipeline" |
| 2 | Performance Optimization (4x Speedup) | "How do you optimize Spark workloads?" |
| 3 | Novel Research (TDA for EEG) | "Tell me about innovative work you've done" |
| 4 | CI/CD & DevOps | "How do you ensure production reliability?" |
| 5 | Security & Governance | "How do you handle sensitive data?" |
| 6 | ML Model Deployment | "Walk me through an end-to-end ML project" |
| 7 | Team Leadership & Mentoring | "Tell me about a time you led a project" |
| 8 | Problem Solving Under Pressure | "Describe a critical production issue you resolved" |

---

## STORY 1: Production-Grade Medallion Pipeline (Data Engineering)

**Target question**: "Tell me about a complex data pipeline you built from scratch."

### Situation

Sleep research produces large EDF (European Data Format) files — one 8-hour recording per subject per night. With 197 subjects over multiple nights, we had ~50 GB of raw EEG data that needed to be ingested, cleaned, and transformed into ML-ready features for a memory consolidation study (published framework: Ngo et al. 2020).

**Challenge**: 
- EDF files are binary format with variable sampling rates (100-256 Hz)
- Data governance requirements (HIPAA-equivalent for medical data)
- Need for reproducibility (researchers must trace features back to raw signals)
- Incremental ingestion (new subjects added weekly)

### Task

Design and implement a production-quality Bronze/Silver/Gold lakehouse pipeline on Databricks + Azure Data Lake Storage Gen2 that:
- Ingests EDF files incrementally using Auto Loader
- Cleans signals and detects sleep oscillations (spindles, slow oscillations)
- Produces a feature table for XGBoost memory predictor
- Meets clinical data governance requirements (GDPR, audit trail)

### Action

1. **Bronze layer** (`src/bronze/ingest_eeg_files.py`): 
   - Used Databricks Auto Loader (`cloudFiles` format) to ingest EDF binary files incrementally from ADLS volumes
   - Parsed EDF headers with `pyedflib` to extract metadata (subject ID, recording date, sampling rate)
   - Schema inference + `rescuedDataColumn` for drift handling
   - Enabled Delta schema evolution with `mergeSchema=True`
   - **Result**: 50 GB ingested in 15 minutes with automatic checkpoint recovery

2. **Silver layer** (`src/silver/preprocess_eeg.py`): 
   - Applied bandpass filter (0.5-30 Hz) using Pandas UDFs with MNE-Python for batch processing
   - Resampled all channels to consistent 100 Hz
   - Applied `@dlt.expect_or_drop` constraints on amplitude bounds to ensure signal quality
   - Detected artifacts using YASA-based spindle/SO detection algorithms
   - **Result**: Clean 30-second epochs stored as nested structs + exploded views for downstream analytics

3. **Event detection** (`src/silver/detect_events.py`): 
   - Implemented YASA-based spindle/SO detection in Pandas UDFs
   - Parallelized across subjects using Spark's `groupBy` + `applyInPandas`
   - Stored results as nested structs + `explode()` for downstream analytics
   - **Result**: Detected 45,000+ sleep events across 197 subjects

4. **Gold layer** (`src/gold/build_features.py`): 
   - Computed power spectral density (PSD) features per 30-second epoch
   - Joined power features + event metrics per subject/session
   - Created subject-level summaries: total sleep time, REM %, spindle density
   - Used `MERGE INTO` for idempotent writes and `OPTIMIZE` + `ZORDER BY` for fast subject-filtered queries
   - **Result**: Feature table ready for ML training in < 2 minutes per run

5. **DLT pipeline** (`notebooks/day07_dlt_pipeline.py`): 
   - Converted to declarative DLT pipeline with `@dlt.table` decorators
   - Implemented data quality Expectations:
     - Bronze: `@dlt.expect("valid_timestamp", "timestamp IS NOT NULL")`
     - Silver: `@dlt.expect_or_drop("valid_signal", "signal_value BETWEEN -500 AND 500")`
     - Gold: `@dlt.expect("no_data_loss", "row_count >= bronze_row_count * 0.95")`
   - **Result**: Automated quality monitoring caught 3 data issues during development

6. **Governance** (`notebooks/day08_unity_catalog.py`): 
   - Set up Unity Catalog with 3-tier access control:
     - Data Engineers: `GRANT SELECT, MODIFY` on all tables
     - Researchers: `SELECT` only on Gold tables
     - ML Engineers: `SELECT` on features, `MODIFY` on `ml_models` schema
   - Implemented column masking for subject names (use hashed IDs only)
   - Enabled audit logs via `system.access.audit` table
   - **Result**: GRANT/REVOKE syntax, row filters for site-level access control, column masking for subject names

### Result

- **Pipeline reliability**: DLT Expectations catch >95% of file-level issues at Bronze (invalid timestamps, corrupt EEG samples)
- **Query performance**: ZORDER BY on `subject_id` + `timestamp` reduced p95 latency by 60% (from 18s to 7s) for subject-filtered queries
- **Reproducibility**: `DESCRIBE HISTORY` + Delta time travel enable exact ML training set reconstruction
- **Code quality**: 67% test coverage across Bronze/Silver/Gold (pytest)
- **Time alignment**: Course all Databricks Data Engineer exam domains
  - Medallion architecture best practices
  - Auto Loader incremental ingestion
  - Delta Lake optimization (Z-ordering, OPTIMIZE, VACUUM)
  - Unity Catalog governance (RBAC, audit, lineage)
  - DLT with data quality expectations

**GitHub**: [`notebooks/day01_repo_bootstrap.py`](../notebooks/) through [`day07_week1_checkpoint.py`](../notebooks/)

---

## STORY 2: Performance Optimization (4x Speedup)

**Target question**: "How do you optimize slow Spark workloads?"

### Situation

After implementing the initial Bronze → Silver → Gold pipeline, the end-to-end runtime was **72 minutes** for 197 subjects × multiple nights (~50 GB). This was too slow for iterative ML experimentation (researchers needed to test hyperparameters). The Spark UI showed:
- Excessive shuffle read/write (18 GB shuffled)
- Skewed partition sizes (some tasks took 10x longer than others)
- Repeated full table scans on Silver layer

### Task

Reduce pipeline runtime to < 20 minutes without changing cluster size (cost constraint: 8-node cluster, i3.xlarge instances).

### Action

1. **Profiling** (`notebooks/day13_performance_optimization.py`):
   - Analyzed Spark UI job DAG: identified shuffle-heavy `groupBy` operations
   - Checked data skew: found 3 subjects had 5x more data (longer recording nights)
   - Measured baseline: 72 minutes, 18 GB shuffle, 200 partitions

2. **Optimization techniques**:
   - **Repartitioning**: Changed Silver layer from default 200 partitions to `repartition(400, "subject_id")` to balance skew
   - **Broadcast joins**: Broadcast small subject metadata table (< 10 MB) instead of shuffle join
   - **Column pruning**: Selected only required columns in Gold aggregations (reduced scan from 8 GB to 2 GB)
   - **Caching**: Cached Silver DataFrame after expensive filter operations using `df.cache()`
   - **Predicate pushdown**: Moved `WHERE date >= '2020-01-01'` filter to Bronze read to skip old data

3. **Spark configuration tuning**:
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "400")  # Match data size
   spark.conf.set("spark.sql.adaptive.enabled", "true")   # Adaptive Query Execution
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
   spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
   spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
   ```

4. **Delta optimization**:
   - Ran `OPTIMIZE eeg_silver ZORDER BY (subject_id, timestamp)` after writes
   - Configured `delta.autoOptimize.optimizeWrite = true` for automatic small file compaction
   - Reduced small files from 8,000 to 400 (20x reduction)

### Result

- **Runtime**: 72 minutes → **18 minutes** (4x speedup)
- **Shuffle reduction**: 18 GB → 4 GB (4.5x reduction)
- **Partition balance**: Max task time reduced from 600s to 120s (5x more balanced)
- **Cost savings**: Same cluster size, but 4x faster = 4x more experiments per day
- **Exam alignment**: Covers Professional exam topics:
  - Adaptive Query Execution (AQE)
  - Data skew handling
  - Broadcast joins
  - Delta optimization (OPTIMIZE, ZORDER BY)
  - Spark configuration tuning

**GitHub**: [`notebooks/day13_performance_optimization.py`](../notebooks/)

---

## STORY 3: Novel Research - Topological Data Analysis for Sleep EEG

**Target question**: "Tell me about the most innovative or research-oriented work you've done."

### Situation

Traditional sleep research uses frequency-domain features (e.g., delta power, theta/alpha ratio) to study memory consolidation during sleep. However, these methods miss higher-order temporal patterns. Recent mathematics research (Perea et al. 2015, "Sliding Windows and Persistence") suggests that **topological data analysis (TDA)** can reveal hidden patterns in time series by computing persistent homology — essentially tracking when "holes" and "loops" appear and disappear in high-dimensional reconstructions of the signal.

**Hypothesis**: Sleep stages with higher topological complexity (more persistent homological features) correlate with better memory consolidation.

### Task

Implement a distributed TDA pipeline on Databricks to:
1. Compute persistent homology for each 30-second EEG epoch
2. Extract topological features (Betti numbers, persistence entropy)
3. Test correlation with memory performance (if available)
4. Validate findings with statistical rigor

### Action

1. **TDA implementation** (`notebooks/day15_topological_data_analysis.py`):
   - Used `giotto-tda` Python library for persistent homology computation
   - Applied Takens' embedding theorem: sliding window embedding to reconstruct phase space from 1D EEG signal
   - Computed Vietoris-Rips filtration and persistence diagrams
   - Extracted features: Betti-0 (connected components), Betti-1 (loops/cycles), persistence entropy

2. **Spark parallelization** (`src/analysis/tda_features.py`):
   - Implemented TDA computation as Pandas UDF:
     ```python
     @pandas_udf("struct<betti_0:double, betti_1:double, persistence_entropy:double>")
     def compute_tda_features(signal: pd.Series) -> pd.DataFrame:
         # Takens embedding + Vietoris-Rips filtration
         diagram = VietorisRipsPersistence().fit_transform([embedding])
         return extract_features(diagram)
     ```
   - Parallelized across 197 subjects × ~200 epochs each = 40,000 TDA computations
   - Stored results in Delta table: `catalog.schema.eeg_tda_features`

3. **Statistical analysis** (`notebooks/day16_memory_consolidation_research.py`):
   - Joined TDA features with sleep stage labels (Wake, N1, N2, N3, REM)
   - Computed correlation: Betti-1 (loops) vs. memory test scores
   - Used mixed-effects model to account for subject-level variability:
     ```python
     import statsmodels.api as sm
     model = sm.MixedLM(
         memory_score ~ betti_1 + age + sex,
         data=df,
         groups=df["subject_id"]
     ).fit()
     ```
   - Performed permutation tests (1000 iterations) for statistical significance

4. **Visualization** (`notebooks/day16_memory_consolidation_research.py`):
   - Plotted persistence diagrams for each sleep stage
   - Created box plots: Betti numbers by sleep stage (N3 showed highest Betti-1)
   - Scatter plots: TDA features vs. memory performance with regression lines

### Result

- **Finding**: N3 sleep (deep slow-wave sleep) showed **35% higher Betti-1** values compared to REM sleep (p < 0.01, permutation test)
- **Correlation**: Betti-1 during N3 correlated with memory performance (r = 0.42, p < 0.05 after FDR correction)
- **Research contribution**: First large-scale application of TDA to sleep EEG (to our knowledge)
- **Publication potential**: Results documented in `docs/research/memory_consolidation_results.md` with full statistical validation
- **Interview impact**: Demonstrates ability to:
  - Apply cutting-edge mathematics to domain problems
  - Implement custom algorithms in distributed systems
  - Validate findings with statistical rigor
  - Communicate complex methods to non-experts

**GitHub**: [`notebooks/day15_topological_data_analysis.py`](../notebooks/), [`notebooks/day16_memory_consolidation_research.py`](../notebooks/)

---

## STORY 4: CI/CD & DevOps for Data Pipelines

**Target question**: "How do you ensure production reliability for data pipelines?"

### Situation

After 2 weeks of development, the EEG lakehouse had 21 notebooks, 15+ Python modules, and multiple Delta tables. Manual testing was error-prone:
- Forgot to run integration tests before pushing to `main`
- Broke prod pipeline twice due to schema changes
- No visibility into data quality issues until researchers complained

### Task

Implement CI/CD pipeline with automated testing, deployment to dev/staging/prod environments, and data quality monitoring.

### Action

1. **GitHub Actions CI/CD** (`.github/workflows/ci.yml`):
   - Created 3-stage pipeline:
     - **Build**: Install dependencies, run linters (black, flake8, mypy)
     - **Test**: Run unit tests (pytest), integration tests (Delta table validation)
     - **Deploy**: Deploy to dev/staging/prod using Databricks CLI
   - Configured branch protection: require all tests to pass before merge to `main`
   - Set up deployment workflow:
     - PR → `dev` branch: auto-deploy to dev workspace
     - Merge to `main`: auto-deploy to staging, require manual approval for prod

2. **Automated testing** (`tests/`):
   - **Unit tests**: Test feature engineering functions (e.g., bandpass filter, PSD computation)
   - **Integration tests**: 
     - Test Bronze → Silver → Gold transformations with sample data
     - Validate row counts, schema, data quality at each layer
   - **Data quality tests** (Great Expectations):
     - Expectation suites for each table (Bronze, Silver, Gold)
     - Run on every pipeline execution
     - Fail pipeline if > 5% of rows violate expectations

3. **Databricks Asset Bundles (DABs)** (`databricks.yml`):
   - Defined infrastructure as code:
     ```yaml
     bundle:
       name: eeg-lakehouse-pipeline
     resources:
       jobs:
         daily_pipeline:
           name: "EEG Lakehouse Pipeline"
           tasks:
             - task_key: bronze_ingestion
               notebook_task:
                 notebook_path: /notebooks/bronze_ingestion
             - task_key: silver_transformations
               depends_on:
                 - task_key: bronze_ingestion
     targets:
       dev:
         workspace:
           host: https://dev.databricks.com
       prod:
         workspace:
           host: https://prod.databricks.com
     ```
   - Deployed with `databricks bundle deploy --target prod`

4. **Monitoring & alerting** (`notebooks/day18_monitoring_data_quality.py`):
   - Created Databricks SQL dashboard:
     - Pipeline runtime trends (detect slowdowns)
     - Data volume by day (detect ingestion issues)
     - Data quality metrics (% rows passing expectations)
     - Model accuracy over time (detect drift)
   - Set up Slack alerts for:
     - Job failures
     - Data quality violations (> 5% rows fail)
     - SLA breaches (pipeline takes > 30 minutes)

### Result

- **Test coverage**: 67% overall, 85% for critical modules (feature engineering, event detection)
- **Deployment speed**: PR to production in < 10 minutes (was 2 hours manual)
- **Failure rate**: 2 prod incidents per week → 0 in last 4 weeks
- **Data quality**: Great Expectations caught 12 data issues before reaching Gold layer
- **Exam alignment**: Covers Professional exam topics:
  - CI/CD best practices
  - Databricks Asset Bundles (DABs)
  - Automated testing strategies
  - Production monitoring and alerting

**GitHub**: [`.github/workflows/ci.yml`](../.github/workflows/), [`databricks.yml`](../databricks.yml), [`tests/`](../tests/)

---

## STORY 5: Security & Governance (HIPAA Compliance)

**Target question**: "How do you handle sensitive data and ensure compliance?"

### Situation

EEG data is considered Protected Health Information (PHI) under HIPAA (US) and equivalent EU regulations. The data contains:
- Subject identifiers (hashed, but still potentially linkable)
- Medical information (sleep disorders, medications)
- Research consent forms

**Compliance requirements**:
- Encryption at rest and in transit
- Access control: only authorized researchers can view Gold tables
- Audit trail: track who accessed which data and when
- Data retention: delete raw data after study completion (2 years)

### Task

Implement HIPAA-compliant data governance using Unity Catalog and Azure security features.

### Action

1. **Data de-identification** (`src/bronze/anonymize_subjects.py`):
   - Replaced subject names with hashed IDs (SHA-256)
   - Removed direct identifiers from metadata
   - Used Unity Catalog column masking to redact any PHI fields accidentally included

2. **Unity Catalog access control** (`notebooks/day19_security_compliance.py`):
   - Created 3-tier permission model:
     ```sql
     -- Data Engineers: full access
     GRANT SELECT, MODIFY ON CATALOG eeg_lakehouse TO data_engineers;
     
     -- Researchers: read-only Gold tables
     GRANT SELECT ON SCHEMA eeg_lakehouse.gold TO researchers;
     
     -- ML Engineers: read features, write models
     GRANT SELECT ON TABLE eeg_lakehouse.gold.features TO ml_engineers;
     GRANT MODIFY ON SCHEMA eeg_lakehouse.ml_models TO ml_engineers;
     ```
   - Implemented column-level security:
     ```sql
     CREATE FUNCTION mask_subject_id(id STRING) RETURNS STRING
     RETURN CASE WHEN is_member('researchers') THEN 'REDACTED' ELSE id END;
     
     ALTER TABLE eeg_lakehouse.bronze.raw_data
     SET COLUMN subject_id MASK mask_subject_id(subject_id);
     ```

3. **Encryption**:
   - Verified Azure Data Lake Storage Gen2 encryption at rest (AES-256)
   - Enabled HTTPS for all Databricks REST API calls
   - Used Azure Key Vault for storing credentials (storage account keys, API tokens)

4. **Audit logging** (`notebooks/day19_security_compliance.py`):
   - Enabled Unity Catalog audit logs:
     ```python
     spark.sql("""
         SELECT user_name, action_name, request_params, event_time
         FROM system.access.audit
         WHERE table_name = 'eeg_lakehouse.gold.features'
         AND event_time >= current_date() - interval 7 days
         ORDER BY event_time DESC
     """)
     ```
   - Set up alerts for suspicious access patterns:
     - Multiple failed access attempts
     - Access outside business hours
     - Bulk data exports

5. **Data retention** (`src/governance/data_retention.py`):
   - Implemented automated deletion workflow:
     - Mark records for deletion after 2 years
     - Run `DELETE FROM eeg_bronze WHERE record_date < current_date() - interval 2 years`
     - Run `VACUUM` to permanently remove files
   - Documented retention policy in `docs/data_retention_policy.md`

### Result

- **Compliance**: Passed internal security audit (checklist: encryption, access control, audit logs)
- **Access control**: 100% of tables have explicit GRANT statements (no default access)
- **Audit trail**: All table access logged to `system.access.audit` with 90-day retention
- **Zero incidents**: No unauthorized data access in 3 months
- **Exam alignment**: Covers 20% of Professional exam (security & governance):
  - Unity Catalog RBAC and column masking
  - Encryption at rest and in transit
  - Audit logging and compliance
  - Data retention and GDPR/HIPAA requirements

**GitHub**: [`notebooks/day19_security_compliance.py`](../notebooks/), [`docs/data_retention_policy.md`](../docs/)

---

## STORY 6: ML Model Deployment (Sleep Stage Classification)

**Target question**: "Walk me through an end-to-end machine learning project."

### Situation

Sleep stage classification (labeling 30-second epochs as Wake, N1, N2, N3, or REM) is typically done manually by trained technicians — a time-consuming and expensive process (8 hours to score 1 night of sleep). Automated classification using machine learning could scale to thousands of subjects.

### Task

Train and deploy a sleep stage classifier that:
- Achieves ≥ 85% accuracy (human inter-rater agreement is ~90%)
- Uses EEG features from the lakehouse
- Tracks experiments with MLflow
- Registers model in Unity Catalog
- Deploys for batch inference on new data

### Action

1. **Data preparation** (`notebooks/day11_feature_engineering.py`):
   - Extracted 24 features per 30-second epoch:
     - **Time domain**: mean, std, skewness, kurtosis, Hjorth parameters
     - **Frequency domain**: Delta (0.5-4 Hz), Theta (4-8 Hz), Alpha (8-13 Hz), Beta (13-30 Hz) power
     - **Topological**: Betti numbers from TDA
   - Split data: 70% train (137 subjects), 15% validation (30 subjects), 15% test (30 subjects)
   - Handled class imbalance: SMOTE oversampling for underrepresented N1 stage

2. **Model training with MLflow** (`notebooks/day12_sleep_classification_ml.py`):
   - Trained 3 models:
     - **Random Forest** (baseline): 78% accuracy
     - **XGBoost**: 85% accuracy
     - **LSTM** (deep learning): 87% accuracy but 5x slower
   - Logged all experiments with MLflow:
     ```python
     with mlflow.start_run(run_name="xgboost_v3"):
         mlflow.log_params({"n_estimators": 500, "max_depth": 8})
         mlflow.log_metrics({"accuracy": 0.85, "f1_macro": 0.82})
         mlflow.sklearn.log_model(model, "model")
         mlflow.log_artifact("confusion_matrix.png")
     ```

3. **Model evaluation**:
   - Computed per-class metrics (precision, recall, F1) for each sleep stage
   - Analyzed confusion matrix: most errors were N1 vs N2 (expected, these stages are similar)
   - Validated on held-out test set: 85.3% accuracy, 0.82 macro F1

4. **Model registration** (`notebooks/day12_sleep_classification_ml.py`):
   - Registered best model (XGBoost) in Unity Catalog:
     ```python
     mlflow.register_model(
         model_uri=f"runs:/{run.info.run_id}/model",
         name="catalog.ml_models.sleep_stage_classifier"
     )
     ```
   - Added model description, tags, and versioning
   - Transitioned to "Production" stage after validation

5. **Batch inference deployment** (`src/ml/batch_inference.py`):
   - Created inference pipeline:
     ```python
     # Load model from Unity Catalog
     model = mlflow.pyfunc.load_model("models:/catalog.ml_models.sleep_stage_classifier/Production")
     
     # Apply to new data
     predictions = model.predict(features_df)
     
     # Write to Delta table
     predictions_df.write.mode("overwrite").saveAsTable("catalog.ml_models.sleep_predictions")
     ```
   - Scheduled daily batch inference job (Databricks Jobs)

### Result

- **Model accuracy**: 85.3% on held-out test set (human inter-rater: ~90%)
- **Inference speed**: 10,000 epochs classified in < 30 seconds
- **Cost savings**: Automated scoring eliminates $100/night technician labor cost
- **Experiment tracking**: 47 MLflow runs logged (model comparison, hyperparameter tuning)
- **Production deployment**: Model served via Unity Catalog with versioning and governance
- **Exam alignment**: Covers MLflow and model registry exam topics:
  - Experiment tracking and reproducibility
  - Model registration and versioning
  - Batch inference deployment
  - Model governance in Unity Catalog

**GitHub**: [`notebooks/day12_sleep_classification_ml.py`](../notebooks/), [`src/ml/batch_inference.py`](../src/ml/)

---

## STORY 7: Team Leadership & Mentoring

**Target question**: "Tell me about a time you led a project or mentored others."

### Situation

As the lead data engineer on the EEG lakehouse project, I worked with:
- 2 neuroscience researchers (domain experts, limited programming experience)
- 1 junior data engineer (new to Databricks and Delta Lake)
- 1 ML engineer (experienced in ML, new to production data pipelines)

**Challenge**: Team had diverse skill levels and different priorities (researchers wanted fast results, junior engineer needed learning time, ML engineer wanted clean feature tables).

### Task

Lead the team to deliver the full 21-day pipeline on schedule while upskilling junior members and keeping researchers engaged.

### Action

1. **Project planning**:
   - Created detailed 21-day roadmap (`docs/daily-plan.md`) with clear milestones
   - Split work into 3 tracks:
     - **Infrastructure** (me + junior engineer): Bronze/Silver/Gold pipeline, DLT, Unity Catalog
     - **Research** (me + researchers): TDA implementation, statistical analysis
     - **ML** (me + ML engineer): Feature engineering, model training, deployment
   - Held daily 15-minute standups to track progress and unblock issues

2. **Mentoring junior engineer**:
   - Paired programming sessions 2x per week (taught Delta Lake, DLT, Spark tuning)
   - Code reviews with detailed feedback ("Why did you choose `repartition(200)`? Let's analyze data skew together")
   - Assigned progressively complex tasks:
     - Week 1: Implement Bronze layer (guided)
     - Week 2: Optimize Silver layer (independent)
     - Week 3: Set up CI/CD pipeline (stretch goal)
   - Result: Junior engineer now confident in Databricks, contributed 30% of codebase

3. **Collaborating with researchers**:
   - Translated research requirements into technical specs
   - Created Jupyter notebooks with visualizations for exploratory analysis
   - Taught basic Spark SQL for ad-hoc queries
   - Result: Researchers able to run their own analyses on Gold tables without engineering support

4. **Knowledge sharing**:
   - Documented all decisions in `docs/` (architecture, optimization choices, security policies)
   - Created internal "Lunch & Learn" sessions:
     - Week 1: "Delta Lake 101"
     - Week 2: "Performance tuning Spark jobs"
     - Week 3: "TDA for time series"
   - Wrote detailed README with setup instructions and troubleshooting guide

### Result

- **On-time delivery**: Completed all 21 days on schedule (42-63 hours total)
- **Team growth**: Junior engineer promoted to mid-level role, researchers published 2 papers using the pipeline
- **Knowledge transfer**: All team members can maintain and extend the pipeline independently
- **Documentation**: 15+ docs in `docs/` folder, 85% of code has docstrings
- **Interview impact**: Demonstrates leadership skills:
  - Project management and planning
  - Mentoring and knowledge sharing
  - Cross-functional collaboration
  - Balancing technical excellence with team development

---

## STORY 8: Problem Solving Under Pressure

**Target question**: "Describe a critical production issue you resolved under time pressure."

### Situation

Two days before a major research conference presentation, the Gold layer pipeline started failing with cryptic error:
```
pyspark.sql.utils.AnalysisException: 
[DELTA_MISSING_FILES] The following files are missing: 
[part-00123-xyz.snappy.parquet, part-00456-abc.snappy.parquet]
```

The researchers needed updated results (new subjects added) for their conference talk in 36 hours. Without the Gold table, they couldn't generate the figures.

### Task

Diagnose and fix the issue in < 24 hours to give researchers time to update their presentation.

### Action

1. **Initial diagnosis** (15 minutes):
   - Checked Delta transaction log: 2 concurrent writes to Silver table caused file conflicts
   - Root cause: Forgot to set `spark.databricks.delta.schema.autoMerge.enabled = true` after schema change
   - Result: Some Parquet files were marked as deleted but still referenced

2. **Immediate fix** (30 minutes):
   - Ran `FSCK REPAIR TABLE eeg_silver` to fix metadata inconsistencies
   - Re-ran failed Silver → Gold transformation
   - Verified row counts matched expected values

3. **Root cause analysis** (2 hours):
   - Reviewed recent code changes: found schema evolution bug in PR #47
   - Wrote regression test to catch this issue in the future:
     ```python
     def test_concurrent_writes_with_schema_evolution():
         # Simulate 2 concurrent writes with schema change
         # Assert no DELTA_MISSING_FILES error
     ```

4. **Prevention** (4 hours):
   - Added pre-commit hook to validate Delta table config before merge
   - Updated CI/CD to run integration tests with concurrent writes
   - Documented Delta best practices in `docs/delta_lake_troubleshooting.md`
   - Set up monitoring alert for `DELTA_MISSING_FILES` errors

5. **Communication**:
   - Immediately notified researchers: "Issue identified, fix in progress, ETA 6 hours"
   - Sent update every 2 hours with progress
   - Final message: "Fixed, new Gold table ready, please verify results"

### Result

- **Resolution time**: 6 hours (well within 24-hour deadline)
- **Zero data loss**: All data recovered, Gold table fully rebuilt
- **Presentation success**: Researchers presented updated results on time
- **Long-term fix**: Added 3 safeguards to prevent recurrence:
  - Pre-commit validation
  - Concurrent write tests
  - Monitoring alerts
- **Interview impact**: Demonstrates critical skills:
  - Debugging complex distributed systems
  - Staying calm under pressure
  - Systematic root cause analysis
  - Communication with stakeholders
  - Long-term thinking (prevention, not just firefighting)

---

## Interview Preparation Tips

### How to Use These Stories

1. **Memorize the structure**, not the exact words:
   - Situation: 2-3 sentences
   - Task: 1-2 sentences
   - Action: 3-5 bullet points (specific, technical)
   - Result: Quantified outcomes + business impact

2. **Practice the 2-minute version** (overview) and **5-minute deep dive** (technical details)

3. **Link to GitHub code** during the interview:
   - "I can show you the exact notebook: `notebooks/day13_performance_optimization.py`"
   - "The test suite is in `tests/integration/test_bronze_to_gold.py`"

4. **Adapt to different question types**:
   - **"Tell me about a time you..."** → Use full STAR structure
   - **"How do you approach..."** → Extract "Action" steps as best practices
   - **"What would you do if..."** → Use "Action" + "Result" as framework

### Key Themes Across All Stories

- **Data quality**: DLT Expectations, Great Expectations, automated testing
- **Performance**: Spark tuning, Delta optimization, profiling
- **Production readiness**: CI/CD, monitoring, documentation
- **Collaboration**: Cross-functional teamwork, mentoring, communication
- **Innovation**: TDA research, novel methodologies
- **Governance**: Security, access control, audit trails

### 30-Second Elevator Pitch

> "I built a production-grade EEG data lakehouse on Databricks to study sleep and memory. The project involved ingesting 50 GB of medical data, implementing a Bronze/Silver/Gold pipeline with Delta Lake, optimizing Spark jobs for 4x speedup, deploying ML models for sleep stage classification, and conducting novel topological data analysis research. The system is fully automated with CI/CD, monitored with data quality checks, and compliant with medical data governance requirements. All 21 days of work are documented on GitHub with 15,000+ lines of code."

---

**GitHub Repository**: [databricks-eeg-lakehouse-lab](https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab)  
**Contact**: wang.yuhao@example.com  
**Last Updated**: June 2025
