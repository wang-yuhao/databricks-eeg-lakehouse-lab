# Databricks Certified Data Engineer Professional — Exam Domains Overview (2026)

> 📅 **June 2026 exam blueprint** | 59 questions | 120 minutes | 70% passing | $200 USD  
> ⚠️ **IMPORTANT:** This repo was originally built for the Associate exam. This file maps how existing content aligns with the Professional exam and identifies gaps.

---

## Exam Structure

| Domain | Weight | Questions (~) |
|--------|--------|---------------|
| 1. Developing Code for Data Processing (Python & SQL) | 22% | ~13 |
| 2. Data Ingestion & Acquisition | 7% | ~4 |
| 3. Data Transformation, Cleansing, and Quality | 10% | ~6 |
| 4. Data Sharing and Federation | 5% | ~3 |
| 5. Monitoring and Alerting | 10% | ~6 |
| 6. Cost & Performance Optimisation | 13% | ~8 |
| 7. Ensuring Data Security and Compliance | 10% | ~6 |
| 8. Data Governance | 7% | ~4 |
| 9. Debugging and Deploying | 10% | ~6 |
| 10. Data Modelling | 6% | ~4 |

**Total: 60 questions** (approximately)

---

## Domain 1: Developing Code for Data Processing (22% — HIGHEST WEIGHT)

### Core Topics
- **Advanced PySpark & SQL:** Complex joins, window functions, higher-order functions (`transform`, `filter`, `aggregate`), nested data (explode, flatten)
- **Pandas UDFs & Arrow Optimization:** Performance comparison Python UDF vs Pandas UDF vs `applyInPandas`
- **Broadcast Joins & Skew Handling:** Managing data skew with salting, broadcast hints
- **Complex Transformations:** Pivots, unpivots, array/map manipulation, struct operations
- **SQL Advanced:** CTEs, MERGE INTO with complex conditions, SCD Type 2 implementations
- **DataFrame API vs SQL:** When to use each; converting between them

### Repo Coverage
✅ **STRONG:**  
- `src/silver/preprocess_eeg.py` — Pandas UDFs for signal processing  
- `src/silver/detect_events.py` — Nested struct handling, array operations  
- `src/gold/build_features.py` — Window functions, aggregations  
- `notebooks/day04_silver_preprocessing.py` — UDF performance comparisons  
- Study notes Days 4-5 cover UDFs and nested data

⚠️ **GAPS:**  
- ❌ No broadcast join examples with real skew handling  
- ❌ No advanced SQL CTEs or complex MERGE scenarios  
- ❌ Limited higher-order function examples (only basic `transform`)  
- ❌ No pivot/unpivot examples

### Recommended Additions
1. **New notebook:** `notebooks/day15_advanced_spark_sql.py`  
   - Complex CTEs with window functions  
   - Broadcast joins with skew handling (salting technique)  
   - Pivot/unpivot for EEG channel analysis  
   - SCD Type 2 implementation for subject metadata

2. **New src module:** `src/utils/spark_optimization.py`  
   - Skew detection utilities  
   - Broadcast join helpers  
   - Higher-order function examples

---

## Domain 2: Data Ingestion & Acquisition (7%)

### Core Topics
- **Auto Loader:** Schema inference, schema evolution, rescue columns, checkpointing  
- **COPY INTO vs Auto Loader:** When to use each  
- **Ingestion Patterns:** Batch vs streaming, incremental loads, file notification vs directory listing  
- **Data Source Connectors:** JDBC, REST APIs, cloud storage (ADLS, S3)  
- **File Formats:** Parquet, Delta, JSON, CSV performance characteristics

### Repo Coverage
✅ **STRONG:**  
- `src/bronze/ingest_eeg_files.py` — Auto Loader with cloudFiles  
- Study note Day 2 covers Auto Loader in depth  
- `notebooks/day03_bronze_ingestion.py` — COPY INTO vs Auto Loader comparison

⚠️ **GAPS:**  
- ❌ No schema evolution examples (schema hints, `mergeSchema`)  
- ❌ No rescue column handling  
- ❌ No REST API or JDBC ingestion examples  
- ❌ Limited file format comparison (performance benchmarks)

### Recommended Additions
1. **Update:** `src/bronze/ingest_eeg_files.py`  
   - Add schema evolution handling  
   - Add rescue column processing  
   - Add file notification vs directory listing comparison

2. **New study note:** `docs/study-notes/day15-data-source-connectors.md`  
   - JDBC ingestion from PostgreSQL  
   - REST API ingestion with pagination  
   - File format performance benchmarks

---

## Domain 3: Data Transformation, Cleansing, and Quality (10%)

### Core Topics
- **Data Quality:** DLT expectations, constraint violations, quarantine patterns  
- **Deduplication:** Window functions, `dropDuplicates`, identity columns  
- **Data Cleansing:** Null handling, outlier detection, data type coercion  
- **Validation:** Schema validation, business rule enforcement  
- **Delta Lake:** MERGE for upserts, UPDATE/DELETE operations

### Repo Coverage
✅ **STRONG:**  
- `src/dlt/eeg_pipeline.py` — DLT with `@dlt.expect_or_drop`  
- Study note Day 7 covers DLT expectations comprehensively  
- `notebooks/day03_bronze_ingestion.py` — MERGE INTO examples

⚠️ **GAPS:**  
- ❌ No quarantine table pattern  
- ❌ No advanced deduplication scenarios (composite keys, time-based)  
- ❌ No constraint violation handling and recovery  
- ❌ Limited outlier detection examples

### Recommended Additions
1. **New module:** `src/silver/data_quality.py`  
   - Quarantine table implementation  
   - Advanced deduplication with window functions  
   - Outlier detection for EEG amplitude ranges  
   - Constraint violation logging

2. **Expand:** Study note Day 7 with quarantine patterns

---

## Domain 4: Data Sharing and Federation (5%)

### Core Topics
- **Delta Sharing:** Share tables across organizations, recipient configuration  
- **Lakehouse Federation:** Query external data sources (PostgreSQL, MySQL) without ingestion  
- **Unity Catalog Shares:** Creating shares, managing recipients  
- **Cross-workspace Collaboration:** Catalog federation, metastore linking

### Repo Coverage
❌ **CRITICAL GAP:** This domain is **COMPLETELY MISSING**  
- No Delta Sharing examples  
- No Lakehouse Federation examples  
- No Unity Catalog share configuration

### Recommended Additions
1. **New study note:** `docs/study-notes/day16-delta-sharing.md`  
   - Delta Sharing setup and configuration  
   - Creating shares in Unity Catalog  
   - Recipient management  
   - Lakehouse Federation with PostgreSQL example

2. **New notebook:** `notebooks/day16_delta_sharing_federation.py`  
   - Practical Delta Sharing example  
   - Lakehouse Federation query examples

3. **New exam reference:** `docs/exam/delta-sharing-cheatsheet.md`

---

## Domain 5: Monitoring and Alerting (10%)

### Core Topics
- **Spark UI:** Job stages, task details, DAG visualization, shuffle metrics  
- **Delta Lake Metrics:** Table history, transaction log, CDF (Change Data Feed)  
- **DLT Observability:** Pipeline event logs, data quality metrics  
- **Job Monitoring:** Task runtime, cluster metrics, failure notifications  
- **Logging:** Structured logging, log aggregation  
- **Alerting:** Email/webhook notifications on pipeline failures

### Repo Coverage
⚠️ **MODERATE:**  
- Study note Day 12 covers Spark UI briefly  
- Study note Day 7 mentions DLT lineage  

❌ **GAPS:**  
- ❌ No hands-on Spark UI analysis examples  
- ❌ No DLT event log querying  
- ❌ No alerting configuration examples  
- ❌ No Change Data Feed examples  
- ❌ No structured logging implementation

### Recommended Additions
1. **New study note:** `docs/study-notes/day17-monitoring-observability.md`  
   - Spark UI deep dive (stages, tasks, shuffle)  
   - DLT event log queries  
   - Delta Lake CDF setup  
   - Job alerting configuration

2. **New module:** `src/utils/monitoring.py`  
   - Structured logging wrapper  
   - Metrics collection utilities  
   - Alerting helpers (email, webhook)

3. **New notebook:** `notebooks/day17_spark_ui_analysis.py`  
   - Analyze Spark UI for EEG processing jobs  
   - Query DLT event logs  
   - Enable and query Change Data Feed

---

## Domain 6: Cost & Performance Optimisation (13% — 2nd HIGHEST)

### Core Topics
- **Partitioning:** Strategies, Z-ordering, compaction  
- **Caching:** DataFrame caching, Delta caching, disk vs memory  
- **AQE (Adaptive Query Execution):** Dynamic partition pruning, join optimization  
- **Photon Engine:** When to enable, performance gains  
- **Serverless Compute:** Cost comparison, use cases  
- **Cluster Configuration:** Autoscaling, node types, spot instances  
- **Delta Optimizations:** OPTIMIZE, VACUUM, file size management  
- **Cost Management:** DBU monitoring, job cost analysis

### Repo Coverage
✅ **STRONG:**  
- Study note Day 12 covers OPTIMIZE, ZORDER, AQE, caching  
- `notebooks/day12_performance.py` has optimization examples

⚠️ **GAPS:**  
- ❌ No Photon Engine comparison  
- ❌ No serverless compute examples  
- ❌ No cost analysis examples  
- ❌ No cluster autoscaling configuration  
- ❌ Limited partitioning strategy analysis

### Recommended Additions
1. **Expand:** Study note Day 12 with:  
   - Photon Engine benchmarks  
   - Serverless vs classic compute comparison  
   - Cost monitoring queries  
   - Advanced partitioning strategies

2. **New exam reference:** `docs/exam/cost-optimization-checklist.md`

3. **New notebook:** `notebooks/day18_cost_performance.py`  
   - Photon enable/disable comparison  
   - File size impact on query performance  
   - Cluster autoscaling configuration

---

## Domain 7: Ensuring Data Security and Compliance (10%)

### Core Topics
- **Unity Catalog Security:** GRANT/REVOKE privileges  
- **Row-Level Security:** Row filters in UC  
- **Column-Level Security:** Column masks, dynamic views  
- **Data Encryption:** At rest and in transit  
- **PII Protection:** Masking, tokenization, anonymization  
- **Audit Logging:** Table access logs, Unity Catalog audit  
- **Secrets Management:** Databricks secrets, Azure Key Vault integration

### Repo Coverage
⚠️ **MODERATE:**  
- Study note Day 8 covers Unity Catalog GRANT/REVOKE  
- `docs/exam/uc-governance.md` has governance basics

❌ **GAPS:**  
- ❌ No row filter examples  
- ❌ No column mask examples  
- ❌ No PII masking implementations  
- ❌ No audit log querying  
- ❌ No secrets management examples

### Recommended Additions
1. **Expand:** `docs/exam/uc-governance.md` with:  
   - Row filter implementation for subject privacy  
   - Column mask for PII (subject names, birthdates)  
   - Audit log analysis queries

2. **New module:** `src/utils/security.py`  
   - PII masking functions  
   - Data anonymization utilities  
   - Secret retrieval wrappers

3. **New study note:** `docs/study-notes/day19-security-compliance.md`

---

## Domain 8: Data Governance (7%)

### Core Topics
- **Unity Catalog:** Catalog/schema/table hierarchy  
- **Metastore Management:** External vs managed metastores  
- **Lineage:** Automatic lineage tracking  
- **Tags & Metadata:** Table tagging, schema comments  
- **Data Discovery:** Search and discovery in UC  
- **External Locations:** Configuring cloud storage paths  
- **Managed vs External Tables:** When to use each

### Repo Coverage
✅ **GOOD:**  
- Study note Day 8 covers UC hierarchy  
- `notebooks/day08_unity_catalog.py` has setup examples

⚠️ **GAPS:**  
- ❌ No lineage visualization/querying  
- ❌ No table tagging examples  
- ❌ No external location configuration  
- ❌ Limited metastore management

### Recommended Additions
1. **Expand:** Study note Day 8 with:  
   - Lineage query examples  
   - Table tagging best practices  
   - External location setup

2. **New notebook section:** Add to `notebooks/day08_unity_catalog.py`  
   - Query system tables for lineage  
   - Tag tables with domain metadata

---

## Domain 9: Debugging and Deploying (10%)

### Core Topics
- **Databricks CLI:** Authentication, workspace operations, job management  
- **REST API:** Jobs API, Workspace API, DBFS API  
- **Databricks Asset Bundles (DABs):** Bundle structure, deployment targets  
- **CI/CD:** GitHub Actions, Azure DevOps integration  
- **Error Handling:** Try/except patterns, retry logic  
- **Debugging Tools:** Spark UI, logs, breakpoints in notebooks  
- **Version Control:** Git integration, notebook versioning

### Repo Coverage
✅ **STRONG:**  
- Study note Day 11 covers DABs and CI/CD  
- `.github/workflows/ci.yml` has GitHub Actions setup  
- `databricks.yml` has Asset Bundle structure

⚠️ **GAPS:**  
- ❌ No Databricks CLI examples  
- ❌ No REST API usage examples  
- ❌ Limited error handling patterns  
- ❌ No debugging workflow examples

### Recommended Additions
1. **New study note:** `docs/study-notes/day20-databricks-cli-api.md`  
   - CLI authentication and common commands  
   - REST API examples (Jobs, Workspace)  
   - Error handling best practices

2. **New exam reference:** `docs/exam/cli-api-cheatsheet.md`  
   - Common CLI commands  
   - REST API endpoints reference

---

## Domain 10: Data Modelling (6%)

### Core Topics
- **Medallion Architecture:** Bronze/Silver/Gold best practices  
- **SCD (Slowly Changing Dimensions):** Type 1, Type 2 implementations  
- **Star Schema & Kimball:** Fact/dimension tables  
- **Data Vault:** Hub/link/satellite patterns  
- **Denormalization:** When and how to denormalize  
- **Partitioning Strategy:** Partition key selection

### Repo Coverage
✅ **EXCELLENT:**  
- Medallion architecture is the core design  
- Bronze/Silver/Gold clearly separated in `src/`  
- Study note Day 1 explains medallion architecture

⚠️ **GAPS:**  
- ❌ No SCD Type 2 implementation (only Type 1 mentioned)  
- ❌ No star schema example  
- ❌ No Data Vault patterns

### Recommended Additions
1. **New module:** `src/gold/dimensional_model.py`  
   - Star schema for EEG analysis (subject dim, epoch fact)  
   - SCD Type 2 for subject metadata history

2. **New study note:** `docs/study-notes/day21-data-modeling.md`  
   - SCD Type 1 vs Type 2 comparison  
   - Star schema design for EEG data  
   - Partitioning strategy analysis

---

## Coverage Summary

| Domain | Current Coverage | Gap Severity | Priority |
|--------|------------------|--------------|----------|
| 1. Code Development (22%) | 🟡 Moderate | Medium | **HIGH** |
| 2. Ingestion (7%) | 🟢 Good | Low | Medium |
| 3. Transformation & Quality (10%) | 🟡 Moderate | Medium | High |
| 4. Data Sharing (5%) | 🔴 None | **CRITICAL** | **HIGHEST** |
| 5. Monitoring (10%) | 🟠 Weak | High | **HIGH** |
| 6. Performance (13%) | 🟡 Moderate | Medium | **HIGH** |
| 7. Security (10%) | 🟠 Weak | High | High |
| 8. Governance (7%) | 🟡 Moderate | Medium | Medium |
| 9. Debugging & Deploy (10%) | 🟢 Good | Low | Medium |
| 10. Data Modelling (6%) | 🟢 Good | Low | Low |

### Overall Assessment
**Current Alignment: ~60%** — This repo was built for the **Associate** exam (5 domains), not the **Professional** (10 domains).

### Critical Gaps (Must Fix)
1. ❌ **Domain 4:** Data Sharing & Federation — COMPLETELY MISSING
2. ❌ **Domain 5:** Monitoring & Alerting — Very weak
3. ❌ **Domain 7:** Security (row filters, column masks, PII)

### High-Priority Gaps
4. ❌ **Domain 1:** Advanced Spark (broadcast joins, skew, CTEs)
5. ❌ **Domain 6:** Cost optimization (Photon, serverless, cost analysis)

---

## Recommended Enhancement Plan

See `docs/exam/professional-upgrade-plan.md` for the complete step-by-step upgrade guide.
