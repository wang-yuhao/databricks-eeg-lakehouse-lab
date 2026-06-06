# Databricks Data Engineer Associate — Exam Domains Overview

> 📅 2026 exam blueprint. Use this file as your running checklist.

---

## Domain 1: Databricks Lakehouse Platform (24%)

**Core topics:**
- Lakehouse vs Data Warehouse vs Data Lake architecture
- Delta Lake format: ACID transactions, time travel, schema enforcement
- Databricks workspace, clusters, notebooks, repos
- Unity Catalog: catalog/schema/table/volume hierarchy
- Databricks SQL, Photon engine

**How this repo covers it:**
- Bronze/Silver/Gold medallion architecture in `src/`
- Unity Catalog setup in `notebooks/day08_unity_catalog_setup.py`
- `docs/exam/uc-governance.md`

**Key exam pitfall:** Know the difference between *managed* vs *external* Delta tables (managed tables: Databricks controls storage; external: you specify LOCATION). Dropping a managed table deletes data; dropping external table keeps data.

---

## Domain 2: ELT with Apache Spark (31%)

**Core topics:**
- DataFrame API: `select`, `filter`, `groupBy`, `agg`, `join`, `window`
- PySpark UDFs: Python UDF vs Pandas UDF (Arrow-based) vs `mapInPandas`
- Nested data: `StructType`, `ArrayType`, `MapType`, `explode`, `flatten`
- Higher-order functions: `transform`, `filter`, `aggregate`
- Reading/writing: Parquet, Delta, JSON, CSV with options
- Schema inference vs explicit schema

**How this repo covers it:**
- Silver preprocessing with Pandas UDF: `src/silver/preprocess_eeg.py`
- Event detection with nested structs: `src/silver/detect_events.py`
- Gold feature joins: `src/gold/build_features.py`

**Key exam pitfall:** `@pandas_udf` requires matching return type declaration. Arrow optimization applies only to Pandas UDFs, not Python UDFs — Python UDFs serialize row-by-row (slow for EEG signal processing at scale).

---

## Domain 3: Incremental Data Processing (28%)

**Core topics:**
- Auto Loader (`format("cloudFiles")`) — schema inference, schema evolution, checkpointing
- Delta Live Tables (DLT) — `@dlt.table`, `@dlt.expect`, LIVE vs streaming tables
- Structured Streaming — `readStream`, `writeStream`, triggers, watermarks, output modes
- MERGE INTO — upserts, SCD Type 1/2 patterns
- COPY INTO vs Auto Loader (know when to use each)

**How this repo covers it:**
- Auto Loader ingestion: `src/bronze/ingest_eeg_files.py`
- DLT pipeline: `resources/eeg_dlt_pipeline.yml` + `notebooks/day07_dlt_pipeline.py`
- Streaming path: `src/bronze/ingest_streaming_events.py`
- MERGE INTO mini-lab: `notebooks/day13_exam_mini_labs.py`

**Key exam pitfall:** Auto Loader vs COPY INTO — Auto Loader is for streaming/incremental (tracks new files automatically via cloud notifications or directory listing); COPY INTO is for batch (idempotent, loads once, skips already-loaded files).

---

## Domain 4: Production Pipelines (10%)

**Core topics:**
- Databricks Jobs: tasks, dependencies, clusters, scheduling
- DLT pipeline deployment and monitoring
- Databricks Repos and Git integration
- Databricks CLI and Asset Bundles
- Error handling, retry logic, notifications

**How this repo covers it:**
- `databricks.yml` bundle skeleton (Jobs + DLT pipeline)
- `.github/workflows/ci.yml` CI/CD
- `docs/exam/ci-cd-notes.md`

**Key exam pitfall:** DLT handles retries and checkpointing automatically; manual Spark Structured Streaming jobs require explicit checkpoint locations.

---

## Domain 5: Data Governance (7%)

**Core topics:**
- Unity Catalog: GRANT, REVOKE, privileges on catalog/schema/table
- Row-level security (row filters) and column-level security (column masks)
- Data lineage and audit logging
- Volume management for unstructured data

**How this repo covers it:**
- `docs/exam/uc-governance.md`
- `notebooks/day08_unity_catalog_setup.py`

**Key exam pitfall:** `GRANT SELECT ON TABLE` vs `GRANT SELECT ON SCHEMA` — schema-level grant applies to all current AND future tables; table-level grant is per-table.

---

## Quick Reference: Exam Weights

| Domain | Weight | Status |
|--------|--------|--------|
| Databricks Lakehouse Platform | 24% | 🟡 In progress |
| ELT with Apache Spark | 31% | 🟡 In progress |
| Incremental Data Processing | 28% | 🔴 Not started |
| Production Pipelines | 10% | 🔴 Not started |
| Data Governance | 7% | 🔴 Not started |

Update status as you complete each day's tasks.

---

## Study Resources

- [Databricks Academy: Data Engineer Learning Path](https://customer-academy.databricks.com/learn/learning_plan/view/15/data-engineer-learning-plan)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [DLT Concepts](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-concepts.html)
