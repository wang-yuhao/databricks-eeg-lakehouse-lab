# Databricks Data Engineer Associate 2026 — Exam Domains Overview

> This document maps every official exam domain to concrete files in this repo.

---

## Official Exam Domains (Approximate Weights)

| # | Domain | Weight | Repo Coverage |
|---|---|---|---|
| 1 | Databricks Lakehouse Platform | ~24% | `src/utils/config.py`, `notebooks/day01_intro_and_setup.py`, this doc |
| 2 | ELT with Spark SQL and Python | ~29% | `src/silver/`, `src/gold/`, all notebooks |
| 3 | Incremental Data Processing | ~22% | `src/bronze/ingest_eeg_files.py` (Auto Loader), `src/bronze/ingest_streaming_events.py` |
| 4 | Production Pipelines (DLT) | ~16% | `src/` DLT decorators, `resources/eeg_dlt_pipeline.yml`, `docs/exam/dlt-cheatsheet.md` |
| 5 | Data Governance (Unity Catalog) | ~9% | `notebooks/day08_unity_catalog_setup.py`, `docs/exam/uc-governance.md` |

---

## Domain 1: Databricks Lakehouse Platform

**Key concepts:**
- Delta Lake as the storage layer (ACID, time travel, DML)
- Lakehouse vs Data Warehouse vs Data Lake: no data movement, open format, BI + ML on same data
- Cluster types: All-Purpose (interactive) vs Job (scheduled) vs SQL Warehouse (BI/SQL)
- Databricks Runtime versions and LTS releases
- Unity Catalog object hierarchy: Metastore → Catalog → Schema → Table/View/Volume

**Exam traps:**
- Delta tables do NOT require a running cluster to retain data — data lives in object storage
- Unity Catalog metastore is 1-per-region per Databricks account (not per workspace)
- `DESCRIBE EXTENDED` shows table location; `DESCRIBE DETAIL` shows full Delta metadata

**Repo file:** `src/utils/config.py` defines catalog/schema/table naming convention used throughout.

---

## Domain 2: ELT with Spark SQL and Python

**Key concepts:**
- DataFrame API vs Spark SQL — functionally equivalent, interoperable
- Lazy evaluation: transformations build a logical plan; actions trigger execution
- `cache()` vs `persist()` — cache = memory only; persist = configurable storage level
- UDF vs Pandas UDF: UDF = row-by-row (slow), Pandas UDF = vectorized (fast), use Pandas UDF for EEG signal processing
- Window functions: `OVER (PARTITION BY subject_id ORDER BY epoch_start)`
- `explode()` for unnesting event arrays (used in spindle event expansion)

**Exam traps:**
- `filter()` and `where()` are aliases
- `groupBy().agg()` requires explicit import: `from pyspark.sql import functions as F`
- Python UDFs serialize data row-by-row through JVM ↔ Python boundary — always prefer Pandas UDF for numerical EEG work

**Repo files:** `src/silver/preprocess_eeg.py`, `src/silver/detect_events.py`, `src/gold/build_features.py`

---

## Domain 3: Incremental Data Processing

**Key concepts:**
- Auto Loader (`.format("cloudFiles")`): file notification vs directory listing mode
  - Notification mode: uses Azure Event Grid + Queue Storage → scalable for millions of files
  - Directory listing: simpler, no extra services, adequate for < 1M files
- `cloudFiles.schemaLocation`: Auto Loader infers and evolves schema automatically
- Structured Streaming: micro-batch vs continuous, output modes (append / complete / update)
- Watermarks: `withWatermark("event_time", "10 minutes")` — drops late data beyond threshold
- Checkpointing: `option("checkpointLocation", ...)` — mandatory for fault tolerance

**Exam traps:**
- Auto Loader with `cloudFiles.format` = `json` still needs `cloudFiles.schemaLocation` to avoid full re-scan on restart
- `complete` output mode materializes the entire result — use only with aggregations, not with append-only streams
- NREM sleep simulation: incoming "EEG events" can be modeled as a rate source for exam practice

**Repo files:** `src/bronze/ingest_eeg_files.py`, `src/bronze/ingest_streaming_events.py`

---

## Domain 4: Production Pipelines (Delta Live Tables)

**Key concepts:**
- `@dlt.table` — defines a materialized Delta table
- `@dlt.view` — ephemeral, not materialized
- `dlt.read("table_name")` — read from within the same pipeline
- `dlt.read_stream("table_name")` — incremental read (streaming semantics)
- `@dlt.expect("rule", condition)` — logs violations, pipeline continues
- `@dlt.expect_or_drop("rule", condition)` — drops failing rows
- `@dlt.expect_or_fail("rule", condition)` — halts pipeline on failure
- Pipeline modes: Triggered (scheduled) vs Continuous
- Enhanced Autoscaling for DLT: separate from standard cluster autoscaling

**Exam traps:**
- `dlt.read()` creates a dependency in the DAG — Databricks resolves order automatically
- `@dlt.expect_or_drop` is preferred for data quality in research pipelines (drop artifacts, not fail)
- Pipeline update modes: Full Refresh vs Normal update

**Repo files:** DLT decorators in all `src/` layers; `docs/exam/dlt-cheatsheet.md`

---

## Domain 5: Data Governance (Unity Catalog)

**Key concepts:**
- Three-level namespace: `catalog.schema.table`
- Object types: TABLE, VIEW, VOLUME, FUNCTION, SHARE (Delta Sharing)
- Volumes: managed (UC-controlled path) vs external (customer ADLS path)
- `GRANT privilege ON securable_object TO principal`
- Privileges: SELECT, MODIFY, CREATE TABLE, USE CATALOG, USE SCHEMA, ALL PRIVILEGES
- Column-level masking and row-level security via dynamic views
- Data lineage: Unity Catalog tracks lineage automatically at column level

**Exam traps:**
- You need `USE CATALOG` + `USE SCHEMA` before table-level SELECT
- External locations require a credential (Service Principal / Managed Identity) — not just a path
- Metastore admin ≠ workspace admin; separate roles

**Repo files:** `notebooks/day08_unity_catalog_setup.py`, `docs/exam/uc-governance.md`

---

## How to Study with This Repo

1. **Do the daily task** — run the code, commit it.
2. **After each day** — re-read the relevant exam section above and ask: "Can I explain this to an interviewer using my EEG pipeline as the example?"
3. **Week 2 review** — run the mini-labs in `notebooks/day13_exam_mini_labs.py` timed (30 min).
4. **Mock questions** — generate 5 MCQs from each domain section in this doc.
