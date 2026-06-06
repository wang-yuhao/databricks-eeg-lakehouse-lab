# Day 1 — Repo Bootstrap & Goal Alignment

**Exam Domain:** Lakehouse fundamentals, Databricks architecture overview 
**Pipeline Layer:** Foundations 
**Session Time:** ~2 hours

---

## Learning Objectives

By the end of Day 1 you should be able to:
- Describe the three-layer Lakehouse architecture (Bronze / Silver / Gold)
- Explain what Databricks Asset Bundles (DAB) are and why they matter for CI/CD
- Map every exam domain to a concrete repo file
- Articulate the research hypothesis and how the pipeline tests it

---

## Core Concepts

### 1. Lakehouse Architecture

The Databricks Lakehouse combines the reliability of a data warehouse with the flexibility of a data lake:

```
Data Sources  →  [BRONZE]  →  [SILVER]  →  [GOLD]  →  Consumers
                 Raw ingest    Cleaned     Aggregated
                 Delta table   validated   ML-ready
```

| Layer  | Purpose                         | Schema enforcement | Typical writers        |
|--------|---------------------------------|--------------------|------------------------|
| Bronze | Exact copy of source data        | Relaxed (schema-on-read) | Auto Loader, COPY INTO |
| Silver | Cleaned, validated, conformed    | Enforced           | Spark UDFs, DLT        |
| Gold   | Business-level aggregates        | Strict             | SQL, Feature Store     |

**Key insight:** Bronze tables should be **append-only** and **idempotent**. Never delete raw data.

### 2. Delta Lake Fundamentals

Delta Lake adds an **ACID transaction log** (`_delta_log/`) on top of Parquet files:

- Every write creates a new JSON commit entry in `_delta_log/`
- Reads load only the latest snapshot (time travel is free)
- `DESCRIBE HISTORY <table>` shows the full audit trail
- `VACUUM <table> RETAIN <n> HOURS` removes old Parquet files

```sql
-- Check table history
DESCRIBE HISTORY eeg_lakehouse.bronze.eeg_files;

-- Time travel
SELECT * FROM eeg_lakehouse.bronze.eeg_files VERSION AS OF 3;
SELECT * FROM eeg_lakehouse.bronze.eeg_files TIMESTAMP AS OF '2026-01-15';
```

### 3. Databricks Asset Bundles (DAB)

DAB is the recommended way to deploy Databricks workloads as code:

```yaml
# databricks.yml — minimal bundle
bundle:
  name: eeg-lakehouse-lab

targets:
  dev:
    workspace:
      host: https://<your-workspace>.azuredatabricks.net
  prod:
    workspace:
      host: https://<prod-workspace>.azuredatabricks.net

resources:
  jobs:
    eeg_pipeline:
      name: EEG Bronze-to-Gold Pipeline
      tasks:
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: notebooks/day03_bronze_ingestion
```

**CLI commands:**
```bash
databricks bundle validate     # check config
databricks bundle deploy       # deploy to target
databricks bundle run eeg_pipeline  # run a job
```

### 4. Repository Structure Philosophy

This repo follows the **production monorepo** pattern:

```
src/       — importable Python modules (testable with pytest)
notebooks/ — Databricks notebooks (thin wrappers calling src/ functions)
docs/      — study notes, exam cheatsheets, research docs
tests/     — pytest unit tests with local SparkSession
```

**Why separate src/ from notebooks?**
- Notebooks are hard to test (no pytest support without extra tooling)
- Pure Python modules in `src/` can be unit-tested locally
- Notebooks import from `src/` → thin orchestration layer only

---

## Exam Focus Areas

### Frequently Tested: Delta Lake Properties

| Property        | Detail                                                  |
|-----------------|---------------------------------------------------------|
| ACID            | Atomicity, Consistency, Isolation, Durability via transaction log |
| Optimistic concurrency | Multiple writers conflict-detect on commit       |
| Schema enforcement | `mergeSchema` option, `overwriteSchema` option      |
| Time travel     | `VERSION AS OF`, `TIMESTAMP AS OF`                     |
| Audit log       | `DESCRIBE HISTORY` shows all operations                |

### Common Exam Trap: VACUUM

```sql
-- Default retention is 7 days (168 hours)
VACUUM my_table;                    -- keeps last 7 days
VACUUM my_table RETAIN 0 HOURS;    -- ERROR: below safe threshold

-- To override (NOT recommended in prod):
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM my_table RETAIN 0 HOURS;
```

> ⚠️ If you VACUUM with too short retention, time travel to those versions breaks.

---

## Research Context

**Project:** Persistent Homology Reveals Topological Dynamics of Sleep EEG Networks During Memory Consolidation

**Dataset:** PhysioNet Sleep-EDF Expanded — 197 subjects, overnight polysomnography
- EEG channels: Fpz-Cz, Pz-Oz (2-channel cassette recordings)
- Sampling rate: 100 Hz
- Hypnogram: 30-second epochs labeled W/1/2/3/R

**Research hypotheses:**
1. SO-spindle coupling windows → higher β₁ persistence than non-coupling
2. High spindle-density subjects → lower Wasserstein distance (topological stability)
3. TDA features → ΔR² > 0.10 over spectral baselines in memory proxy prediction

**Why this data in a Databricks lakehouse?**
- 197 subjects × 2 nights × ~8 hours × 100 Hz × 2 channels = ~56 GB raw
- EDF binary format requires custom Pandas UDFs (MNE-Python)
- Gold feature table is small enough for ML but pipeline exercises all Databricks patterns

---

## Key Files Created Today

| File | Purpose |
|------|---------|
| `README.md` | Project overview, architecture, exam domain table |
| `databricks.yml` | Asset Bundle skeleton |
| `requirements.txt` | Python dependencies |
| `src/utils/config.py` | `AppConfig` dataclass — paths, catalog, schema |
| `src/utils/logging.py` | Loguru-based structured logger |
| `docs/daily-plan.md` | 14-day checklist |
| `docs/exam/domains-overview.md` | Exam domain → file mapping |
| `docs/research/project-overview.md` | EEG TDA research description |
| `tests/conftest.py` | Local SparkSession fixture for pytest |

---

## Self-Check Questions

1. What is the purpose of the `_delta_log/` directory?
2. What happens when two Spark jobs write to the same Delta table concurrently?
3. What is `mergeSchema` and when would you use it?
4. How does `VACUUM` interact with time travel?
5. What is a Databricks Asset Bundle and how does it differ from a Databricks Repo?
6. Why should notebooks be thin wrappers around `src/` modules?

---

## Further Reading

- [Delta Lake Documentation](https://docs.delta.io/latest/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [PhysioNet Sleep-EDF Expanded](https://physionet.org/content/sleep-edfx/1.0.0/)
