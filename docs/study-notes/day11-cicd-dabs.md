# Day 11 – CI/CD & Databricks Asset Bundles (DABs)

## Overview
Production data engineering requires automated testing and deployment pipelines.
Today covers GitHub Actions CI, Databricks Asset Bundles (DABs) for IaC-style
deployments, and testing strategies for PySpark code.

---

## Core Concepts

### 1. Databricks Asset Bundles (DABs)
DABs define Databricks resources (jobs, pipelines, clusters) as code in `databricks.yml`.

```yaml
# databricks.yml
bundle:
  name: eeg-lakehouse

variables:
  catalog:
    default: eeg_catalog
  env:
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-xxxx.azuredatabricks.net

  prod:
    mode: production
    workspace:
      host: https://adb-yyyy.azuredatabricks.net

resources:
  jobs:
    eeg_bronze_ingestion:
      name: "EEG Bronze Ingestion - ${var.env}"
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: notebooks/day03_bronze_ingestion.py
          job_cluster_key: shared_cluster
      job_clusters:
        - job_cluster_key: shared_cluster
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: Standard_DS3_v2
            num_workers: 2
```

**DABs CLI commands:**
```bash
databricks bundle validate       # Validate databricks.yml
databricks bundle deploy         # Deploy to workspace
databricks bundle run <job_name> # Run a job
databricks bundle destroy        # Remove all resources
```

### 2. GitHub Actions CI Pipeline
```yaml
# .github/workflows/ci.yml
name: EEG Lakehouse CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install -e ".[dev]"

      - name: Lint with Black
        run: black --check src/ tests/
        continue-on-error: true  # Non-fatal; format is enforced in PR review

      - name: Run tests
        run: pytest tests/ -v --tb=short
```

### 3. Testing PySpark Code
```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("eeg-test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

# tests/test_bronze.py
def test_bronze_schema(spark):
    from src.bronze.ingest import ingest_eeg_bronze
    df = ingest_eeg_bronze(spark, test_path)
    assert "subject_id" in df.columns
    assert "epoch_id" in df.columns
    assert df.count() > 0
```

### 4. Test Pyramid for Data Engineering
```
               /\ Integration tests (Databricks cluster)
              /  \ e.g., full pipeline end-to-end
             /----\
            / Unit  \ Unit tests (local SparkSession)
           /  tests  \ e.g., transformation logic
          /----------\
         /  Schema    \ Schema validation (local)
        /   checks     \ e.g., column names, dtypes
       /--------------\
```

### 5. Code Quality Tools
| Tool | Purpose | Config |
|---|---|---|
| `black` | Code formatting | `pyproject.toml` |
| `flake8` | Style linting | `.flake8` |
| `mypy` | Type checking | `mypy.ini` |
| `pytest` | Testing | `pytest.ini` or `pyproject.toml` |
| `pre-commit` | Local git hooks | `.pre-commit-config.yaml` |

```toml
# pyproject.toml
[tool.black]
line-length = 100
target-version = ["py311"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --tb=short"
```

---

## EEG / Neuroscience Context

### CI/CD in Clinical Data Pipelines
- **Regulatory requirement:** Clinical software must have auditable change history.
- **Pipeline validation:** Automated tests prevent broken EEG processing reaching production.
- **DABs for reproducibility:** Each experiment can deploy the exact same pipeline version.
- **Schema evolution tests:** Catch breaking changes when EEG device firmware updates alter data format.

---

## Exam-Focused Summary

| Topic | Key Fact |
|---|---|
| DABs | `databricks.yml` defines jobs/pipelines as code |
| `bundle deploy` | Deploys resources to workspace |
| `availableNow` trigger | Preferred for scheduled streaming in production |
| GitHub Actions | `actions/checkout`, `setup-python`, then `pytest` |
| `conftest.py` | Shared fixtures; `scope="session"` reuses SparkSession |
| Test pyramid | Unit (local) → Integration (cluster) → E2E |

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `databricks.yml` | DABs bundle definition for all jobs and pipelines |
| `.github/workflows/ci.yml` | GitHub Actions CI pipeline |
| `tests/conftest.py` | Shared SparkSession fixture |
| `pyproject.toml` | Black, pytest configuration |

---

## Self-Check Questions
1. What is the difference between `bundle deploy` and `bundle run`?
2. Why do we use `scope="session"` for the SparkSession fixture?
3. What does `continue-on-error: true` do in GitHub Actions?
4. What are the three levels of the DE test pyramid?
5. Why should clinical EEG pipelines have CI/CD?
6. How do DABs support multi-environment deployments (dev/staging/prod)?

---

## Further Reading
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [pytest Documentation](https://docs.pytest.org/)
