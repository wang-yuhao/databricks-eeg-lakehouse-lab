# Databricks notebook source
# Day 11: CI/CD — Comprehensive pytest Coverage + GitHub Actions
# ==============================================================
# Exam domains covered:
#   - pytest patterns for PySpark (no cluster required via mock DataFrames)
#   - GitHub Actions workflow for Databricks Asset Bundle (DAB) deployment
#   - Code quality gates: flake8, black, mypy
#   - Integration test patterns with Delta tables
#
# Research context:
#   Production ML pipelines require automated quality gates before
#   any EEG preprocessing changes reach the Silver/Gold tables.
#   This notebook documents the CI/CD design decisions and serves
#   as a runbook for the GitHub Actions pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CI/CD Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC GitHub Push/PR
# MAGIC     │
# MAGIC     ▼
# MAGIC GitHub Actions (.github/workflows/ci.yml)
# MAGIC     ├── lint: flake8 + black --check + mypy
# MAGIC     ├── test: pytest tests/ (mock Spark via pytest-spark or pyspark)
# MAGIC     └── deploy (main branch only):
# MAGIC             databricks bundle deploy --target staging
# MAGIC ```
# MAGIC
# MAGIC **Key principle:** Tests run WITHOUT a live Databricks cluster using
# MAGIC `pyspark.sql.SparkSession.builder.master("local[*]")`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. pytest Configuration Review
# MAGIC
# MAGIC Verify `tests/conftest.py` creates a local SparkSession fixture:

# COMMAND ----------

# Read conftest.py to review fixture setup
with open("/Workspace/Repos/wang-yuhao/databricks-eeg-lakehouse-lab/tests/conftest.py") as f:
    print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run All Tests from Notebook
# MAGIC
# MAGIC In production CI: `pytest tests/ -v --tb=short --cov=src --cov-report=xml`

# COMMAND ----------

import subprocess
result = subprocess.run(
    ["python", "-m", "pytest", "tests/", "-v", "--tb=short", "--no-header"],
    capture_output=True,
    text=True,
    cwd="/Workspace/Repos/wang-yuhao/databricks-eeg-lakehouse-lab",
)
print("STDOUT:")
print(result.stdout)
print("STDERR:")
print(result.stderr)
print(f"Return code: {result.returncode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. GitHub Actions Workflow — Key Sections
# MAGIC
# MAGIC The full workflow lives at `.github/workflows/ci.yml`.
# MAGIC Below are the critical patterns for the Databricks exam.

# COMMAND ----------

GITHUB_ACTIONS_EXPLANATION = """
CI/CD Pipeline Design Decisions
================================

1. TRIGGER: Push to main + Pull Requests
   - All PRs must pass tests before merge (branch protection rule)
   - Main branch deploys automatically to staging

2. ENVIRONMENT MATRIX:
   - Python 3.10 (matches Databricks Runtime 14.x)
   - PySpark installed via pip (same version as DBR)

3. TEST STAGES:
   a) Lint: flake8 (style) + black (formatting) + mypy (type hints)
   b) Unit tests: pytest tests/ (local Spark, no cluster needed)
   c) Coverage: codecov upload (badge in README)

4. DEPLOY (main branch only):
   - Uses Databricks Asset Bundle (DAB): databricks bundle deploy
   - Target: staging environment
   - Requires DATABRICKS_HOST + DATABRICKS_TOKEN secrets

5. KEY EXAM TOPICS:
   - databricks.yml (root bundle config) defines workspace + resources
   - bundle deploy validates schema before pushing to workspace
   - pipeline resources in bundle: DLT, Jobs, MLflow experiments

6. SECRETS MANAGEMENT:
   - DATABRICKS_HOST and DATABRICKS_TOKEN stored in GitHub Secrets
   - Never hardcode tokens — use environment variables
"""

print(GITHUB_ACTIONS_EXPLANATION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Databricks Asset Bundle (DAB) Verification
# MAGIC
# MAGIC `databricks.yml` should already be at repo root.

# COMMAND ----------

with open("/Workspace/Repos/wang-yuhao/databricks-eeg-lakehouse-lab/databricks.yml") as f:
    print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Writing Testable Code — Patterns Used in This Repo

# COMMAND ----------

TESTABILITY_PATTERNS = """
Patterns that make EEG pipeline code testable without a cluster
================================================================

1. DEPENDENCY INJECTION for SparkSession:
   - Functions accept `spark: SparkSession` as first argument
   - Tests inject a local SparkSession fixture
   - Never call spark.builder inside library functions

2. PURE TRANSFORMATION FUNCTIONS:
   - src/silver/preprocess.py: bandpass_filter(df, ...) → DataFrame
   - No side effects (no writes inside transformation functions)
   - Easy to test with small mock DataFrames

3. SCHEMA VALIDATION HELPERS:
   - tests/test_bronze.py: assert actual schema == expected schema
   - Catches breaking schema changes before deployment

4. MOCK DELTA TABLES:
   - Use spark.createDataFrame(data, schema) for test input
   - tmpdir fixture for Delta write path
   - DeltaTable.isDeltaTable() returns False on tmpdir → graceful fallback

5. PARAMETRISED TESTS:
   - @pytest.mark.parametrize for multiple sleep stage values
   - Tests all EEG annotation labels (W, N1, N2, N3, R) systematically
"""

print(TESTABILITY_PATTERNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Coverage Report Interpretation

# COMMAND ----------

COVERAGE_TARGETS = {
    "src/bronze/": "≥ 90%",
    "src/silver/": "≥ 85%",
    "src/gold/": "≥ 85%",
    "src/streaming/": "≥ 70% (harder to test streaming without mock)",
    "src/dlt/": "≥ 60% (DLT decorators need Databricks runtime)",
}

print("Coverage targets by module:")
for module, target in COVERAGE_TARGETS.items():
    print(f"  {module:<25} {target}")

print()
print("Run with coverage:")
print("  pytest tests/ --cov=src --cov-report=term-missing --cov-fail-under=80")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. CI Exam Cheatsheet
# MAGIC
# MAGIC | Concept | Command/Pattern |
# MAGIC |---------|----------------|
# MAGIC | Run tests | `pytest tests/ -v` |
# MAGIC | Coverage | `pytest --cov=src --cov-report=xml` |
# MAGIC | Lint | `flake8 src/ tests/` |
# MAGIC | Format check | `black --check src/ tests/` |
# MAGIC | Type check | `mypy src/` |
# MAGIC | Bundle validate | `databricks bundle validate` |
# MAGIC | Bundle deploy | `databricks bundle deploy --target staging` |
# MAGIC | Bundle run | `databricks bundle run eeg_dlt_pipeline` |
