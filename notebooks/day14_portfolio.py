# Databricks notebook source
# Day 14: Portfolio Polish — STAR Stories, Exam Coverage, Interview Prep
# ======================================================================
# This notebook is a self-contained portfolio summary and interview preparation
# tool.  It generates a structured overview of the 14-day project, prints
# exam domain coverage, and provides a mock interview Q&A drill.
#
# Run this notebook top-to-bottom before any Databricks interview or exam.

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks EEG Lakehouse Lab — Day 14: Portfolio Summary
# MAGIC
# MAGIC **Repo:** `github.com/wang-yuhao/databricks-eeg-lakehouse-lab`
# MAGIC **Purpose:** Three-in-one: Databricks 2026 Exam Prep + Sleep EEG Research + Senior DE Portfolio

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What Was Built (14-Day Overview)

# COMMAND ----------

DAILY_SUMMARY = {
    "Day 1": "Repo bootstrap, goals, exam domain map, README, databricks.yml",
    "Day 2": "Bronze schema design, Auto Loader skeleton, dataset interface",
    "Day 3": "Bronze ingestion, Delta basics, DESCRIBE HISTORY, time travel",
    "Day 4": "Silver preprocessing UDFs: bandpass filter, Pandas UDFs",
    "Day 5": "Silver event detection: sleep spindles, nested structs, explode",
    "Day 6": "Gold feature table: OPTIMIZE/ZORDER/MERGE patterns",
    "Day 7": "DLT pipeline: @dlt.table + @dlt.expect + pipeline modes",
    "Day 8": "Unity Catalog: GRANT/REVOKE, row filters, column masks",
    "Day 9": "Structured Streaming: watermarks, ForeachBatch, triggers",
    "Day 10": "MLflow XGBoost + SHAP: EEG memory predictor, model registry",
    "Day 11": "pytest CI/CD: comprehensive coverage, GitHub Actions, DAB",
    "Day 12": "Performance tuning: AQE, broadcast joins, OPTIMIZE, caching",
    "Day 13": "Exam mini-labs: CTAS, INSERT/MERGE/RESTORE, COPY INTO",
    "Day 14": "Portfolio polish: STAR stories, exam coverage, interview prep",
}

print("=" * 70)
print("14-DAY BUILD SUMMARY")
print("=" * 70)
for day, summary in DAILY_SUMMARY.items():
    print(f"  {day:<8} {summary}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exam Domain Coverage Check

# COMMAND ----------

EXAM_COVERAGE = {
    "Databricks Lakehouse Platform (~24%)": {
        "Days": "1-3, 8",
        "Topics": "Delta Lake, Unity Catalog, Medallion, Auto Loader",
        "Confidence": "HIGH",
    },
    "ELT with Spark SQL & Python (~29%)": {
        "Days": "4-6, 13",
        "Topics": "UDFs, MERGE, CTAS, window functions, nested structs",
        "Confidence": "HIGH",
    },
    "Incremental Data Processing (~22%)": {
        "Days": "3, 7, 9",
        "Topics": "Auto Loader, DLT, Structured Streaming, watermarks",
        "Confidence": "HIGH",
    },
    "Production Pipelines (~16%)": {
        "Days": "7, 11",
        "Topics": "DLT modes, CI/CD, pytest, GitHub Actions, DAB",
        "Confidence": "MEDIUM-HIGH",
    },
    "Data Governance & Security (~9%)": {
        "Days": "8",
        "Topics": "GRANT/REVOKE, row filters, column masks, audit logs",
        "Confidence": "HIGH",
    },
}

print("=" * 70)
print("EXAM DOMAIN COVERAGE")
print("=" * 70)
for domain, info in EXAM_COVERAGE.items():
    print(f"\n  {domain}")
    print(f"    Days:       {info['Days']}")
    print(f"    Topics:     {info['Topics']}")
    print(f"    Confidence: {info['Confidence']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Mock Interview — Technical Deep Dive
# MAGIC
# MAGIC Practice answering these questions out loud before interviews.
# MAGIC The model answers are below each question.

# COMMAND ----------

MOCK_INTERVIEW_QA = [
    {
        "q": "Walk me through your EEG lakehouse architecture.",
        "a": (
            "The pipeline follows a Medallion architecture implemented both as a DLT graph and "
            "standalone PySpark modules. Bronze ingests raw Parquet files from PhysioNet Sleep-EDF "
            "via Auto Loader with schema evolution. Silver applies bandpass filtering and spindle "
            "detection using Pandas UDFs with @dlt.expect_or_drop for GDPR-safe quality control. "
            "Gold aggregates per-subject spectral features (delta/theta ratio, spindle density, "
            "SWS ratio) that feed an XGBoost memory consolidation predictor tracked in MLflow."
        ),
    },
    {
        "q": "What's the difference between dlt.read() and dlt.read_stream()?",
        "a": (
            "dlt.read() reads a DLT dataset as a static batch DataFrame — required for Gold "
            "aggregations that need complete data. dlt.read_stream() reads as a streaming "
            "DataFrame — used for Bronze→Silver where you want incremental processing. "
            "Using dlt.read() on a streaming source fails; using dlt.read_stream() on a "
            "batch table works but may be less efficient."
        ),
    },
    {
        "q": "How does your Unity Catalog setup enforce GDPR compliance?",
        "a": (
            "Three mechanisms: (1) Row filters — each EEG recording site only sees their own "
            "subjects via IS_ACCOUNT_GROUP_MEMBER() function. (2) Column masks — subject_id is "
            "masked to first 4 chars for non-admin groups. (3) Automatic audit logging to "
            "system.access.audit_log satisfies GDPR Article 30 accountability requirement. "
            "All controls are declarative SQL — zero custom ETL code needed."
        ),
    },
    {
        "q": "When would you use trigger(availableNow=True) vs processingTime?",
        "a": (
            "availableNow=True (replaces once=True) processes all available data and then stops "
            "— ideal for scheduled batch-style streaming jobs (e.g., nightly EEG ingestion). "
            "processingTime='30 seconds' runs continuously with micro-batches — better for "
            "near-real-time monitoring dashboards. For exam: availableNow is the modern "
            "replacement; both appear on the exam."
        ),
    },
    {
        "q": "How do you handle skewed data in Spark joins?",
        "a": (
            "Three approaches in order of preference: (1) AQE skew join — enable "
            "spark.sql.adaptive.skewJoin.enabled=true and let Databricks Runtime handle it "
            "automatically. (2) Broadcast join — if one side is small enough, force broadcast "
            "with F.broadcast(). (3) Salting — add a random suffix to the join key and "
            "replicate the smaller table with the same suffix range. In the EEG lab, "
            "subject demographics are small enough for broadcast."
        ),
    },
]

print("=" * 70)
print("MOCK INTERVIEW Q&A")
print("=" * 70)
for i, qa in enumerate(MOCK_INTERVIEW_QA, 1):
    print(f"\nQ{i}: {qa['q']}")
    print(f"\nA:  {qa['a']}")
    print("-" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Repo File Map — Quick Reference

# COMMAND ----------

REPO_MAP = """
databricks-eeg-lakehouse-lab/
├── README.md                          ← Architecture + exam domain map
├── requirements.txt                   ← All dependencies with versions
├── databricks.yml                     ← Asset Bundle config (DAB)
├── .github/workflows/ci.yml           ← GitHub Actions CI/CD
│
├── notebooks/                         ← Runnable Day 1–14 Databricks notebooks
│   ├── day01_intro_and_setup.py
│   ├── day02_bronze_schema_design.py
│   ├── day03_bronze_ingestion.py
│   ├── day04_silver_preprocessing.py
│   ├── day05_event_detection.py
│   ├── day06_gold_features.py
│   ├── day07_dlt_pipeline.py
│   ├── day08_unity_catalog.py
│   ├── day09_streaming.py
│   ├── day10_mlflow.py
│   ├── day11_cicd.py
│   ├── day12_performance.py
│   ├── day13_exam_mini_labs.py
│   └── day14_portfolio.py             ← This file
│
├── src/                               ← Production library code
│   ├── bronze/                        ← Auto Loader ingestion
│   ├── silver/                        ← UDFs, preprocessing, event detection
│   ├── gold/                          ← Feature engineering, ML training
│   ├── streaming/                     ← Structured Streaming processor
│   ├── dlt/                           ← DLT pipeline definitions
│   └── utils/                         ← Shared helpers
│
├── tests/                             ← pytest unit tests
│   ├── conftest.py                    ← Local SparkSession fixture
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
│
└── docs/
    ├── daily-plan.md
    ├── interview-star-stories.md      ← 5 STAR interview stories
    ├── exam/
    │   ├── domains-overview.md
    │   ├── delta-patterns.md
    │   ├── dlt-cheatsheet.md
    │   ├── uc-governance.md
    │   └── performance-tuning.md
    └── research/
        └── (TDA research notes)
"""

print(REPO_MAP)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Next Steps After Day 14

# COMMAND ----------

NEXT_STEPS = [
    "1. Take the Databricks Data Engineer Associate 2026 exam",
    "2. Extend research: Replace mock Pandas UDFs with real MNE + YASA on Sleep-EDF Expanded",
    "3. TDA integration: Add Ripser + Giotto-TDA for persistent homology on EEG epochs",
    "4. Second domain: Add Allianz insurance claims or Deutsche Bank transaction scenario",
    "5. Deploy to Azure: Connect ADLS Gen2 + Unity Catalog in real workspace",
    "6. Apply to senior DE roles in Germany with this repo as portfolio evidence",
]

print("NEXT STEPS:")
for step in NEXT_STEPS:
    print(f"  {step}")

print()
print("=" * 70)
print("CONGRATULATIONS — 14-DAY PROGRAM COMPLETE!")
print("=" * 70)
print("Repo: github.com/wang-yuhao/databricks-eeg-lakehouse-lab")
print("Commits: 25+  |  Files: 50+  |  Exam domains: ALL COVERED")
