# Day 13 – Exam Mini-Labs: Databricks DE Associate Practice Questions

## Overview
Final preparation day with hands-on mini-labs covering the most heavily tested
Databricks Data Engineer Associate exam topics. Each lab is a self-contained
scenario with solution and exam-tip annotation.

---

## Mini-Lab 1: Delta Lake Time Travel

**Scenario:** A batch job accidentally overwrote the `silver.epochs` table with
incorrect artefact flags. Restore the table to the last known good state.

```sql
-- Check table history
DESCRIBE HISTORY eeg_catalog.silver.epochs;

-- Restore to version before the bad write (e.g., version 5)
RESTORE TABLE eeg_catalog.silver.epochs TO VERSION AS OF 5;

-- Or restore to a specific timestamp
RESTORE TABLE eeg_catalog.silver.epochs
TO TIMESTAMP AS OF '2024-01-15 08:00:00';

-- Verify the restore
SELECT COUNT(*) FROM eeg_catalog.silver.epochs;
```

**Exam tip:** `RESTORE` requires Delta Lake; it creates a new version in the history.
The table cannot be restored to a version that has been vacuumed.

---

## Mini-Lab 2: Schema Evolution with Auto Merge

**Scenario:** A new EEG device adds a `channel_impedance` column. The Bronze
ingestion should accept the new column without breaking.

```python
# Enable schema evolution on write
new_data.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("eeg_catalog.bronze.raw_signals")

# For streaming: set schema evolution config
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

**Exam tip:** `overwriteSchema` drops all existing columns and replaces them.
`mergeSchema` adds new columns while preserving existing ones.

---

## Mini-Lab 3: Streaming with Checkpoints

**Scenario:** A streaming job failed mid-run. Restart it from where it left off.

```python
# The checkpoint location stores offsets – reuse it on restart
query = (
    silver_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/silver_epochs")  # Must be same path
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("eeg_catalog.silver.epochs")
)
# Spark reads checkpoint and resumes from last committed offset
```

**Exam tip:** Never change the checkpoint location for an existing stream. If you
need to change schema or logic, you must delete the checkpoint and re-process.

---

## Mini-Lab 4: DLT Data Quality Report

**Scenario:** After a DLT pipeline run, analyse how many rows were dropped by each expectation.

```sql
-- Query DLT event log for quality metrics
SELECT
  details:flow_progress:data_quality:dropped_records AS dropped,
  details:flow_progress:data_quality:expectations AS expectations,
  timestamp
FROM delta.`/pipelines/<pipeline_id>/system/events`
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 20;
```

**Exam tip:** DLT event logs are stored in the pipeline storage location and
can be queried as Delta tables.

---

## Mini-Lab 5: Unity Catalog – Restrict Access

**Scenario:** A new intern should be able to query `gold.subject_features` but
not see the `subject_id` column.

```sql
-- Step 1: Create masking function
CREATE OR REPLACE FUNCTION eeg_catalog.masks.mask_subject(
    subject_id STRING
) RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('researchers') THEN subject_id
    ELSE SHA2(subject_id, 256)  -- Return hash instead of real ID
END;

-- Step 2: Apply mask to column
ALTER TABLE eeg_catalog.gold.subject_features
    ALTER COLUMN subject_id
    SET MASK eeg_catalog.masks.mask_subject;

-- Step 3: Grant SELECT to intern group
GRANT SELECT ON TABLE eeg_catalog.gold.subject_features
    TO `interns`;
```

---

## Mini-Lab 6: MLflow – Compare Experiment Runs

**Scenario:** You ran 5 experiments. Find the best model by F1 score and promote it.

```python
import mlflow

client = mlflow.tracking.MlflowClient()
experiment = client.get_experiment_by_name("/eeg-sleep-staging")

# Get all runs sorted by F1
runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["metrics.f1_macro DESC"],
    max_results=1
)
best_run = runs[0]
print(f"Best run: {best_run.info.run_id}, F1: {best_run.data.metrics['f1_macro']}")

# Register and promote
model_uri = f"runs:/{best_run.info.run_id}/model"
result = mlflow.register_model(model_uri, "eeg-sleep-stage-classifier")
client.transition_model_version_stage(
    name="eeg-sleep-stage-classifier",
    version=result.version,
    stage="Production"
)
```

---

## Exam Quick Reference Card

### Delta Lake Commands
| Command | Purpose |
|---|---|
| `DESCRIBE HISTORY` | View table version history |
| `RESTORE TABLE ... TO VERSION AS OF N` | Time travel restore |
| `OPTIMIZE ... ZORDER BY` | Compact + co-locate |
| `VACUUM ... RETAIN N HOURS` | Delete old files |
| `MERGE INTO` | Upsert / CDC |

### Streaming
| Topic | Answer |
|---|---|
| Restart after failure | Reuse same checkpoint location |
| Process new data only | `trigger(availableNow=True)` |
| Late data handling | `.withWatermark(col, delay)` |
| MERGE in stream | `foreachBatch` |

### Unity Catalog
| Topic | Answer |
|---|---|
| 3-level namespace | `catalog.schema.table` |
| Column security | `SET MASK` function |
| Row security | `SET ROW FILTER` function |
| Grant syntax | `GRANT privilege ON object TO principal` |

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `notebooks/day13_exam_mini_labs.py` | All 6 mini-labs as runnable notebook |
| `tests/test_exam_patterns.py` | Automated verification of exam patterns |

---

## Self-Check Questions
1. What happens if you `VACUUM` a table before restoring to a previous version?
2. What is the difference between `mergeSchema` and `overwriteSchema`?
3. Why must you never change the checkpoint location for a running stream?
4. Where are DLT event logs stored and how do you query them?
5. What does `SHA2(subject_id, 256)` return and why use it for masking?
6. How do you find the best MLflow run by a specific metric programmatically?
