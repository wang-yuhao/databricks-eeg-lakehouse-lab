# Day 06 – Gold Layer: Aggregations, Window Functions & Feature Engineering

## Overview
Today we build the **Gold layer** of the EEG lakehouse: aggregating Silver events into
subject-level and epoch-level features ready for ML. Key Databricks exam topics covered:
window functions, `groupBy`/`agg`, broadcast joins, and writing optimised Delta tables.

---

## Core Concepts

### 1. Medallion Architecture – Gold Layer
- **Purpose:** Aggregated, business-ready or ML-ready data; denormalised for performance.
- **Input:** Silver tables (clean events, epochs, metadata).
- **Output:** Wide feature tables, summary statistics, subject-night aggregates.
- **Key rule:** Gold never contains raw signals; only derived metrics.

### 2. `groupBy` + `agg`
```python
from pyspark.sql import functions as F

gold_subject = (
    silver_events
    .groupBy("subject_id", "night_id", "event_type")
    .agg(
        F.count("*").alias("event_count"),
        F.mean("duration_s").alias("mean_duration_s"),
        F.stddev("duration_s").alias("std_duration_s"),
        F.sum("duration_s").alias("total_duration_s"),
        F.max("peak_amp_uv").alias("max_amplitude_uv"),
    )
)
```
**Exam tip:** `agg()` accepts multiple column expressions; each must use an alias.

### 3. Window Functions
```python
from pyspark.sql.window import Window

# Rolling count of spindles in a 30-s epoch window
w = Window.partitionBy("subject_id", "night_id").orderBy("epoch_start_s").rangeBetween(-30, 0)

silver_epochs = silver_epochs.withColumn(
    "spindle_density_30s",
    F.count("spindle_flag").over(w)
)
```
- **`partitionBy`** – resets the window per group (like `groupBy` but keeps all rows).
- **`orderBy` + `rangeBetween`** – defines a sliding time range window.
- **`rowsBetween`** – use when ordering by row position, not value.

**Common window functions:**
| Function | Description |
|---|---|
| `rank()` | rank with gaps |
| `dense_rank()` | rank without gaps |
| `row_number()` | unique sequential row id |
| `lag(col, n)` | value n rows behind |
| `lead(col, n)` | value n rows ahead |
| `sum().over(w)` | running/cumulative sum |

### 4. Broadcast Joins
```python
from pyspark.sql.functions import broadcast

# metadata is small (<= ~10 MB) – broadcast to avoid shuffle
gold = silver_events.join(
    broadcast(subject_metadata),
    on="subject_id",
    how="left"
)
```
**When to broadcast:**
- One side of join is small (rule of thumb: < 10 MB, configurable via `spark.sql.autoBroadcastJoinThreshold`).
- Avoids expensive shuffle/sort-merge join.
- Databricks often auto-broadcasts; explicit hint guarantees it.

### 5. Writing Gold Delta Tables
```python
gold_subject.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("night_id") \
    .saveAsTable("eeg_catalog.gold.subject_sleep_features")
```
**Optimisation after write:**
```sql
OPTIMIZE eeg_catalog.gold.subject_sleep_features ZORDER BY (subject_id);
```

---

## EEG / Neuroscience Context

### Gold Features for Sleep Research
| Feature | Formula | Relevance |
|---|---|---|
| Spindle density | spindles / min NREM | Memory consolidation proxy |
| SO-spindle coupling rate | spindles within 1 s of SO peak | Hippocampal-neocortical transfer |
| Sleep efficiency | TST / TIB * 100 | Clinical sleep quality metric |
| Mean spindle frequency | Hz per event | Age-related decline marker |
| K-complex rate | KC / hr NREM | Arousal threshold indicator |

TST = Total Sleep Time, TIB = Time In Bed

---

## Exam-Style SQL Equivalent
```sql
-- Gold aggregation in SQL
SELECT
  subject_id,
  night_id,
  event_type,
  COUNT(*) AS event_count,
  AVG(duration_s) AS mean_duration_s,
  SUM(duration_s) AS total_duration_s,
  MAX(peak_amp_uv) AS max_amplitude_uv,
  -- Window: running total per subject
  SUM(COUNT(*)) OVER (PARTITION BY subject_id ORDER BY night_id) AS cumulative_events
FROM silver_events
GROUP BY subject_id, night_id, event_type;
```

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `src/gold/build_features.py` | `build_subject_features_udf`, broadcast join, OPTIMIZE |
| `notebooks/day06_gold_features.py` | End-to-end Gold build notebook |
| `tests/test_gold.py` | Aggregation correctness, schema checks |

---

## Self-Check Questions
1. What is the difference between `rangeBetween` and `rowsBetween`?
2. When does Spark choose a broadcast join automatically?
3. What does `ZORDER BY` optimise and how does it differ from `partitionBy`?
4. Why do we use `overwriteSchema=true` when evolving Gold tables?
5. What is spindle density and why is it a clinically useful metric?
6. How would you compute a 5-night rolling average of sleep efficiency per subject?

---

## Further Reading
- [Spark Window Functions](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [Delta Lake OPTIMIZE & ZORDER](https://docs.delta.io/latest/optimizations-oss.html)
- Dijk, D.J. (1995). EEG slow waves and sleep spindles: windows on the sleeping brain. *Behavioural Brain Research*.
