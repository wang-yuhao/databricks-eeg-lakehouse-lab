# Day 12 – Performance Tuning: Spark Optimization & Delta Lake Best Practices

## Overview
Performance tuning is a heavily tested area of the Databricks DE exam. Today covers
partitioning strategies, caching, query optimization, skew handling, and Delta Lake
optimisation commands.

---

## Core Concepts

### 1. Shuffle Partitions
```python
# Default is 200 – too high for small datasets
spark.conf.set("spark.sql.shuffle.partitions", "8")  # Match cluster cores

# AQE (Adaptive Query Execution) automatically optimises partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```
**Rule of thumb:** `shuffle.partitions` ≈ 2-4x the number of CPU cores.

### 2. Partitioning Strategy
```python
# Write partitioned by night_id (high cardinality = many small files)
gold_features.write \
    .format("delta") \
    .partitionBy("night_id") \  # Good: ~100 distinct values
    .save("/mnt/gold/features")

# Avoid over-partitioning (e.g., by epoch_id with millions of values)
# Use OPTIMIZE to compact small files instead
```

**Partition best practices:**
- Partition by columns used in WHERE clauses.
- Avoid > 1000 partitions for most tables.
- Use `ZORDER BY` for high-cardinality filter columns instead.

### 3. Delta Lake OPTIMIZE & ZORDER
```sql
-- Compact small files
OPTIMIZE eeg_catalog.silver.epochs;

-- Compact AND co-locate data by query pattern
OPTIMIZE eeg_catalog.silver.epochs
ZORDER BY (subject_id, epoch_start_s);

-- Remove old snapshots (vacuums files older than retention period)
VACUUM eeg_catalog.silver.epochs RETAIN 168 HOURS;  -- 7 days

-- View table history
DESCRIBE HISTORY eeg_catalog.silver.epochs;
```

### 4. Caching
```python
# Cache frequently reused DataFrames
silver_epochs.cache()
silver_epochs.count()  # Trigger materialisation

# Explicitly unpersist when done
silver_epochs.unpersist()

# Delta Cache (Databricks-specific disk cache)
# Enabled by default; caches decompressed data on local SSD
# Configure: spark.databricks.io.cache.enabled = true
```
**When to cache:**
- DataFrame is used multiple times in the same job.
- Cost of re-computation > cost of caching.
- Do NOT cache very large tables that won't fit in memory.

### 5. Skew Handling
```python
# Option 1: Salting for join skew
from pyspark.sql import functions as F
import random

SALT = 10
large_df = large_df.withColumn("salt", (F.rand() * SALT).cast("int"))
small_df = small_df.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT)])))

joined = large_df.join(small_df, ["subject_id", "salt"])

# Option 2: AQE skew join (automatic)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 6. Reading Performance
```python
# Predicate pushdown (Delta handles automatically)
df = spark.read.format("delta").load("/mnt/silver/epochs") \
    .filter(F.col("subject_id") == "S001")  # Pruned at file scan level

# Column pruning (select only needed columns)
df = spark.read.format("delta").load("/mnt/silver/epochs") \
    .select("subject_id", "epoch_id", "spindle_count")  # Only reads 3 columns
```

---

## EEG / Neuroscience Context

### Performance Challenges in EEG Data
- **File size:** Raw EEG recordings can be 100s of MB per subject night; need efficient file layout.
- **High-frequency data:** 256 Hz * 30 s * 64 channels = ~500K values per epoch; shuffle is expensive.
- **Analysis patterns:** Most queries filter by `subject_id` or `night_id`; ZORDER by these columns dramatically speeds up scans.
- **Delta caching:** Repeatedly querying spectral power for the same cohort benefits from Delta cache.

### Recommended Table Layout
```
silver_epochs
  partitionBy: night_id  (low cardinality: 1-30 per subject)
  ZORDER BY:   subject_id, epoch_start_s

gold_features
  partitionBy: night_id
  ZORDER BY:   subject_id
  OPTIMIZE:    run after each batch ingestion
```

---

## Exam-Focused Summary

| Topic | Key Fact |
|---|---|
| `shuffle.partitions` | Set to ~2-4x cores; AQE can auto-coalesce |
| `OPTIMIZE` | Compacts small files into larger Parquet files |
| `ZORDER BY` | Co-locates related data; improves range/equality scans |
| `VACUUM` | Removes files outside retention window; irreversible |
| Broadcast join | Auto when table < `autoBroadcastJoinThreshold` (default 10MB) |
| AQE | Adaptive query execution; handles skew, coalesces partitions |

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `notebooks/day12_performance.py` | OPTIMIZE, ZORDER, caching demo notebook |
| `src/utils/optimization.py` | Helper functions for OPTIMIZE and VACUUM |
| `tests/test_performance.py` | Query plan validation tests |

---

## Self-Check Questions
1. What is the difference between `OPTIMIZE` and `ZORDER`?
2. When should you NOT use `.cache()`?
3. How does AQE help with data skew?
4. What is the risk of running `VACUUM` with a very short retention period?
5. Why does `partitionBy("epoch_id")` perform poorly for EEG data?
6. How does column pruning improve read performance in Delta Lake?

---

## Further Reading
- [Delta Lake Optimization](https://docs.delta.io/latest/optimizations-oss.html)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [Databricks Performance Tuning Guide](https://docs.databricks.com/en/optimizations/index.html)
