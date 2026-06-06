# Performance Tuning Cheatsheet — Databricks Data Engineer 2026

> **Exam weight**: ~15% questions on performance optimization
> **Repo examples**: `notebooks/day12_performance.py`, `src/utils/config.py`

---

## 1. Delta Lake File Management

### OPTIMIZE
```sql
-- Compact small files into larger target file size (~1 GB)
OPTIMIZE eeg_lakehouse.gold.eeg_features;

-- ZORDER: co-locate rows with same subject_id + date on same files
-- Enables DATA SKIPPING: Spark skips files where min/max statistics exclude the filter
OPTIMIZE eeg_lakehouse.gold.eeg_features
  ZORDER BY (subject_id, recording_date);
```

**When to run OPTIMIZE:**
- After large batch writes (e.g., daily ingestion)
- When query scan times increase (too many small files)
- Schedule via Databricks Job (nightly)

**ZORDER rule of thumb:**
- Use on columns frequently in `WHERE` / `JOIN` conditions
- Max 2-4 columns (diminishing returns beyond that)
- ZORDER != partitioning; they work together

### VACUUM
```sql
-- Remove files no longer referenced by current/historical Delta versions
-- Default retention: 168 hours (7 days)
VACUUM eeg_lakehouse.gold.eeg_features RETAIN 168 HOURS;

-- DRY RUN first to preview files to delete
VACUUM eeg_lakehouse.gold.eeg_features RETAIN 168 HOURS DRY RUN;
```
**EXAM TRAP**: Running VACUUM with retention < 7 days breaks time travel. Requires:
```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
```

---

## 2. Partitioning Strategy

```python
# Good partition column: low-cardinality, commonly filtered
# EEG example: partition by recording_date
df.write.format("delta").partitionBy("recording_date").saveAsTable("...")

# BAD: partitioning by subject_id (197 subjects = too many small partitions)
# BAD: partitioning by channel (high cardinality + rarely filtered alone)
```

| Partition column | Good? | Reason |
|-----------------|-------|--------|
| `recording_date` | YES | Date-range filters are common |
| `session` | YES (low cardinality: 2-3 values) | Study night filter |
| `subject_id` | NO | 197 partitions = small files |
| `channel` | NO | 64 channels = too many partitions |
| `epoch_index` | NO | Thousands of values |

---

## 3. Adaptive Query Execution (AQE)

AQE dynamically re-optimizes queries at runtime based on actual data statistics.

```python
# Enable AQE (on by default in Databricks Runtime 7+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Auto-coalesce shuffle partitions (avoids too many tiny partitions post-shuffle)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Auto-convert sort-merge joins to broadcast joins when one side is small
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10m")
```

**AQE benefits for EEG pipeline:**
- Subject metadata table (197 rows) auto-broadcast when joining with events
- Spindle events table (skewed if one subject has many events) gets adaptive skew join

---

## 4. Broadcast Joins

```python
from pyspark.sql import functions as F

# Force broadcast join for small lookup table
# Rule: use when one side < spark.sql.autoBroadcastJoinThreshold (default: 10 MB)
df_subjects = spark.table("eeg_lakehouse.bronze.subject_metadata")  # 197 rows
df_events   = spark.table("eeg_lakehouse.silver.eeg_events")         # millions

# Explicit broadcast hint
df_joined = df_events.join(
    F.broadcast(df_subjects),
    on="subject_id",
    how="left"
)

# EXAM NOTE: broadcast joins avoid shuffle; entire small table copied to every executor
# Only safe when small table fits in executor memory
```

---

## 5. Shuffle Optimization

```python
# Default shuffle partitions = 200 (often too many for small data)
# Rule of thumb: ~128 MB per partition after shuffle
spark.conf.set("spark.sql.shuffle.partitions", "32")  # for small EEG datasets

# Partition the DataFrame explicitly before expensive operations
df_events = df_events.repartition(32, "subject_id")  # repartition by join key

# coalesce() reduces partitions WITHOUT shuffle (use after filtering)
df_filtered = df_events.filter(...).coalesce(4)
```

---

## 6. Caching

```python
# Cache frequently accessed DataFrames (only if fits in memory)
df_gold = spark.table("eeg_lakehouse.gold.eeg_features")
df_gold.cache()  # lazy - won't cache until first action
df_gold.count()  # trigger cache

# EXAM NOTE:
# .cache() = MEMORY_AND_DISK (spills to disk if needed)
# .persist(StorageLevel.MEMORY_ONLY) = fail if doesn't fit in memory
# Delta tables: disk I/O is often faster than cache for large tables

# Always unpersist when done
df_gold.unpersist()
```

---

## 7. Data Skipping

Delta Lake stores min/max statistics per column per file in the transaction log.
When you filter on a column, Spark reads the log and **skips files** where
the filter cannot possibly match.

```sql
-- This query will skip files where subject_id statistics exclude 'SC4001'
SELECT * FROM eeg_lakehouse.gold.eeg_features
WHERE subject_id = 'SC4001' AND recording_date = '2024-01-10';
```

**Maximizing data skipping:**
1. ZORDER BY most-filtered columns
2. Keep files reasonably sized (1 GB target)
3. Don't over-partition (more partitions = less data per file = worse statistics)

---

## 8. EEG Pipeline Performance Map

| Table | Size estimate | Strategy |
|-------|--------------|----------|
| `bronze.raw_eeg_files` | ~50 GB (197 subjects x 20 nights x EDF) | Partition by `recording_date`, ZORDER by `subject_id` |
| `silver.eeg_events` | ~2 GB (millions of events) | Partition by `recording_date`, ZORDER by `subject_id, event_type` |
| `gold.eeg_features` | ~10 MB (197 rows) | No partition needed; ZORDER by `subject_id` |
| `bronze.subject_metadata` | ~10 KB | Always broadcast in joins |

---

## 9. Photon Engine

- Databricks native vectorized query engine (C++)
- Enabled by default on Photon-enabled cluster types
- Fastest for: SQL queries, Delta scans, joins, aggregations
- **EXAM NOTE**: Photon accelerates Delta Lake reads/writes automatically; no code changes needed

---

## 10. Key Exam Questions

**Q: What is ZORDER and when should you use it?**
A: ZORDER co-locates rows with the same values in the specified columns within the same set of Delta files. Use on high-cardinality columns frequently used in `WHERE` filters. Run via `OPTIMIZE ... ZORDER BY`.

**Q: What is the difference between partitioning and ZORDER?**
A: Partitioning creates separate directories per partition value (coarse-grained, OS-level). ZORDER sorts data within existing files for fine-grained skipping.

**Q: When should you broadcast a join?**
A: When one side is smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). Use `F.broadcast()` for explicit control.

**Q: What does VACUUM do?**
A: Deletes files from Delta table storage that are no longer referenced by any version in the transaction log and are older than the retention threshold (default 7 days).

**Q: How do you prevent small file problems in Delta?**
A: Use `OPTIMIZE` to compact files. Enable `delta.autoOptimize.autoCompact` and `delta.autoOptimize.optimizeWrite` table properties for automatic compaction.
