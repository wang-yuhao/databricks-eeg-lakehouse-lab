# Databricks notebook source
# Day 12: Performance Tuning — AQE, Broadcast Joins, Partition Strategies
# =========================================================================
# Exam domains covered:
#   - Adaptive Query Execution (AQE): coalescing partitions, skew joins
#   - Broadcast joins: when/how to use, threshold config
#   - OPTIMIZE and ZORDER for Delta table layout
#   - Caching strategies: cache(), persist(), CACHE TABLE
#   - Partition pruning and predicate pushdown
#
# Research context:
#   EEG feature tables for N=200 subjects are small enough for broadcast
#   joins when joined against lookup tables (subject demographics, site info).
#   ZORDER on subject_id + epoch_start_ts dramatically speeds up per-subject
#   feature extraction queries in the Gold layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE automatically re-optimises query plans at runtime using actual data statistics.
# MAGIC Enabled by default in Databricks Runtime 7.3+.

# COMMAND ----------

# Check AQE status
print("AQE configuration:")
print(f"  spark.sql.adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled', 'true (default)')}")
print(f"  spark.sql.adaptive.coalescePartitions.enabled: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled', 'true (default)')}")
print(f"  spark.sql.adaptive.skewJoin.enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled', 'true (default)')}")

# Explicitly enable for this session (already on by default in DBR)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Broadcast Joins — EEG Subject Demographics

# COMMAND ----------

from pyspark.sql import functions as F

# Simulate EEG epoch data (large table)
epochs_df = spark.table("eeg_catalog.silver.eeg_epochs")

# Simulate small lookup table (demographics — suitable for broadcast)
demographics_df = spark.table("eeg_catalog.bronze.subject_demographics")

print(f"Epochs count: {epochs_df.count():,}")
print(f"Demographics count: {demographics_df.count():,}")

# COMMAND ----------

# Check current broadcast threshold
threshold_mb = int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")) / (1024 * 1024)
print(f"Current autoBroadcastJoinThreshold: {threshold_mb:.0f} MB")

# Explicit broadcast hint — overrides threshold
result_df = epochs_df.join(
    F.broadcast(demographics_df),  # force broadcast of small dimension table
    on="subject_id",
    how="left",
)

print("Join plan (broadcast hint applied):")
result_df.explain(mode="formatted")

# COMMAND ----------

# Increase threshold for this session (default: 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # 50 MB
print("Broadcast threshold raised to 50 MB")

# MAGIC Exam tip: Set to -1 to DISABLE broadcast joins entirely (useful for debugging)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. OPTIMIZE and ZORDER — Delta Table Layout

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE compacts small files into larger ones (target: 1GB per file)
# MAGIC OPTIMIZE eeg_catalog.silver.eeg_epochs;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ZORDER co-locates related data in the same files for column skipping
# MAGIC -- Best columns: high-cardinality, frequently filtered
# MAGIC OPTIMIZE eeg_catalog.silver.eeg_epochs
# MAGIC ZORDER BY (subject_id, epoch_start_ts);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM removes files no longer referenced by Delta log
# MAGIC -- Default retention: 7 days (168 hours)
# MAGIC VACUUM eeg_catalog.silver.eeg_epochs RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check optimization history
# MAGIC DESCRIBE HISTORY eeg_catalog.silver.eeg_epochs LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Caching Strategies

# COMMAND ----------

# DataFrame cache (memory + disk, serialized)
gold_df = spark.table("eeg_catalog.gold.subject_features")
gold_df.cache()
gold_df.count()  # trigger cache materialisation
print("Gold features cached in memory")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL-level cache (persists across notebook sessions)
# MAGIC CACHE TABLE eeg_catalog.gold.subject_features;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncache when done to release memory
# MAGIC UNCACHE TABLE eeg_catalog.gold.subject_features;

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

# Persist with specific storage level
gold_df.persist(StorageLevel.MEMORY_AND_DISK)
gold_df.count()

# Check what's cached
for rdd_info in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(f"  Cached: {rdd_info.name()}")

gold_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Partition Strategies

# COMMAND ----------

# Check current shuffle partitions
print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# For small EEG dataset (N=200 subjects), reduce shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "8")
print("Reduced shuffle partitions to 8 (appropriate for small EEG dataset)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file statistics after OPTIMIZE
# MAGIC SELECT
# MAGIC   COUNT(*) AS num_files,
# MAGIC   SUM(size) / 1e6 AS total_size_mb,
# MAGIC   AVG(size) / 1e6 AS avg_file_size_mb,
# MAGIC   MIN(size) / 1e6 AS min_file_size_mb,
# MAGIC   MAX(size) / 1e6 AS max_file_size_mb
# MAGIC FROM (
# MAGIC   DESCRIBE DETAIL eeg_catalog.silver.eeg_epochs
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Skew Detection and Mitigation

# COMMAND ----------

# Check data distribution across subjects (look for skew)
skew_check = (
    spark.table("eeg_catalog.silver.eeg_epochs")
    .groupBy("subject_id")
    .count()
    .orderBy(F.col("count").desc())
)

print("Top 10 subjects by epoch count (check for skew):")
skew_check.show(10)

# COMMAND ----------

# Salting technique for skewed joins (if one subject_id dominates)
# Add random salt to spread records across partitions
SALT_FACTOR = 5

skewed_df = spark.table("eeg_catalog.silver.eeg_epochs")
salted_df = skewed_df.withColumn(
    "salted_subject_id",
    F.concat(F.col("subject_id"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int").cast("string")),
)
print(f"Salting applied with factor {SALT_FACTOR}")
print("Use salted_subject_id for the join key when subject data is skewed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Tuning Exam Quick Reference
# MAGIC
# MAGIC | Issue | Solution | Config |
# MAGIC |-------|----------|--------|
# MAGIC | Too many small files | `OPTIMIZE` | — |
# MAGIC | Slow range scans | `ZORDER BY (col)` | — |
# MAGIC | Skewed shuffles | AQE skew join | `spark.sql.adaptive.skewJoin.enabled=true` |
# MAGIC | Too many partitions after filter | AQE coalesce | `spark.sql.adaptive.coalescePartitions.enabled=true` |
# MAGIC | Small dimension table join slow | Broadcast join | `spark.sql.autoBroadcastJoinThreshold` |
# MAGIC | Repeated scans of same table | Cache/Persist | `df.cache()` or `CACHE TABLE` |
# MAGIC | Shuffle bottleneck | Tune partitions | `spark.sql.shuffle.partitions` |
# MAGIC | Stale cached plan | Clear cache | `UNCACHE TABLE` or `df.unpersist()` |
