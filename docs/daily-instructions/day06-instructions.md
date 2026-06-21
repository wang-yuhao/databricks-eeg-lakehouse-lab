# Day 6: Gold Layer — Feature Engineering and Aggregations for TDA

**Notebook**: `notebooks/day06_gold_features.py`
**Source modules**: `src/gold/build_features.py`
**Exam domains**: Spark SQL & Python (Domain 2), Data pipelines (Domain 1)
**Time estimate**: 3–4 hours
**Prerequisite**: Day 5 completed, Silver table optimized

---

## Objectives

- Build Gold layer aggregated features per subject
- Compute sleep stage distributions (% N1, N2, N3, REM, Wake)
- Calculate band power statistics (mean, std, percentiles)
- Create time-series features (sleep fragmentation, transitions)
- Write Gold table for downstream TDA (Topological Data Analysis)
- Validate data quality with assertions

---

## Background

**Why Gold layer?**

The Gold layer aggregates Silver data into **analysis-ready features**. In this project:

- **Silver** = EEG epochs (30-second windows) → millions of rows
- **Gold** = Per-subject aggregates (1 row per subject) → hundreds of rows

Gold features will be used for:

1. **Machine Learning** (predict sleep disorders)
2. **Topological Data Analysis** (TDA) to detect sleep patterns
3. **Dashboards and reporting**

**Key aggregations:**

| Feature type | Example | SQL function |
|---|---|---|
| Sleep stage distribution | % time in N3 (deep sleep) | `count(*) filter(where sleep_stage = 'N3') / count(*)` |
| Band power statistics | Mean delta power | `avg(delta_power)` |
| Artifact rate | % of epochs with artifacts | `count(*) filter(where is_artifact = true) / count(*)` |
| Sleep fragmentation | Number of stage transitions | `count(distinct epoch_idx) - 1` where stage changes |
| Percentiles | 75th percentile of sigma power | `percentile_approx(sigma_power, 0.75)` |

**Exam tip:**

- Use `F.expr("count(*) filter(where ...)")` for filtered aggregations (Spark 3.0+)
- Use `F.percentile_approx()` for large datasets (exact percentile is slow)

---

## Step-by-Step Instructions

### Step 1 — Open the notebook

1. Go to Databricks Workspace
2. Open `notebooks/day06_gold_features.py`
3. Attach to your cluster

---

### Step 2 — Run Cell 1: Load Silver table

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Load Silver table
silver_df = spark.read.table("eeg_lakehouse.silver.cleaned_epochs")

print(f"Silver table record count: {silver_df.count()}")
silver_df.printSchema()
```

**Expected output:**

```
Silver table record count: 15000
root
 |-- subject_id: string
 |-- epoch_idx: integer
 |-- sleep_stage: string
 |-- sigma_power: double
 |-- delta_power: double
 |-- is_artifact: boolean
 |-- signal_blob: binary
```

---

### Step 3 — Run Cell 2: Compute sleep stage distribution

```python
# Aggregate sleep stage counts per subject
sleep_dist = (
    silver_df
    .groupBy("subject_id")
    .agg(
        F.count("*").alias("total_epochs"),
        F.expr("count(*) filter(where sleep_stage = 'Wake')").alias("wake_count"),
        F.expr("count(*) filter(where sleep_stage = 'N1')").alias("n1_count"),
        F.expr("count(*) filter(where sleep_stage = 'N2')").alias("n2_count"),
        F.expr("count(*) filter(where sleep_stage = 'N3')").alias("n3_count"),
        F.expr("count(*) filter(where sleep_stage = 'REM')").alias("rem_count"),
    )
    # Compute percentages
    .withColumn("wake_pct", F.col("wake_count") / F.col("total_epochs") * 100)
    .withColumn("n1_pct", F.col("n1_count") / F.col("total_epochs") * 100)
    .withColumn("n2_pct", F.col("n2_count") / F.col("total_epochs") * 100)
    .withColumn("n3_pct", F.col("n3_count") / F.col("total_epochs") * 100)
    .withColumn("rem_pct", F.col("rem_count") / F.col("total_epochs") * 100)
)

sleep_dist.show(5)
```

**Expected output:**

```
+----------+------------+----------+--------+--------+--------+---------+--------+------+------+------+-------+
|subject_id|total_epochs|wake_count|n1_count|n2_count|n3_count|rem_count|wake_pct|n1_pct|n2_pct|n3_pct|rem_pct|
+----------+------------+----------+--------+--------+--------+---------+--------+------+------+------+-------+
|001       |100         |10        |15      |40      |20      |15       |10.0    |15.0  |40.0  |20.0  |15.0   |
|002       |100         |8         |12      |45      |18      |17       |8.0     |12.0  |45.0  |18.0  |17.0   |
...
```

**Interpretation:**

- Subject 001 spends 20% of time in N3 (deep sleep) → healthy
- Wake % > 20% → possible insomnia

---

### Step 4 — Run Cell 3: Compute band power statistics

```python
# Aggregate band power features per subject
band_power_stats = (
    silver_df
    .filter(F.col("is_artifact") == False)  # Exclude artifacts
    .groupBy("subject_id")
    .agg(
        F.mean("sigma_power").alias("sigma_mean"),
        F.stddev("sigma_power").alias("sigma_std"),
        F.expr("percentile_approx(sigma_power, 0.25)").alias("sigma_p25"),
        F.expr("percentile_approx(sigma_power, 0.50)").alias("sigma_p50"),
        F.expr("percentile_approx(sigma_power, 0.75)").alias("sigma_p75"),
        F.mean("delta_power").alias("delta_mean"),
        F.stddev("delta_power").alias("delta_std"),
        F.expr("percentile_approx(delta_power, 0.25)").alias("delta_p25"),
        F.expr("percentile_approx(delta_power, 0.50)").alias("delta_p50"),
        F.expr("percentile_approx(delta_power, 0.75)").alias("delta_p75"),
    )
)

band_power_stats.show(5)
```

**Expected output:**

```
+----------+----------+---------+--------+--------+--------+----------+---------+--------+--------+--------+
|subject_id|sigma_mean|sigma_std|sigma_p25|sigma_p50|sigma_p75|delta_mean|delta_std|delta_p25|delta_p50|delta_p75|
+----------+----------+---------+--------+--------+--------+----------+---------+--------+--------+--------+
|001       |4.2       |2.1      |2.8     |3.9     |5.1     |12.5      |6.3      |8.2     |11.0    |15.3    |
|002       |3.8       |1.9      |2.5     |3.6     |4.8     |11.8      |5.9      |7.8     |10.5    |14.1    |
...
```

**Why exclude artifacts?**

- Artifacts skew mean/std calculations
- Filter `is_artifact = False` before aggregation

---

### Step 5 — Run Cell 4: Compute artifact rate

```python
# Artifact rate per subject
artifact_rate = (
    silver_df
    .groupBy("subject_id")
    .agg(
        F.expr("count(*) filter(where is_artifact = true)").alias("artifact_count"),
        F.count("*").alias("total_epochs"),
    )
    .withColumn("artifact_rate", F.col("artifact_count") / F.col("total_epochs"))
)

artifact_rate.show(5)
```

**Expected output:**

```
+----------+--------------+------------+-------------+
|subject_id|artifact_count|total_epochs|artifact_rate|
+----------+--------------+------------+-------------+
|001       |5             |100         |0.05         |
|002       |8             |100         |0.08         |
...
```

**Quality check:**

- Artifact rate > 20% → poor data quality, exclude subject

---

### Step 6 — Run Cell 5: Compute sleep fragmentation

```python
from pyspark.sql.window import Window

# Add lag column to detect stage transitions
window_spec = Window.partitionBy("subject_id").orderBy("epoch_idx")

transitions_df = (
    silver_df
    .withColumn("prev_stage", F.lag("sleep_stage").over(window_spec))
    .withColumn("is_transition", F.col("sleep_stage") != F.col("prev_stage"))
)

# Count transitions per subject
fragmentation = (
    transitions_df
    .filter(F.col("is_transition") == True)
    .groupBy("subject_id")
    .agg(
        F.count("*").alias("transition_count"),
    )
)

fragmentation.show(5)
```

**Expected output:**

```
+----------+----------------+
|subject_id|transition_count|
+----------+----------------+
|001       |15              |
|002       |20              |
...
```

**Interpretation:**

- High transition count → fragmented sleep (possible sleep disorder)
- Low transition count → stable sleep stages

---

### Step 7 — Run Cell 6: Join all features into Gold table

```python
# Join all aggregated features
gold_df = (
    sleep_dist
    .join(band_power_stats, on="subject_id", how="inner")
    .join(artifact_rate.select("subject_id", "artifact_rate"), on="subject_id", how="inner")
    .join(fragmentation, on="subject_id", how="inner")
    # Drop intermediate count columns
    .drop("wake_count", "n1_count", "n2_count", "n3_count", "rem_count", "artifact_count")
)

print(f"Gold table record count: {gold_df.count()}")
gold_df.printSchema()
gold_df.show(5, truncate=False)
```

**Expected output:**

```
Gold table record count: 50
root
 |-- subject_id: string
 |-- total_epochs: long
 |-- wake_pct: double
 |-- n1_pct: double
 |-- n2_pct: double
 |-- n3_pct: double
 |-- rem_pct: double
 |-- sigma_mean: double
 |-- sigma_std: double
 |-- sigma_p25: double
 |-- sigma_p50: double
 |-- sigma_p75: double
 |-- delta_mean: double
 |-- delta_std: double
 |-- delta_p25: double
 |-- delta_p50: double
 |-- delta_p75: double
 |-- artifact_rate: double
 |-- transition_count: long
```

---

### Step 8 — Run Cell 7: Write Gold table to Delta

```python
# Write Gold table
gold_path = "dbfs:/eeg_lakehouse/gold/subject_features"

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

# Register as table
spark.sql("CREATE DATABASE IF NOT EXISTS eeg_lakehouse")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS eeg_lakehouse.gold.subject_features
    USING DELTA
    LOCATION '{gold_path}'
""")

print("✓ Gold table written")
```

**Expected output:**

```
✓ Gold table written
```

---

### Step 9 — Run Cell 8: Data quality assertions

```python
# Assertion 1: All percentages sum to ~100%
for row in gold_df.select("subject_id", "wake_pct", "n1_pct", "n2_pct", "n3_pct", "rem_pct").collect():
    total_pct = row.wake_pct + row.n1_pct + row.n2_pct + row.n3_pct + row.rem_pct
    assert 99 < total_pct < 101, f"Subject {row.subject_id} stage percentages don't sum to 100%: {total_pct}"

print("✓ PASS: Stage percentages sum to 100%")

# Assertion 2: Artifact rate < 20%
max_artifact_rate = gold_df.agg(F.max("artifact_rate")).collect()[0][0]
assert max_artifact_rate < 0.20, f"FAIL: Max artifact rate {max_artifact_rate:.1%} exceeds 20%"

print("✓ PASS: Artifact rate acceptable")

# Assertion 3: No null values in key features
null_counts = gold_df.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in gold_df.columns]
).collect()[0].asDict()

for col, null_count in null_counts.items():
    assert null_count == 0, f"FAIL: Column {col} has {null_count} null values"

print("✓ PASS: No null values in Gold table")
```

**Expected output:**

```
✓ PASS: Stage percentages sum to 100%
✓ PASS: Artifact rate acceptable
✓ PASS: No null values in Gold table
```

---

### Step 10 — Run Cell 9: Descriptive statistics

```python
# Show summary statistics
gold_df.select(
    "wake_pct", "n3_pct", "sigma_mean", "delta_mean", "artifact_rate", "transition_count"
).describe().show()
```

**Expected output:**

```
+-------+--------+-------+----------+----------+-------------+----------------+
|summary|wake_pct|n3_pct |sigma_mean|delta_mean|artifact_rate|transition_count|
+-------+--------+-------+----------+----------+-------------+----------------+
|count  |50      |50     |50        |50        |50           |50              |
|mean   |12.5    |18.3   |4.1       |12.2      |0.08         |17.5            |
|stddev |5.2     |6.1    |1.2       |3.4       |0.04         |5.2             |
|min    |5.0     |8.0    |2.1       |7.5       |0.02         |8               |
|max    |25.0    |30.0   |7.2       |20.5      |0.18         |32              |
+-------+--------+-------+----------+----------+-------------+----------------+
```

---

### Step 11 — Run Cell 10: Visualize distributions (optional)

```python
# Create Pandas DataFrame for plotting
import matplotlib.pyplot as plt
import seaborn as sns

gold_pd = gold_df.toPandas()

fig, axes = plt.subplots(2, 3, figsize=(15, 10))

# Sleep stage distributions
axes[0, 0].hist(gold_pd["wake_pct"], bins=20, color="orange")
axes[0, 0].set_title("Wake %")
axes[0, 0].set_xlabel("Percentage")

axes[0, 1].hist(gold_pd["n3_pct"], bins=20, color="blue")
axes[0, 1].set_title("N3 (Deep Sleep) %")
axes[0, 1].set_xlabel("Percentage")

axes[0, 2].hist(gold_pd["rem_pct"], bins=20, color="purple")
axes[0, 2].set_title("REM %")
axes[0, 2].set_xlabel("Percentage")

# Band power
axes[1, 0].hist(gold_pd["sigma_mean"], bins=20, color="green")
axes[1, 0].set_title("Mean Sigma Power")
axes[1, 0].set_xlabel("Power (µV²)")

axes[1, 1].hist(gold_pd["delta_mean"], bins=20, color="red")
axes[1, 1].set_title("Mean Delta Power")
axes[1, 1].set_xlabel("Power (µV²)")

# Sleep fragmentation
axes[1, 2].hist(gold_pd["transition_count"], bins=20, color="gray")
axes[1, 2].set_title("Sleep Stage Transitions")
axes[1, 2].set_xlabel("Count")

plt.tight_layout()
plt.show()
```

---

## Self-Check: Answer Exam Reflection Questions

1. Why use `F.expr("count(*) filter(where ...)")` instead of two separate `groupBy()` calls?
2. What is the difference between `percentile()` and `percentile_approx()`?
3. Why exclude artifacts before computing band power statistics?
4. How do you detect sleep stage transitions using window functions?
5. What are the key quality checks for the Gold table?
6. Why does the Gold table have far fewer rows than the Silver table?

**Answers:**

1. `filter(where ...)` computes multiple conditional aggregations in a single pass over the data. Separate `groupBy()` calls would scan the data multiple times.
2. `percentile()` is exact but slow (requires sorting). `percentile_approx()` uses sketching algorithms (t-digest) and is ~10x faster for large datasets with <1% error.
3. Artifacts are outliers that skew mean and standard deviation. Excluding them gives more accurate statistics for normal EEG signal.
4. Use `lag()` window function to get previous stage, then compare `sleep_stage != prev_stage` to detect transitions.
5. (1) Stage percentages sum to 100%, (2) Artifact rate < 20%, (3) No null values in key features, (4) N3% and REM% are reasonable (10-25%).
6. Gold aggregates per subject (1 row per subject). Silver has per-epoch data (100s of epochs per subject).

---

## Day 6 Summary

| What was built | Feature type | SQL function |
|---|---|---|
| Sleep stage distribution | Wake%, N1%, N2%, N3%, REM% | `count(*) filter(where ...)` |
| Band power statistics | Mean, std, percentiles (sigma, delta) | `avg()`, `stddev()`, `percentile_approx()` |
| Artifact rate | % of epochs with artifacts | `count(*) filter(where is_artifact = true)` |
| Sleep fragmentation | Number of stage transitions | `lag()` window function + `count(*)` |
| Gold Delta table | Per-subject aggregates | `write.format("delta").mode("overwrite")` |
| Data quality assertions | Null checks, percentage sums | Python `assert` statements |

**Next**: Day 7 explores Delta Live Tables (DLT) for orchestrating the Bronze → Silver → Gold pipeline with auto-recovery and data quality constraints.
