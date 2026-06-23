# Day 6: Gold Layer — Feature Engineering and Aggregations

**Notebook**: `notebooks/day06_gold_features.py`
**Source modules**: `src/gold/build_features.py`
**Exam domains**: Spark SQL & Python (Domain 2), Data Pipelines (Domain 1)
**Time estimate**: 3–4 hours
**Prerequisite**: Day 5 completed; Silver table `eeg_lakehouse.silver.cleaned_epochs` is optimised and accessible

---

## Environment Setup

Complete every sub-section below before executing any notebook cell.

### 1. Create a GitHub Personal Access Token (PAT)

1. Navigate to [https://github.com/settings/tokens](https://github.com/settings/tokens) and sign in.
2. Click **Generate new token (classic)**.
3. Set **Note** to `databricks-eeg-lab`.
4. Set **Expiration** to `90 days`.
5. Select the following scopes: `repo` (full), `workflow`.
6. Click **Generate token** and copy the value immediately.

### 2. Configure Databricks Git Integration

1. In the Databricks workspace, click your username in the top-right corner and select **User Settings**.
2. Click the **Git Integration** tab.
3. Set **Git provider** to `GitHub`.
4. Paste the PAT into the **Token** field and enter your GitHub username.
5. Click **Save**.

### 3. Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos**.
2. Click **Add Repo**.
3. Enter the URL: `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git`.
4. Leave **Branch** as `main` and click **Create Repo**.

### 4. Create a Unity Catalog–Enabled Cluster

1. In the left sidebar, click **Compute** > **Create compute**.
2. Apply the following configuration.

| Parameter | Value |
|---|---|
| Cluster name | `eeg-lab-cluster` |
| Cluster mode | Single node |
| Databricks Runtime | **14.3 LTS** (Scala 2.12, Spark 3.5) |
| Node type | `Standard_DS3_v2` (Azure) or equivalent |
| Terminate after | 60 minutes of inactivity |
| Unity Catalog | Enabled — set **Access mode** to **Single user** |

3. Under **Advanced options** > **Spark**, add:

```
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
```

4. Click **Create compute** and wait for the status to show **Running**.

### 5. Install Required Libraries

Select the cluster, open the **Libraries** tab, click **Install new**, and add the following.

| Source | Package |
|---|---|
| PyPI | `mne==1.7.0` |
| PyPI | `scipy==1.13.0` |
| PyPI | `numpy==1.26.4` |
| PyPI | `matplotlib==3.9.0` |
| PyPI | `seaborn==0.13.2` |

Wait for each library to show status **Installed** before proceeding.

### 6. Open and Attach the Notebook

1. In the left sidebar, click **Repos** and navigate to `wang-yuhao/databricks-eeg-lakehouse-lab/notebooks/`.
2. Click `day06_gold_features.py` to open it.
3. In the notebook toolbar, click the cluster dropdown and select `eeg-lab-cluster`.
4. Confirm the cluster indicator turns green.

---

## Objectives

- Build a Gold layer of aggregated, per-subject features from Silver epoch data.
- Compute sleep stage distributions (percentage of time in Wake, N1, N2, N3, REM).
- Calculate band power statistics (mean, standard deviation, quartile percentiles).
- Measure artifact rate per subject.
- Quantify sleep fragmentation using window functions.
- Write the Gold Delta table and validate it with programmatic assertions.

---

## Background

The Gold layer collapses millions of epoch-level Silver rows into one analysis-ready row per subject. This compression supports both machine learning classification and exploratory reporting.

| Aggregation type | Example metric | Spark function |
|---|---|---|
| Sleep stage distribution | Percentage of epochs in N3 | `count(*) filter(where sleep_stage = 'N3') / count(*)` |
| Band power — central tendency | Mean delta power per subject | `avg(delta_power)` |
| Band power — spread | Standard deviation of sigma power | `stddev(sigma_power)` |
| Band power — percentiles | 75th percentile of sigma power | `percentile_approx(sigma_power, 0.75)` |
| Artifact rate | Fraction of epochs flagged as artefacts | `count(*) filter(where is_artifact = true) / count(*)` |
| Sleep fragmentation | Number of sleep stage transitions | `lag()` window + conditional count |

> **Exam tip**: Use `F.expr("count(*) filter(where ...)")` to perform multiple conditional aggregations in a single data pass. Use `percentile_approx()` instead of `percentile()` on large datasets — the approximation introduces less than 1 % error while being significantly faster.

---

## Step-by-Step Instructions

### Step 1 — Define constants and load Silver table

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SILVER_TABLE = "eeg_lakehouse.silver.cleaned_epochs"
GOLD_PATH    = "dbfs:/eeg_lakehouse/gold/subject_features"
GOLD_TABLE   = "eeg_lakehouse.gold.subject_features"

silver_df = spark.read.table(SILVER_TABLE)

print(f"Silver record count : {silver_df.count()}")
silver_df.printSchema()
```

**Expected output**:

```
Silver record count : 15000
root
 |-- subject_id: string (nullable = true)
 |-- epoch_idx: integer (nullable = true)
 |-- sleep_stage: string (nullable = true)
 |-- sigma_power: double (nullable = true)
 |-- delta_power: double (nullable = true)
 |-- is_artifact: boolean (nullable = true)
 |-- signal_blob: binary (nullable = true)
```

---

### Step 2 — Compute sleep stage distribution

```python
sleep_dist = (
    silver_df
    .groupBy("subject_id")
    .agg(
        F.count("*").alias("total_epochs"),
        F.expr("count(*) filter(where sleep_stage = 'Wake')").alias("wake_count"),
        F.expr("count(*) filter(where sleep_stage = 'N1')" ).alias("n1_count"),
        F.expr("count(*) filter(where sleep_stage = 'N2')" ).alias("n2_count"),
        F.expr("count(*) filter(where sleep_stage = 'N3')" ).alias("n3_count"),
        F.expr("count(*) filter(where sleep_stage = 'REM')").alias("rem_count"),
    )
    .withColumn("wake_pct", F.round(F.col("wake_count") / F.col("total_epochs") * 100, 2))
    .withColumn("n1_pct",   F.round(F.col("n1_count")   / F.col("total_epochs") * 100, 2))
    .withColumn("n2_pct",   F.round(F.col("n2_count")   / F.col("total_epochs") * 100, 2))
    .withColumn("n3_pct",   F.round(F.col("n3_count")   / F.col("total_epochs") * 100, 2))
    .withColumn("rem_pct",  F.round(F.col("rem_count")  / F.col("total_epochs") * 100, 2))
    .drop("wake_count", "n1_count", "n2_count", "n3_count", "rem_count")
)

sleep_dist.show(5, truncate=False)
```

**Expected output**:

```
+----------+------------+--------+------+------+------+-------+
|subject_id|total_epochs|wake_pct|n1_pct|n2_pct|n3_pct|rem_pct|
+----------+------------+--------+------+------+------+-------+
|001       |300         |10.0    |15.0  |40.0  |20.0  |15.0   |
|002       |300         |8.0     |12.0  |45.0  |18.0  |17.0   |
```

---

### Step 3 — Compute band power statistics

```python
band_power_stats = (
    silver_df
    .filter(F.col("is_artifact") == False)
    .groupBy("subject_id")
    .agg(
        F.round(F.mean("sigma_power"),   4).alias("sigma_mean"),
        F.round(F.stddev("sigma_power"), 4).alias("sigma_std"),
        F.round(F.expr("percentile_approx(sigma_power, 0.25)"), 4).alias("sigma_p25"),
        F.round(F.expr("percentile_approx(sigma_power, 0.50)"), 4).alias("sigma_p50"),
        F.round(F.expr("percentile_approx(sigma_power, 0.75)"), 4).alias("sigma_p75"),
        F.round(F.mean("delta_power"),   4).alias("delta_mean"),
        F.round(F.stddev("delta_power"), 4).alias("delta_std"),
        F.round(F.expr("percentile_approx(delta_power, 0.25)"), 4).alias("delta_p25"),
        F.round(F.expr("percentile_approx(delta_power, 0.50)"), 4).alias("delta_p50"),
        F.round(F.expr("percentile_approx(delta_power, 0.75)"), 4).alias("delta_p75"),
    )
)

band_power_stats.show(5, truncate=False)
```

Artifact epochs are excluded before aggregation because they contain corrupted signal amplitudes that inflate mean and standard deviation estimates.

---

### Step 4 — Compute artifact rate

```python
artifact_rate = (
    silver_df
    .groupBy("subject_id")
    .agg(
        F.count("*").alias("total_epochs"),
        F.expr("count(*) filter(where is_artifact = true)").alias("artifact_count"),
    )
    .withColumn(
        "artifact_rate",
        F.round(F.col("artifact_count") / F.col("total_epochs"), 4)
    )
    .select("subject_id", "artifact_rate")
)

artifact_rate.show(5, truncate=False)
```

**Interpretation**: A subject with `artifact_rate > 0.20` has poor signal quality and should be excluded from downstream analysis.

---

### Step 5 — Compute sleep fragmentation

```python
window_spec = Window.partitionBy("subject_id").orderBy("epoch_idx")

fragmentation = (
    silver_df
    .withColumn("prev_stage", F.lag("sleep_stage").over(window_spec))
    .withColumn(
        "is_transition",
        (F.col("sleep_stage") != F.col("prev_stage")) & F.col("prev_stage").isNotNull()
    )
    .groupBy("subject_id")
    .agg(
        F.sum(F.col("is_transition").cast("int")).alias("transition_count")
    )
)

fragmentation.show(5, truncate=False)
```

**Expected output**:

```
+----------+----------------+
|subject_id|transition_count|
+----------+----------------+
|001       |15              |
|002       |22              |
```

A high `transition_count` indicates fragmented sleep, which is associated with sleep disorders such as insomnia or sleep apnoea.

---

### Step 6 — Join all features into the Gold DataFrame

```python
gold_df = (
    sleep_dist
    .join(band_power_stats, on="subject_id", how="inner")
    .join(artifact_rate,    on="subject_id", how="inner")
    .join(fragmentation,    on="subject_id", how="inner")
)

print(f"Gold record count: {gold_df.count()}")
gold_df.printSchema()
gold_df.show(5, truncate=False)
```

**Expected output**:

```
Gold record count: 50
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

### Step 7 — Write the Gold Delta table

```python
(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_PATH)
)

spark.sql("CREATE CATALOG IF NOT EXISTS eeg_lakehouse")
spark.sql("CREATE DATABASE IF NOT EXISTS eeg_lakehouse.gold")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE}
    USING DELTA
    LOCATION '{GOLD_PATH}'
""")

print(f"Gold table written to {GOLD_PATH} and registered as {GOLD_TABLE}.")
```

---

### Step 8 — Run data quality assertions

```python
rows = gold_df.select(
    "subject_id", "wake_pct", "n1_pct", "n2_pct", "n3_pct", "rem_pct", "artifact_rate"
).collect()

# Assertion 1: Stage percentages must sum to approximately 100 %
for row in rows:
    total_pct = row.wake_pct + row.n1_pct + row.n2_pct + row.n3_pct + row.rem_pct
    assert 99.0 < total_pct < 101.0, (
        f"Subject {row.subject_id}: stage percentages sum to {total_pct:.2f}, expected ~100."
    )
print("PASS — Stage percentages sum to 100 % for all subjects.")

# Assertion 2: Artifact rate must be below the 20 % threshold
for row in rows:
    assert row.artifact_rate < 0.20, (
        f"Subject {row.subject_id}: artifact rate {row.artifact_rate:.1%} exceeds 20 % threshold."
    )
print("PASS — Artifact rate is within the acceptable range for all subjects.")

# Assertion 3: No null values in any Gold column
null_counts = gold_df.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in gold_df.columns]
).collect()[0].asDict()

for col_name, null_count in null_counts.items():
    assert null_count == 0, (
        f"Column '{col_name}' contains {null_count} null value(s)."
    )
print("PASS — No null values detected in the Gold table.")
```

**Expected output**:

```
PASS — Stage percentages sum to 100 % for all subjects.
PASS — Artifact rate is within the acceptable range for all subjects.
PASS — No null values detected in the Gold table.
```

---

### Step 9 — Summary statistics

```python
gold_df.select(
    "wake_pct", "n3_pct", "sigma_mean", "delta_mean", "artifact_rate", "transition_count"
).describe().show(truncate=False)
```

**Expected output**:

```
+-------+--------+------+----------+----------+-------------+----------------+
|summary|wake_pct|n3_pct|sigma_mean|delta_mean|artifact_rate|transition_count|
+-------+--------+------+----------+----------+-------------+----------------+
|count  |50      |50    |50        |50        |50           |50              |
|mean   |12.5    |18.3  |4.1       |12.2      |0.08         |17.5            |
|stddev |5.2     |6.1   |1.2       |3.4       |0.04         |5.2             |
|min    |5.0     |8.0   |2.1       |7.5       |0.02         |8               |
|max    |25.0    |30.0  |7.2       |20.5      |0.18         |32              |
+-------+--------+------+----------+----------+-------------+----------------+
```

---

### Step 10 — Visualise feature distributions

```python
import matplotlib.pyplot as plt
import seaborn as sns

gold_pd = gold_df.toPandas()

fig, axes = plt.subplots(2, 3, figsize=(16, 10))
fig.suptitle("Gold Layer — Subject Feature Distributions", fontsize=14)

plot_config = [
    ("wake_pct",        "Wake %",                "orange"),
    ("n3_pct",          "N3 (Deep Sleep) %",     "steelblue"),
    ("rem_pct",         "REM %",                  "mediumpurple"),
    ("sigma_mean",      "Mean Sigma Power (µV²)", "seagreen"),
    ("delta_mean",      "Mean Delta Power (µV²)", "tomato"),
    ("transition_count","Sleep Stage Transitions","slategray"),
]

for ax, (col, title, colour) in zip(axes.flat, plot_config):
    ax.hist(gold_pd[col], bins=20, color=colour, edgecolor="white")
    ax.set_title(title, fontsize=11)
    ax.set_xlabel(title)
    ax.set_ylabel("Subjects")

plt.tight_layout()
plt.show()
```

---

## Exam Reflection Questions

1. Why does `count(*) filter(where ...)` outperform a separate `groupBy().agg()` for each sleep stage?
2. What is the difference between `percentile()` and `percentile_approx()`, and why does the latter scale better?
3. Why are artifact epochs excluded before computing band power statistics?
4. How does the `lag()` window function detect sleep stage transitions?
5. Name three data quality checks that are appropriate for a Gold layer.
6. Why does the Gold table have far fewer rows than the Silver table?

**Reference answers**:

1. `filter(where ...)` computes all conditional counts in a single full-table scan. A separate `groupBy()` per stage requires one scan per stage, multiplying I/O cost.
2. `percentile()` sorts all values to compute exact quantiles, which is O(n log n). `percentile_approx()` uses the t-digest sketch algorithm and runs in a single pass with less than 1 % error, making it suitable for millions of rows.
3. Artifact epochs contain clipped or saturated signal amplitudes that are outliers relative to true EEG. Including them inflates the mean and inflates the standard deviation, producing misleading feature values.
4. `lag("sleep_stage").over(window)` retrieves the stage from the previous epoch. Comparing the current stage to the lagged stage yields a Boolean `is_transition` flag. Summing those flags gives the total transition count.
5. Stage percentages sum to 100 %; artifact rate below a threshold; no null values in key feature columns.
6. Silver stores one row per 30-second epoch, yielding hundreds of rows per subject. Gold aggregates all epochs into exactly one summary row per subject.

---

## Day 6 Operation Reference

| Operation | Function | Notes |
|---|---|---|
| Filtered aggregation | `count(*) filter(where ...)` | Single-pass; preferred over multiple `groupBy()` calls |
| Approximate percentile | `percentile_approx(col, q)` | < 1 % error; O(n) complexity |
| Window lag | `F.lag("col").over(window)` | Requires `partitionBy` and `orderBy` |
| Inner join | `df.join(other, on="key", how="inner")` | Drops subjects absent from either side |
| Delta write | `.write.format("delta").mode("overwrite")` | Overwrites data; preserves history |
| Programmatic assertion | `assert condition, message` | Raises `AssertionError` on failure |

**Next**: Day 7 orchestrates the complete Bronze → Silver → Gold pipeline using Delta Live Tables, with declarative data quality constraints and auto-recovery.
