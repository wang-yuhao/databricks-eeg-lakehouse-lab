# Day 8: Advanced Topics — TDA on EEG Data & Production Deployment

**Notebook**: `notebooks/day08_tda_and_deployment.py`
**Source modules**: `src/tda/persistence.py`, CI/CD configs
**Exam domains**: Advanced analytics, Production deployment (Domain 3 — Jobs, Workflows)
**Time estimate**: 4–5 hours
**Prerequisite**: Day 7 completed, Gold table exists

---

## Objectives

- Apply Topological Data Analysis (TDA) to Gold features
- Compute persistent homology on sleep data
- Visualize persistence diagrams and barcodes
- Deploy DLT pipeline to production with Databricks Jobs
- Implement CI/CD with GitHub Actions
- Set up monitoring and alerting

---

## Background

**What is Topological Data Analysis (TDA)?**

TDA is a mathematical framework for analyzing the **shape** of data:

- **Persistent Homology**: Tracks topological features (clusters, loops, voids) as you vary a scale parameter
- **Persistence Diagrams**: Visualize when features appear (birth) and disappear (death)
- **Barcodes**: Show feature lifetimes

**Why TDA for sleep data?**

Sleep stages form **clusters** in feature space (e.g., wake vs. deep sleep). TDA can:

1. Detect sleep disorders (abnormal cluster structure)
2. Find biomarkers (persistent topological features)
3. Reduce dimensionality while preserving geometry

**Key Python libraries:**

- `giotto-tda`: TDA toolkit (persistent homology, Mapper algorithm)
- `ripser`: Fast Vietoris-Rips persistence computation

---

## Part 1: Topological Data Analysis

### Step 1 — Install TDA libraries

```python
# In Databricks notebook cell
%pip install giotto-tda scikit-learn
```

---

### Step 2 — Load Gold features

```python
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# Load Gold table
gold_df = spark.read.table("eeg_lakehouse.gold.subject_features")

# Convert to Pandas for TDA (TDA libraries use NumPy/Pandas)
gold_pd = gold_df.toPandas()

print(f"Gold table shape: {gold_pd.shape}")
gold_pd.head()
```

**Expected output:**

```
Gold table shape: (50, 18)
```

---

### Step 3 — Prepare feature matrix

```python
# Select features for TDA
feature_cols = [
    "wake_pct", "n1_pct", "n2_pct", "n3_pct", "rem_pct",
    "sigma_mean", "sigma_std", "delta_mean", "delta_std",
    "transition_count", "artifact_rate"
]

X = gold_pd[feature_cols].values

print(f"Feature matrix shape: {X.shape}")
print(f"Feature matrix dtype: {X.dtype}")
```

**Expected output:**

```
Feature matrix shape: (50, 11)
Feature matrix dtype: float64
```

---

### Step 4 — Standardize features

```python
from sklearn.preprocessing import StandardScaler

# Standardize features (TDA is sensitive to scale)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print(f"Scaled feature matrix:\n{X_scaled[:5]}")
```

**Why standardize?**

- TDA uses Euclidean distance → features with large ranges dominate
- Standardization ensures all features contribute equally

---

### Step 5 — Compute persistent homology

```python
from gtda.homology import VietorisRipsPersistence

# Compute 0-dimensional (clusters) and 1-dimensional (loops) persistence
persistence = VietorisRipsPersistence(homology_dimensions=[0, 1], n_jobs=-1)

# Fit on scaled data
diagrams = persistence.fit_transform([X_scaled])

print(f"Persistence diagrams shape: {diagrams.shape}")
print(f"First 10 features:\n{diagrams[0][:10]}")
```

**Expected output:**

```
Persistence diagrams shape: (1, 150, 3)
First 10 features:
[[0.         0.234      0.        ]
 [0.         0.189      0.        ]
 [0.         0.156      0.        ]
 ...
 [0.521      0.892      1.        ]]
```

**Interpretation:**

- Each row = one topological feature
- Column 0: Birth time (when feature appears)
- Column 1: Death time (when feature disappears)
- Column 2: Dimension (0 = cluster, 1 = loop)

---

### Step 6 — Visualize persistence diagram

```python
import matplotlib.pyplot as plt
from gtda.plotting import plot_diagram

# Plot persistence diagram
plot_diagram(diagrams[0])
plt.title("Persistence Diagram: Sleep EEG Features")
plt.xlabel("Birth")
plt.ylabel("Death")
plt.show()
```

**Expected visualization:**

- Points far from diagonal = **persistent** features (long-lived)
- Points near diagonal = **noise** (short-lived)

---

### Step 7 — Filter persistent features

```python
# Extract H0 (0-dimensional features = clusters)
h0_features = diagrams[0][diagrams[0][:, 2] == 0]

# Filter by persistence (death - birth > 0.2)
persistence_threshold = 0.2
persistent_h0 = h0_features[h0_features[:, 1] - h0_features[:, 0] > persistence_threshold]

print(f"Total H0 features: {len(h0_features)}")
print(f"Persistent H0 features (persistence > {persistence_threshold}): {len(persistent_h0)}")
```

**Expected output:**

```
Total H0 features: 50
Persistent H0 features (persistence > 0.2): 5
```

**Interpretation:**

- 5 persistent clusters → likely correspond to 5 sleep stages (Wake, N1, N2, N3, REM)

---

### Step 8 — Apply Mapper algorithm

```python
from gtda.mapper import make_mapper_pipeline
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN

# Create Mapper pipeline
mapper = make_mapper_pipeline(
    filter_func=PCA(n_components=2),
    cover=dict(kind='uniform', n_intervals=10, overlap_frac=0.3),
    clusterer=DBSCAN(eps=0.5, min_samples=3),
    verbose=False
)

# Fit Mapper
graph = mapper.fit_transform(X_scaled)

print(f"Mapper graph: {graph}")
```

**Mapper output:**

- Graph representation of data topology
- Nodes = clusters of similar subjects
- Edges = overlapping clusters

---

### Step 9 — Visualize Mapper graph

```python
from gtda.plotting import plot_static_mapper_graph

# Plot Mapper graph
fig = plot_static_mapper_graph(
    mapper,
    X_scaled,
    color_by_columns_dropdown=True,
    plotly_kwargs={"height": 600}
)
fig.show()
```

**Expected visualization:**

- Network graph showing sleep stage relationships
- Color nodes by `n3_pct` to see deep sleep patterns

---

## Part 2: Production Deployment

### Step 10 — Package DLT pipeline as Databricks Job

1. Go to **Workflows** > **Jobs** > **Create Job**
2. Configure job:
   - **Job name**: `eeg_lakehouse_prod_pipeline`
   - **Task type**: **Delta Live Tables**
   - **Pipeline**: Select `eeg_lakehouse_pipeline`
   - **Schedule**: **Cron** → `0 3 * * *` (daily at 3 AM)
   - **Cluster**: Use existing DLT pipeline cluster
3. Click **Create**

---

### Step 11 — Add alerting on pipeline failure

1. In job UI, go to **Notifications**
2. Add alert:
   - **Trigger**: **On failure**
   - **Recipient**: Your email
3. Save

**Test alert:**

1. Intentionally break pipeline (e.g., add invalid SQL)
2. Run job
3. Verify email alert received

---

### Step 12 — Set up CI/CD with GitHub Actions

Create `.github/workflows/deploy-dlt.yml`:

```yaml
name: Deploy DLT Pipeline

on:
  push:
    branches:
      - main
    paths:
      - 'notebooks/day07_dlt_pipeline.py'
      - 'src/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Databricks CLI
        run: |
          pip install databricks-cli
          echo "${{ secrets.DATABRICKS_TOKEN }}" | databricks configure --token

      - name: Upload DLT notebook
        run: |
          databricks workspace import_dir \
            ./notebooks /Workspace/prod/notebooks \
            --overwrite

      - name: Update DLT pipeline
        run: |
          databricks pipelines update \
            --pipeline-id ${{ secrets.DLT_PIPELINE_ID }} \
            --notebook-path /Workspace/prod/notebooks/day07_dlt_pipeline.py

      - name: Trigger pipeline run
        run: |
          databricks pipelines start --pipeline-id ${{ secrets.DLT_PIPELINE_ID }}
```

**Setup:**

1. Add secrets to GitHub repo:
   - `DATABRICKS_TOKEN`: Personal access token
   - `DLT_PIPELINE_ID`: Pipeline ID from DLT UI
2. Commit and push → pipeline auto-deploys

---

### Step 13 — Monitor pipeline with Databricks SQL

Create monitoring dashboard:

```sql
-- Create monitoring view
CREATE OR REPLACE VIEW eeg_lakehouse.monitoring.pipeline_health AS
SELECT
  'bronze_eeg_files' AS layer,
  COUNT(*) AS row_count,
  MAX(ingestion_time) AS last_updated
FROM eeg_lakehouse.bronze.eeg_files

UNION ALL

SELECT
  'silver_cleaned_epochs' AS layer,
  COUNT(*) AS row_count,
  CURRENT_TIMESTAMP() AS last_updated
FROM eeg_lakehouse.silver.cleaned_epochs

UNION ALL

SELECT
  'gold_subject_features' AS layer,
  COUNT(*) AS row_count,
  CURRENT_TIMESTAMP() AS last_updated
FROM eeg_lakehouse.gold.subject_features;

-- Query monitoring view
SELECT * FROM eeg_lakehouse.monitoring.pipeline_health;
```

**Expected output:**

```
+---------------------+---------+-------------------+
|layer                |row_count|last_updated       |
+---------------------+---------+-------------------+
|bronze_eeg_files     |50       |2026-06-22 03:00:00|
|silver_cleaned_epochs|500      |2026-06-22 03:05:00|
|gold_subject_features|50       |2026-06-22 03:10:00|
+---------------------+---------+-------------------+
```

---

### Step 14 — Create Databricks SQL dashboard

1. Go to **SQL** > **Dashboards** > **Create Dashboard**
2. Add widgets:
   - **Row count by layer** (bar chart)
   - **Artifact rate distribution** (histogram)
   - **Sleep stage percentages** (pie chart)
3. Schedule refresh: **Every 1 hour**

---

### Step 15 — Final review and exam tips

**Key topics covered in this course:**

1. **Domain 1: Delta Live Tables**
   - `@dlt.table`, `@dlt.view`
   - `expect`, `expect_or_drop`, `expect_or_fail`
   - Streaming vs. batch

2. **Domain 2: Spark SQL & Python**
   - `mapInPandas` for EDF parsing
   - `filter(where ...)` aggregations
   - Window functions (`lag`, `lead`)

3. **Domain 3: Delta Lake**
   - Time travel (`versionAsOf`, `timestampAsOf`)
   - OPTIMIZE, ZORDER, VACUUM
   - Transaction log inspection

4. **Domain 4: Data Quality**
   - Artifact detection (z-score)
   - Assertions (`assert`, DLT expectations)
   - Data profiling

**Exam preparation:**

- Review self-check questions from Days 2–7
- Practice writing DLT pipelines from scratch
- Memorize OPTIMIZE vs. VACUUM behavior
- Understand when to use `mapInPandas` vs. Pandas UDF

---

## Self-Check: Answer Exam Reflection Questions

1. What is the difference between persistent homology and clustering?
2. Why do we standardize features before TDA?
3. How do you deploy a DLT pipeline to production?
4. What is the purpose of the Mapper algorithm?
5. How do you monitor DLT pipeline health?
6. What is the benefit of CI/CD for data pipelines?

**Answers:**

1. **Clustering** assigns data points to discrete groups. **Persistent homology** tracks how clusters/loops evolve across all scales, revealing multi-scale structure.
2. TDA uses Euclidean distance. Without standardization, features with large ranges (e.g., `delta_power` in µV²) dominate over small-range features (e.g., `artifact_rate` as %).
3. Create a **Databricks Job** that runs the DLT pipeline on a schedule. Use **Databricks CLI** or **GitHub Actions** to automate deployment.
4. **Mapper** creates a graph representation of data topology by:
   - Projecting data to lower dimensions (e.g., PCA)
   - Covering the projection with overlapping bins
   - Clustering within each bin
   - Connecting overlapping clusters
5. Query `DESCRIBE HISTORY` on Delta tables, monitor row counts, check DLT UI for data quality violations, and set up SQL dashboards.
6. **CI/CD** ensures:
   - Code changes are automatically tested
   - Pipelines are deployed consistently
   - Rollback is easy if bugs are introduced
   - Team collaboration is streamlined

---

## Day 8 Summary

| What was built | Tool/Library | Purpose |
|---|---|---|
| Persistent homology | `giotto-tda` | Detect topological features in sleep data |
| Persistence diagrams | Matplotlib | Visualize feature lifetimes |
| Mapper graph | `giotto-tda.mapper` | Network representation of data |
| Production job | Databricks Jobs | Schedule DLT pipeline |
| CI/CD pipeline | GitHub Actions | Auto-deploy on code changes |
| Monitoring dashboard | Databricks SQL | Track pipeline health |

**Congratulations!** You've completed the 8-day EEG Lakehouse Lab:

- **Day 2**: Bronze layer — PhysioNet ingestion
- **Day 3**: Unity Catalog setup
- **Day 4**: Silver layer — EEG preprocessing
- **Day 5**: Delta Lake internals
- **Day 6**: Gold layer — Feature engineering
- **Day 7**: Delta Live Tables — Production pipeline
- **Day 8**: TDA & Deployment

You're now ready for the **Databricks Data Engineer Associate** exam and have a production-ready EEG lakehouse!
