# Day 8: Topological Data Analysis on EEG Features and Production Deployment

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day08_tda_and_deployment.py` |
| **Exam domains** | Domain 3 — Databricks Jobs and Workflows; Domain 4 — Delta Live Tables |
| **Time estimate** | 4–5 hours |
| **Prerequisite** | Day 7 completed; `eeg_lakehouse.gold.subject_features` table exists |

---

## Section 1: Environment Setup

Complete every step in this section before opening any notebook cell. A reader starting from a blank Databricks workspace must follow these steps in order.

### 1.1 Create a GitHub Personal Access Token

A GitHub PAT is required to authenticate Git integration in Databricks.

1. Sign in to [github.com](https://github.com).
2. Navigate to **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. Click **Generate new token (classic)**.
4. Set the following fields:

   | Field | Value |
   |---|---|
   | Note | `databricks-eeg-lab` |
   | Expiration | 90 days |
   | Scopes | `repo` (full), `workflow` |

5. Click **Generate token** and copy the token immediately. Store it in a password manager — it will not be shown again.

### 1.2 Configure Databricks Git Integration

1. In your Databricks workspace, click your username in the top-right corner and select **User Settings**.
2. Select the **Git integration** tab.
3. Set the following fields:

   | Field | Value |
   |---|---|
   | Git provider | GitHub |
   | Git provider username | Your GitHub username |
   | Personal access token | The token copied in step 1.1 |

4. Click **Save**.

### 1.3 Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos**.
2. Click **Add repo**.
3. Set the following fields:

   | Field | Value |
   |---|---|
   | Git repository URL | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab` |
   | Git provider | GitHub |
   | Repo name | `databricks-eeg-lakehouse-lab` |

4. Click **Create repo**. Databricks clones the repository and makes it available under `/Repos/<your-username>/databricks-eeg-lakehouse-lab`.

### 1.4 Create a Cluster with Unity Catalog Enabled

1. In the left sidebar, click **Compute**.
2. Click **Create compute**.
3. Configure the cluster with the following settings:

   | Setting | Value |
   |---|---|
   | Cluster name | `eeg-lab-day08` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS ML** (includes MLflow, scikit-learn) |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled (set via workspace-level Unity Catalog configuration) |

4. Click **Create compute** and wait for the cluster to reach the **Running** state.

> **Unity Catalog requirement**: DBR 14.3 LTS and above are required for Unity Catalog three-part namespace access (`catalog.schema.table`). Do not use DBR 13.x or earlier.

### 1.5 Install Required Libraries

Install the following libraries on the cluster before running any notebook cells. Libraries installed via `%pip` in the first notebook cell are also acceptable; both methods are shown.

**Method A — Cluster library UI:**

1. On the cluster detail page, select the **Libraries** tab.
2. Click **Install new**.
3. Install each library individually:

   | Library source | Package |
   |---|---|
   | PyPI | `giotto-tda==0.6.0` |
   | PyPI | `scikit-learn>=1.3.0` |
   | PyPI | `matplotlib>=3.7.0` |

4. Wait for all libraries to show status **Installed** before proceeding.

**Method B — First notebook cell (alternative):**

```python
%pip install giotto-tda==0.6.0 scikit-learn matplotlib --quiet
dbutils.library.restartPython()
```

### 1.6 Open and Attach the Notebook

1. In the left sidebar, click **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Click `day08_tda_and_deployment.py` to open it.
3. In the top-right of the notebook, click **Connect** → select `eeg-lab-day08`.
4. Confirm the cluster name appears in the toolbar before executing any cell.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Apply Topological Data Analysis (TDA) to Gold layer features | Domain 4 — Advanced analytics |
| Compute persistent homology and interpret persistence diagrams | Domain 4 — Advanced analytics |
| Package a DLT pipeline as a scheduled Databricks Job | Domain 3 — Jobs and Workflows |
| Configure job failure notifications | Domain 3 — Jobs and Workflows |
| Implement CI/CD with GitHub Actions for pipeline deployment | Domain 3 — Jobs and Workflows |
| Create a monitoring view across Medallion layers | Domain 1 — Databricks Lakehouse Platform |

---

## Section 3: Background — Topological Data Analysis

Topological Data Analysis (TDA) is a mathematical framework that analyzes the **shape** of data independent of its coordinate system. For EEG sleep-stage data, TDA reveals multi-scale cluster structure that distance-based methods such as k-means may miss.

| TDA concept | Definition | EEG interpretation |
|---|---|---|
| Persistent homology | Tracks topological features (clusters, loops) as a distance threshold varies | Reveals how many distinct sleep-stage clusters exist across all scales |
| Persistence diagram | Scatter plot of (birth, death) pairs for each topological feature | Features far from the diagonal are robust signal; those near it are noise |
| Barcode | Horizontal bars showing feature lifetime | Bar length indicates feature persistence |
| H0 (0-dim) features | Connected components — clusters | Sleep stages (Wake, N1, N2, N3, REM) appear as persistent H0 features |
| H1 (1-dim) features | Loops in the data manifold | Cyclic transitions between sleep stages |
| Mapper algorithm | Graph representation of data topology | Nodes = subject clusters; edges = overlapping membership |

**Key library:** `giotto-tda` — provides `VietorisRipsPersistence`, `make_mapper_pipeline`, and Plotly-based visualization utilities.

---

## Section 4: Part 1 — Topological Data Analysis

### Step 1 — Verify Library Installation

```python
# Cell 1: verify imports before proceeding
import importlib

for pkg in ["gtda", "sklearn", "matplotlib"]:
    spec = importlib.util.find_spec(pkg)
    status = "OK" if spec is not None else "MISSING — run %pip install in a new cell"
    print(f"{pkg}: {status}")
```

Expected output:
```
gtda: OK
sklearn: OK
matplotlib: OK
```

If any library is missing, run `%pip install giotto-tda scikit-learn matplotlib --quiet` followed by `dbutils.library.restartPython()`, then re-execute this cell before continuing.

---

### Step 2 — Load Gold Features

```python
# Cell 2: load Gold table and convert to Pandas
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

gold_df = spark.read.table("eeg_lakehouse.gold.subject_features")
gold_pd = gold_df.toPandas()

print(f"Rows: {gold_pd.shape[0]}  |  Columns: {gold_pd.shape[1]}")
print(gold_pd.dtypes.to_string())
```

Expected output:
```
Rows: 50  |  Columns: 18
```

If the table is empty, verify that Day 6 and Day 7 notebooks completed successfully by running:
```sql
DESCRIBE HISTORY eeg_lakehouse.gold.subject_features
```

---

### Step 3 — Select and Validate Feature Columns

The following columns constitute the TDA feature matrix. Confirm all columns are present before proceeding.

| Column | Domain | Unit |
|---|---|---|
| `wake_pct` | Staging | Percentage |
| `n1_pct` | Staging | Percentage |
| `n2_pct` | Staging | Percentage |
| `n3_pct` | Staging | Percentage |
| `rem_pct` | Staging | Percentage |
| `sigma_mean` | Frequency | µV² |
| `sigma_std` | Frequency | µV² |
| `delta_mean` | Frequency | µV² |
| `delta_std` | Frequency | µV² |
| `transition_count` | Temporal | Count |
| `artifact_rate` | Quality | Proportion [0, 1] |

```python
# Cell 3: validate feature columns
FEATURE_COLS = [
    "wake_pct", "n1_pct", "n2_pct", "n3_pct", "rem_pct",
    "sigma_mean", "sigma_std", "delta_mean", "delta_std",
    "transition_count", "artifact_rate",
]

missing_cols = [c for c in FEATURE_COLS if c not in gold_pd.columns]
if missing_cols:
    raise ValueError(f"Missing columns in gold table: {missing_cols}")

X = gold_pd[FEATURE_COLS].values.astype(np.float64)
print(f"Feature matrix shape: {X.shape}")
print(f"NaN count: {np.isnan(X).sum()}")
```

If NaN values are reported, run:
```python
X = np.nan_to_num(X, nan=0.0)
print("NaN values replaced with 0.0")
```

---

### Step 4 — Standardize the Feature Matrix

TDA uses Euclidean distances. Features with large absolute ranges (e.g., delta power in µV²) dominate distance computations unless standardized.

```python
# Cell 4: standardize features
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print("Standardization complete.")
print(f"  Mean per feature (should be ~0): {X_scaled.mean(axis=0).round(4)}")
print(f"  Std per feature  (should be ~1): {X_scaled.std(axis=0).round(4)}")
```

---

### Step 5 — Compute Persistent Homology

```python
# Cell 5: compute Vietoris-Rips persistent homology
from gtda.homology import VietorisRipsPersistence

persistence = VietorisRipsPersistence(
    homology_dimensions=[0, 1],  # H0 = clusters, H1 = loops
    n_jobs=-1,                   # use all CPU cores
)

# gtda expects shape (n_samples, n_points, n_features)
# wrap X_scaled in a list to represent one point cloud
diagrams = persistence.fit_transform([X_scaled])

print(f"Persistence diagrams shape: {diagrams.shape}")
print("Column layout: [birth, death, dimension]")
print(f"First 5 entries:\n{diagrams[0][:5]}")
```

Expected output shape: `(1, N, 3)` where N varies by dataset size.

---

### Step 6 — Visualize the Persistence Diagram

```python
# Cell 6: plot persistence diagram
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(7, 7))

for dim, color, label in [(0, "steelblue", "H0 — Clusters"), (1, "tomato", "H1 — Loops")]:
    mask = diagrams[0][:, 2] == dim
    pts = diagrams[0][mask]
    finite = pts[pts[:, 1] < np.inf]
    ax.scatter(finite[:, 0], finite[:, 1], c=color, label=label, alpha=0.7, s=40)

max_val = diagrams[0][diagrams[0][:, 1] < np.inf, 1].max() * 1.05
ax.plot([0, max_val], [0, max_val], "k--", lw=0.8, label="Diagonal (noise boundary)")
ax.set_xlabel("Birth")
ax.set_ylabel("Death")
ax.set_title("Persistence Diagram: EEG Subject Features")
ax.legend()
plt.tight_layout()
plt.show()
```

Interpretation: points far above the diagonal are topologically significant features. For sleep-stage data, expect 3–6 persistent H0 features corresponding to distinct stage clusters.

---

### Step 7 — Extract Persistent H0 Features

```python
# Cell 7: filter H0 features by persistence threshold
PERSISTENCE_THRESHOLD = 0.2

h0_all = diagrams[0][diagrams[0][:, 2] == 0]
h0_finite = h0_all[h0_all[:, 1] < np.inf]
h0_persistent = h0_finite[(h0_finite[:, 1] - h0_finite[:, 0]) > PERSISTENCE_THRESHOLD]

print(f"Total H0 features:                {len(h0_finite)}")
print(f"Persistent H0 (threshold={PERSISTENCE_THRESHOLD}): {len(h0_persistent)}")
print("\nPersistent features (birth, death, persistence):")
for row in h0_persistent:
    print(f"  birth={row[0]:.3f}  death={row[1]:.3f}  persistence={row[1]-row[0]:.3f}")
```

Expected result: 4–6 persistent H0 features corresponding to the five standard sleep stages plus artifact clusters.

---

### Step 8 — Apply the Mapper Algorithm

The Mapper algorithm projects data to a lower-dimensional space, covers it with overlapping bins, clusters within each bin, and connects overlapping clusters to form a graph.

```python
# Cell 8: build Mapper graph
from gtda.mapper import make_mapper_pipeline
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN

mapper_pipeline = make_mapper_pipeline(
    filter_func=PCA(n_components=2),
    cover=dict(kind="uniform", n_intervals=10, overlap_frac=0.3),
    clusterer=DBSCAN(eps=0.5, min_samples=3),
    verbose=False,
)

graph = mapper_pipeline.fit_transform(X_scaled)
print(f"Mapper graph nodes: {graph.number_of_nodes()}")
print(f"Mapper graph edges: {graph.number_of_edges()}")
```

---

### Step 9 — Visualize the Mapper Graph

```python
# Cell 9: interactive Mapper visualization
from gtda.plotting import plot_static_mapper_graph

fig = plot_static_mapper_graph(
    mapper_pipeline,
    X_scaled,
    color_by_columns_dropdown=True,
    plotly_kwargs={"height": 600, "title": "Mapper Graph — EEG Subject Features"},
)
fig.show()
```

Color the nodes by `n3_pct` (slow-wave sleep percentage) to identify deep-sleep subject clusters visually.

---

## Section 5: Part 2 — Production Deployment

### Step 10 — Create a Scheduled Databricks Job for the DLT Pipeline

The following steps deploy the Day 7 DLT pipeline as a scheduled production job.

1. In the left sidebar, click **Workflows**.
2. Click **Create job**.
3. Configure the job:

   | Field | Value |
   |---|---|
   | Job name | `eeg_lakehouse_prod_pipeline` |
   | Task type | Delta Live Tables |
   | Pipeline | `eeg_lakehouse_pipeline` (created on Day 7) |
   | Cluster | DLT-managed cluster (auto-provisioned) |

4. Click **Add schedule**:

   | Field | Value |
   |---|---|
   | Schedule type | Cron |
   | Cron expression | `0 3 * * *` |
   | Timezone | UTC |

5. Click **Save job**.

---

### Step 11 — Configure Job Failure Notifications

1. With the job open, click the **Notifications** tab.
2. Click **Add notification**.
3. Set the following:

   | Field | Value |
   |---|---|
   | Trigger | On failure |
   | Destination type | Email |
   | Recipients | Your registered workspace email address |

4. Click **Save**.

To validate the alert, temporarily introduce a syntax error in the DLT notebook, trigger a manual run via **Run now**, verify the email is received, then revert the change.

---

### Step 12 — Create the CI/CD GitHub Actions Workflow

Create the file `.github/workflows/deploy-dlt.yml` in the repository with the following content. Replace `<YOUR_WORKSPACE_HOST>` with the Databricks workspace URL (e.g., `https://adb-123456789.azuredatabricks.net`).

```yaml
name: Deploy DLT Pipeline

on:
  push:
    branches:
      - main
    paths:
      - "notebooks/day07_dlt_pipeline.py"
      - "src/**"

jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Databricks CLI v2
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Validate Databricks authentication
        run: databricks auth env --profile DEFAULT || databricks configure --token <<< "${DATABRICKS_TOKEN}"

      - name: Upload notebook to workspace
        run: |
          databricks workspace import notebooks/day07_dlt_pipeline.py \
            /Workspace/prod/notebooks/day07_dlt_pipeline.py \
            --format SOURCE \
            --language PYTHON \
            --overwrite

      - name: Update DLT pipeline definition
        run: |
          databricks pipelines update \
            --pipeline-id "${{ secrets.DLT_PIPELINE_ID }}" \
            --settings '{
              "libraries": [
                {"notebook": {"path": "/Workspace/prod/notebooks/day07_dlt_pipeline.py"}}
              ]
            }'

      - name: Trigger pipeline full refresh
        run: |
          databricks pipelines start \
            --pipeline-id "${{ secrets.DLT_PIPELINE_ID }}" \
            --full-refresh
```

**GitHub Secrets required:**

| Secret name | Description |
|---|---|
| `DATABRICKS_HOST` | Workspace URL, e.g. `https://adb-123.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal access token with `clusters`, `jobs`, and `pipelines` permission |
| `DLT_PIPELINE_ID` | Pipeline UUID from the DLT pipeline Settings panel |

To add secrets: go to the repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.

---

### Step 13 — Create the Pipeline Health Monitoring View

Run the following SQL in a notebook cell attached to `eeg-lab-day08`:

```sql
-- Cell 10: create monitoring view
CREATE OR REPLACE VIEW eeg_lakehouse.monitoring.pipeline_health AS
SELECT
    'bronze_eeg_files'        AS layer,
    COUNT(*)                  AS row_count,
    MAX(ingestion_time)       AS last_updated
FROM eeg_lakehouse.bronze.eeg_files

UNION ALL

SELECT
    'silver_cleaned_epochs'   AS layer,
    COUNT(*)                  AS row_count,
    MAX(processed_timestamp)  AS last_updated
FROM eeg_lakehouse.silver.cleaned_epochs

UNION ALL

SELECT
    'gold_subject_features'   AS layer,
    COUNT(*)                  AS row_count,
    MAX(feature_timestamp)    AS last_updated
FROM eeg_lakehouse.gold.subject_features;
```

```sql
-- Cell 11: query monitoring view
SELECT * FROM eeg_lakehouse.monitoring.pipeline_health
ORDER BY layer;
```

Expected output:

| layer | row_count | last_updated |
|---|---|---|
| bronze_eeg_files | 50 | 2026-06-22 03:00:00 |
| silver_cleaned_epochs | 500 | 2026-06-22 03:05:00 |
| gold_subject_features | 50 | 2026-06-22 03:10:00 |

---

### Step 14 — Create a Databricks SQL Monitoring Dashboard

1. In the left sidebar, click **SQL** → **Dashboards** → **Create dashboard**.
2. Name the dashboard `EEG Lakehouse — Pipeline Health`.
3. Add the following widgets:

   | Widget title | Query | Visualization type |
   |---|---|---|
   | Row count by layer | `SELECT layer, row_count FROM eeg_lakehouse.monitoring.pipeline_health` | Bar chart |
   | Artifact rate distribution | `SELECT artifact_rate FROM eeg_lakehouse.silver.cleaned_epochs` | Histogram |
   | Sleep stage percentages | `SELECT 'Wake' AS stage, AVG(wake_pct) AS avg_pct FROM eeg_lakehouse.gold.subject_features UNION ALL SELECT 'N1', AVG(n1_pct) FROM eeg_lakehouse.gold.subject_features UNION ALL SELECT 'N2', AVG(n2_pct) FROM eeg_lakehouse.gold.subject_features UNION ALL SELECT 'N3', AVG(n3_pct) FROM eeg_lakehouse.gold.subject_features UNION ALL SELECT 'REM', AVG(rem_pct) FROM eeg_lakehouse.gold.subject_features` | Pie chart |

4. Click **Schedule** and set the refresh interval to **Every 1 hour**.

---

## Section 6: Exam Reference Tables

### DLT Expectation Actions

| Decorator | Violation behaviour | Use case |
|---|---|---|
| `@dlt.expect("name", "condition")` | Logs violation; row is kept | Warning-level quality checks |
| `@dlt.expect_or_drop("name", "condition")` | Drops violating rows | Remove corrupt records from Silver |
| `@dlt.expect_or_fail("name", "condition")` | Fails the entire pipeline update | Critical schema or business-rule violations |

### Databricks Jobs — Key Configuration Fields

| Field | Options | Notes |
|---|---|---|
| Task type | Notebook, DLT, JAR, Spark Submit, Python Script | Choose DLT for pipeline tasks |
| Cluster type | New job cluster, Existing cluster, DLT cluster | DLT tasks auto-provision their own cluster |
| Schedule | Manual, Cron, File arrival trigger | Use cron for time-based daily runs |
| Retry policy | Max retries, retry interval | Set retries ≥ 1 for transient failures |
| Notification trigger | On start, On success, On failure, On duration warning | Configure On failure at minimum |

### Certified Professional Exam Domain Mapping — Day 8 Topics

| Topic | Professional exam domain |
|---|---|
| DLT pipeline as a Databricks Job | Domain 3: Databricks Jobs and Workflows |
| Job notification configuration | Domain 3: Databricks Jobs and Workflows |
| CI/CD with GitHub Actions | Domain 3: Databricks Jobs and Workflows |
| `DESCRIBE HISTORY` and monitoring views | Domain 1: Databricks Lakehouse Platform |
| TDA on Gold features | Domain 4: Building Data Pipelines — Advanced analytics |

---

## Section 7: Self-Check Questions

Answer each question before proceeding to Day 9.

1. What distinguishes persistent homology from k-means clustering?
2. Why must features be standardized before applying Vietoris-Rips persistence?
3. What does a point far from the diagonal of a persistence diagram indicate?
4. Which Databricks Job task type should be selected when scheduling a DLT pipeline?
5. What is the purpose of the `DATABRICKS_TOKEN` GitHub Actions secret?
6. How does `expect_or_fail` differ from `expect_or_drop` in a DLT pipeline?

**Reference answers:**

1. K-means partitions data into a fixed number of groups at a single scale. Persistent homology tracks how connected components and loops form and dissolve across all distance scales, revealing multi-scale cluster structure without requiring a pre-specified number of groups.
2. Vietoris-Rips persistence is computed using Euclidean distance. Features with large absolute ranges (e.g., delta power in µV²) would dominate distance computations and suppress the signal from smaller-range features such as `artifact_rate`. Standardization ensures all features contribute equally.
3. A point far from the diagonal has high persistence (death − birth is large), indicating a topological feature that is robust to noise and is therefore a genuine structural property of the data.
4. Select **Delta Live Tables** as the task type. This links the job to an existing DLT pipeline and uses the DLT-managed cluster.
5. The token authenticates the Databricks CLI in the GitHub Actions runner, allowing it to call the Databricks REST API for workspace imports and pipeline updates.
6. `expect_or_fail` immediately halts the entire pipeline update when any row violates the condition, making it suitable for critical schema violations. `expect_or_drop` silently removes only the violating rows and allows the update to continue, which is appropriate for filtering out individual corrupt records.

---

## Section 8: Day 8 Summary

| Artifact | Tool / Library | Medallion layer | Exam domain |
|---|---|---|---|
| Persistent homology computation | `giotto-tda` — `VietorisRipsPersistence` | Gold | Domain 4 |
| Persistence diagram visualization | `matplotlib` | Gold | Domain 4 |
| Mapper graph | `giotto-tda` — `make_mapper_pipeline` | Gold | Domain 4 |
| Scheduled production job | Databricks Jobs | — | Domain 3 |
| CI/CD deployment workflow | GitHub Actions + Databricks CLI v2 | — | Domain 3 |
| Pipeline health monitoring view | Databricks SQL | All layers | Domain 1 |

**Next**: Day 9 covers comprehensive monitoring, custom audit logging, and data quality dashboards for the production EEG lakehouse.
