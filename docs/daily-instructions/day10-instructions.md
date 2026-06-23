# Day 10: Machine Learning with MLflow, Feature Store, and Model Registry

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day10_ml_pipeline.py` |
| **Exam domains** | Domain 2 — ELT with Apache Spark; Domain 5 — ML with Databricks |
| **Time estimate** | 5–6 hours |
| **Prerequisite** | Days 1–9 completed; Gold layer `eeg_lakehouse.gold.eeg_features` table exists; DLT pipeline has run at least once |

---

## Section 1: Environment Setup

Complete every step in this section before opening any notebook cell. A reader starting from a blank Databricks workspace must follow these steps in order.

### 1.1 Create a GitHub Personal Access Token

1. Sign in to [github.com](https://github.com).
2. Navigate to **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. Click **Generate new token (classic)**.
4. Configure the token:

   | Field | Value |
   |---|---|
   | Note | `databricks-eeg-lab` |
   | Expiration | 90 days |
   | Scopes | `repo` (full), `workflow` |

5. Copy the token immediately and store it in a password manager. It will not be displayed again.

### 1.2 Configure Databricks Git Integration

1. Click your username in the top-right corner of the workspace and select **User Settings**.
2. Select the **Git integration** tab.
3. Enter the following:

   | Field | Value |
   |---|---|
   | Git provider | GitHub |
   | Git provider username | Your GitHub username |
   | Personal access token | Token from step 1.1 |

4. Click **Save**.

### 1.3 Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos**.
2. Click **Add repo**.
3. Fill in the fields:

   | Field | Value |
   |---|---|
   | Git repository URL | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab` |
   | Git provider | GitHub |
   | Repo name | `databricks-eeg-lakehouse-lab` |

4. Click **Create repo**.

### 1.4 Create a Cluster with Unity Catalog Enabled

1. In the left sidebar, click **Compute** → **Create compute**.
2. Apply the following configuration:

   | Setting | Value |
   |---|---|
   | Cluster name | `eeg-lab-day10` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS ML** (includes MLflow, scikit-learn, XGBoost) |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled (access mode: Single user) |

   > Use the **14.3 LTS ML** runtime, not the standard 14.3 LTS. The ML variant pre-installs MLflow, scikit-learn, XGBoost, LightGBM, and PyTorch, eliminating manual library installation.

3. Click **Create compute** and wait for the cluster to reach the **Running** state.

### 1.5 Install Required Libraries

The 14.3 LTS ML runtime includes all required packages. Verify availability in Cell 1 of the notebook. No additional `%pip install` commands are needed unless the Feature Store client is absent.

If the Feature Store client import fails, install it once:

```python
# Run only if "from databricks import feature_store" raises ModuleNotFoundError
%pip install databricks-feature-store==0.17.0
dbutils.library.restartPython()
```

### 1.6 Open and Attach the Notebook

1. In the left sidebar, click **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Click `day10_ml_pipeline.py` to open it.
3. Click **Connect** in the top-right and select `eeg-lab-day10`.
4. Confirm the cluster name appears in the toolbar before running any cell.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Create and write to a Databricks Feature Store table | Domain 5 — ML with Databricks |
| Engineer time-domain and frequency-domain EEG features | Domain 2 — ELT with Apache Spark |
| Configure and use MLflow experiments and autologging | Domain 5 — MLflow tracking |
| Train multiple classifiers and compare runs in the MLflow UI | Domain 5 — Experiment tracking |
| Register, version, and promote models via the Model Registry | Domain 5 — Model Registry |
| Perform batch inference against a registered production model | Domain 5 — Model deployment |
| Implement prediction distribution monitoring | Domain 5 — Model monitoring |

---

## Section 3: Background

### MLflow Components

| Component | Purpose | Databricks integration |
|---|---|---|
| Tracking server | Records parameters, metrics, and artifacts for every run | Managed automatically; persists in the workspace |
| Experiments | Groups runs for one logical project | Created via `mlflow.set_experiment()` or the UI |
| Model Registry | Centralized versioned model store with lifecycle stages | Accessible via `MlflowClient` or UI |
| Autologging | Automatically captures hyperparameters, metrics, and model artifacts | `mlflow.<flavor>.autolog()` |
| `pyfunc` flavor | Generic model wrapper enabling framework-agnostic serving | `mlflow.pyfunc.load_model()` |

### Model Registry Lifecycle Stages

| Stage | Description | Promotion trigger |
|---|---|---|
| `None` | Model version registered but unreviewed | Automatic after `mlflow.register_model()` |
| `Staging` | Validated on hold-out data; ready for acceptance | Manual or programmatic via `MlflowClient` |
| `Production` | Serving live inference traffic | Passed validation gate (F1 ≥ threshold) |
| `Archived` | Superseded by a newer version | Auto-archived when a new version enters the same stage |

### Feature Store Architecture

| Concept | Detail |
|---|---|
| Feature table | A Delta table registered in the Feature Store with declared primary keys |
| `FeatureLookup` | Declares how training data is enriched from a feature table at training time |
| Training set | A `TrainingSet` object that joins labels with feature lookups for consistent offline training |
| Point-in-time lookup | Time-travel joins to prevent label leakage in time-series datasets |

---

## Section 4: Part 1 — Catalog and Schema Preparation

### Step 1 — Initialise Schemas

```python
# Cell 1: create required schemas and verify prerequisites
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS eeg_lakehouse.ml_features COMMENT 'Feature Store tables for EEG ML workloads'")
spark.sql("CREATE SCHEMA IF NOT EXISTS eeg_lakehouse.ml_predictions COMMENT 'Batch inference output tables'")

# Verify the Gold source table exists
row_count = spark.table("eeg_lakehouse.gold.eeg_features").count()
print(f"Source Gold table row count: {row_count:,}")
assert row_count > 0, "eeg_lakehouse.gold.eeg_features is empty. Complete Day 6 before proceeding."
print("Prerequisites verified.")
```

---

## Section 5: Part 2 — Feature Engineering

### Step 2 — Load and Engineer Features

```python
# Cell 2: load Gold layer and compute ML-ready features
from pyspark.sql import Window
from pyspark.sql import functions as F

raw_df = spark.table("eeg_lakehouse.gold.eeg_features")

patient_window = Window.partitionBy("patient_id").orderBy("processed_timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

engineered_df = (
    raw_df
    # Temporal context
    .withColumn("recording_hour",        F.hour("processed_timestamp"))
    .withColumn("recording_dow",         F.dayofweek("processed_timestamp"))
    .withColumn("is_weekend",            F.when(F.dayofweek("processed_timestamp").isin([1, 7]), 1).otherwise(0))
    # Frequency-band ratios (clinically interpretable EEG features)
    .withColumn("theta_alpha_ratio",     F.col("theta_power") / (F.col("alpha_power")  + F.lit(1e-6)))
    .withColumn("delta_theta_ratio",     F.col("delta_power") / (F.col("theta_power")  + F.lit(1e-6)))
    .withColumn("high_low_freq_ratio",   (F.col("beta_power") + F.col("gamma_power")) / (F.col("delta_power") + F.col("theta_power") + F.lit(1e-6)))
    # Amplitude statistics
    .withColumn("amplitude_range",       F.col("max_amplitude") - F.col("min_amplitude"))
    .withColumn("coeff_of_variation",    F.col("std_amplitude") / (F.abs(F.col("mean_amplitude")) + F.lit(1e-6)))
    # Patient-level rolling history
    .withColumn("patient_rec_count",     F.count("recording_id").over(patient_window))
    .withColumn("patient_avg_quality",   F.avg("signal_quality_score").over(patient_window))
    .withColumn("patient_seizure_rate",  F.avg(F.when(F.col("has_seizure") == 1, 1.0).otherwise(0.0)).over(patient_window))
)

print(f"Engineered DataFrame row count : {engineered_df.count():,}")
print(f"Column count                   : {len(engineered_df.columns)}")
engineered_df.printSchema()
```

---

## Section 6: Part 3 — Databricks Feature Store

### Step 3 — Create and Populate the Feature Table

```python
# Cell 3: register feature table in the Databricks Feature Store
from databricks import feature_store

fs = feature_store.FeatureStoreClient()

FEATURE_TABLE = "eeg_lakehouse.ml_features.eeg_ml_features"

feature_columns = [
    "recording_id",
    "patient_id",
    "processed_timestamp",
    "mean_amplitude", "std_amplitude", "max_amplitude", "min_amplitude",
    "skewness", "kurtosis",
    "delta_power", "theta_power", "alpha_power", "beta_power", "gamma_power",
    "dominant_frequency",
    "signal_quality_score",
    "num_channels", "sampling_rate", "duration_seconds",
    "recording_hour", "recording_dow", "is_weekend",
    "theta_alpha_ratio", "delta_theta_ratio", "high_low_freq_ratio",
    "amplitude_range", "coeff_of_variation",
    "patient_rec_count", "patient_avg_quality", "patient_seizure_rate",
]

feature_df = engineered_df.select(*feature_columns)

try:
    fs.create_table(
        name=FEATURE_TABLE,
        primary_keys=["recording_id"],
        timestamp_keys=["processed_timestamp"],
        df=feature_df,
        description="ML-ready EEG features for seizure detection. Primary key: recording_id.",
    )
    print(f"Feature table created: {FEATURE_TABLE}")
except Exception as exc:
    if "already exists" in str(exc).lower():
        fs.write_table(name=FEATURE_TABLE, df=feature_df, mode="merge")
        print(f"Feature table updated (merge): {FEATURE_TABLE}")
    else:
        raise
```

### Step 4 — Verify Feature Table

```python
# Cell 4: read back and verify the feature table
verification_df = fs.read_table(FEATURE_TABLE)
print(f"Feature table row count : {verification_df.count():,}")
print(f"Feature table columns   : {len(verification_df.columns)}")
display(verification_df.limit(5))
```

---

## Section 7: Part 4 — Model Training with MLflow

### Step 5 — Configure the MLflow Experiment

```python
# Cell 5: configure MLflow experiment
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

EXPERIMENT_PATH = "/Users/eeg-lab/eeg-seizure-detection"
mlflow.set_experiment(EXPERIMENT_PATH)

# Enable autologging for scikit-learn (captures params, metrics, and model artefact)
mlflow.sklearn.autolog(log_input_examples=True, log_model_signatures=True, silent=True)

experiment = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
print(f"Experiment ID  : {experiment.experiment_id}")
print(f"Artifact store : {experiment.artifact_location}")
```

### Step 6 — Build the Training Set via Feature Lookups

```python
# Cell 6: assemble training set using Feature Store lookups
import pandas as pd
from sklearn.model_selection import train_test_split

LABEL_TABLE  = "eeg_lakehouse.gold.eeg_features"
LABEL_COLUMN = "has_seizure"

label_df = spark.sql(f"""
    SELECT
        recording_id,
        {LABEL_COLUMN} AS label
    FROM {LABEL_TABLE}
    WHERE processed_timestamp >= current_timestamp() - INTERVAL 90 DAYS
""")

feature_lookup = [
    feature_store.FeatureLookup(
        table_name=FEATURE_TABLE,
        lookup_key="recording_id",
        timestamp_lookup_key="processed_timestamp",
        feature_names=[
            "mean_amplitude", "std_amplitude", "max_amplitude", "min_amplitude",
            "skewness", "kurtosis",
            "delta_power", "theta_power", "alpha_power", "beta_power", "gamma_power",
            "dominant_frequency", "signal_quality_score",
            "recording_hour", "recording_dow", "is_weekend",
            "theta_alpha_ratio", "delta_theta_ratio", "high_low_freq_ratio",
            "amplitude_range", "coeff_of_variation",
            "patient_rec_count", "patient_avg_quality", "patient_seizure_rate",
        ],
    )
]

training_set = fs.create_training_set(
    df=label_df,
    feature_lookups=feature_lookup,
    label="label",
    exclude_columns=["patient_id", "processed_timestamp"],
)

training_pd = training_set.load_df().toPandas()
X = training_pd.drop(["recording_id", "label"], axis=1)
y = training_pd["label"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Training samples : {len(X_train):,}")
print(f"Test samples     : {len(X_test):,}")
print(f"Positive class % : {y_train.mean():.2%}")
print(f"Feature count    : {X_train.shape[1]}")
```

### Step 7 — Train Multiple Classifiers

```python
# Cell 7: train classifiers and log all runs to MLflow
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    f1_score, roc_auc_score,
)

MODELS = {
    "LogisticRegression":   LogisticRegression(max_iter=1000, random_state=42, class_weight="balanced"),
    "RandomForest":         RandomForestClassifier(n_estimators=200, max_depth=12, random_state=42, class_weight="balanced"),
    "GradientBoosting":     GradientBoostingClassifier(n_estimators=200, max_depth=5, random_state=42),
}

run_results = {}

for model_name, model in MODELS.items():
    with mlflow.start_run(run_name=model_name) as run:
        model.fit(X_train, y_train)

        y_pred       = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]

        metrics = {
            "accuracy"  : accuracy_score(y_test, y_pred),
            "precision" : precision_score(y_test, y_pred, zero_division=0),
            "recall"    : recall_score(y_test, y_pred, zero_division=0),
            "f1_score"  : f1_score(y_test, y_pred, zero_division=0),
            "auc_roc"   : roc_auc_score(y_test, y_pred_proba),
        }

        mlflow.log_metrics(metrics)
        run_results[model_name] = {"run_id": run.info.run_id, "metrics": metrics, "model": model}

        print(f"\n{model_name} | run_id={run.info.run_id}")
        for k, v in metrics.items():
            print(f"  {k:<12}: {v:.4f}")

print("\nAll models trained and logged to MLflow.")
```

---

## Section 8: Part 5 — Hyperparameter Optimisation

### Step 8 — Grid Search with MLflow Tracking

```python
# Cell 8: hyperparameter tuning for Random Forest with full MLflow logging
from sklearn.model_selection import GridSearchCV

PARAM_GRID = {
    "n_estimators"   : [100, 200, 400],
    "max_depth"      : [6, 10, 15],
    "min_samples_split": [2, 5],
    "min_samples_leaf" : [1, 2],
    "class_weight"   : ["balanced"],
}

with mlflow.start_run(run_name="RandomForest_GridSearch") as gs_run:
    rf_base = RandomForestClassifier(random_state=42)
    grid_search = GridSearchCV(
        rf_base, PARAM_GRID,
        cv=5, scoring="f1",
        n_jobs=-1, refit=True, verbose=0,
    )
    grid_search.fit(X_train, y_train)

    best_model  = grid_search.best_estimator_
    y_pred_gs   = best_model.predict(X_test)
    y_proba_gs  = best_model.predict_proba(X_test)[:, 1]

    gs_metrics = {
        "accuracy"       : accuracy_score(y_test, y_pred_gs),
        "precision"      : precision_score(y_test, y_pred_gs, zero_division=0),
        "recall"         : recall_score(y_test, y_pred_gs, zero_division=0),
        "f1_score"       : f1_score(y_test, y_pred_gs, zero_division=0),
        "auc_roc"        : roc_auc_score(y_test, y_proba_gs),
        "cv_best_f1"     : grid_search.best_score_,
    }

    mlflow.log_params(grid_search.best_params_)
    mlflow.log_metrics(gs_metrics)

    mlflow.sklearn.log_model(
        best_model,
        artifact_path="model",
        registered_model_name="eeg_seizure_rf_tuned",
    )

    run_results["RandomForest_GridSearch"] = {
        "run_id": gs_run.info.run_id, "metrics": gs_metrics, "model": best_model,
    }

    print(f"Best params : {grid_search.best_params_}")
    print(f"CV F1       : {grid_search.best_score_:.4f}")
    print(f"Test F1     : {gs_metrics['f1_score']:.4f}")
    print(f"Test AUC    : {gs_metrics['auc_roc']:.4f}")
    print(f"Run ID      : {gs_run.info.run_id}")
```

---

## Section 9: Part 6 — Feature Importance Analysis

### Step 9 — Plot and Log Feature Importance

```python
# Cell 9: compute and log feature importance for the best Random Forest
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

TOP_N = 20
importances = best_model.feature_importances_
feature_names = X_train.columns.tolist()

importance_pairs = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
top_features, top_importances = zip(*importance_pairs[:TOP_N])

with mlflow.start_run(run_name="FeatureImportance_Analysis"):
    fig, ax = plt.subplots(figsize=(10, 8))
    y_pos = np.arange(TOP_N)
    ax.barh(y_pos, top_importances[::-1], color="#1f77b4")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(list(top_features[::-1]), fontsize=9)
    ax.set_xlabel("Importance (mean decrease in impurity)")
    ax.set_title(f"Top {TOP_N} Feature Importances — Random Forest (Grid Search)")
    plt.tight_layout()

    mlflow.log_figure(fig, "feature_importance.png")
    mlflow.log_param("top_features", ",".join(top_features[:10]))
    plt.close(fig)

print("Top 10 features:")
for name, score in importance_pairs[:10]:
    print(f"  {name:<35}: {score:.6f}")
```

---

## Section 10: Part 7 — Model Registry and Promotion

### Step 10 — Select the Best Run and Register the Model

```python
# Cell 10: identify the best run by F1 score and register the model
experiment    = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
runs_df       = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["metrics.f1_score DESC"],
    max_results=10,
)

best_run = runs_df.iloc[0]
best_run_id      = best_run["run_id"]
best_f1          = best_run["metrics.f1_score"]
REGISTRY_MODEL   = "eeg_seizure_production"

print(f"Best run ID : {best_run_id}")
print(f"Best F1     : {best_f1:.4f}")

model_version = mlflow.register_model(
    model_uri=f"runs:/{best_run_id}/model",
    name=REGISTRY_MODEL,
)
print(f"Registered as version {model_version.version} of '{REGISTRY_MODEL}'")
```

### Step 11 — Transition Model Through Staging to Production

```python
# Cell 11: run validation gate, then promote or reject
client = MlflowClient()

STAGING_F1_THRESHOLD    = 0.70
PRODUCTION_F1_THRESHOLD = 0.75

def transition_model(model_name: str, version: str, stage: str) -> None:
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage=stage,
        archive_existing_versions=True,
    )
    print(f"Model '{model_name}' version {version} → {stage}")


# Promote to Staging unconditionally for validation
transition_model(REGISTRY_MODEL, model_version.version, "Staging")

# Validation gate: load model from Staging and re-evaluate
staging_model   = mlflow.pyfunc.load_model(f"models:/{REGISTRY_MODEL}/Staging")
y_pred_staging  = staging_model.predict(X_test)
staging_f1      = f1_score(y_test, y_pred_staging, zero_division=0)
staging_acc     = accuracy_score(y_test, y_pred_staging)

print(f"\nStaging validation — F1: {staging_f1:.4f} | Accuracy: {staging_acc:.4f}")
print(f"Production F1 threshold : {PRODUCTION_F1_THRESHOLD}")

if staging_f1 >= PRODUCTION_F1_THRESHOLD:
    transition_model(REGISTRY_MODEL, model_version.version, "Production")
    print("Model promoted to Production.")
else:
    print(
        f"Model did NOT meet the production threshold "
        f"(F1={staging_f1:.4f} < {PRODUCTION_F1_THRESHOLD}). "
        f"Keeping in Staging for further review."
    )
```

---

## Section 11: Part 8 — Batch Inference

### Step 12 — Run Batch Inference against the Production Model

```python
# Cell 12: batch inference using the registered production model
INFERENCE_TABLE  = "eeg_lakehouse.ml_features.eeg_ml_features"
PREDICTIONS_TABLE = "eeg_lakehouse.ml_predictions.seizure_predictions"

prod_model = mlflow.pyfunc.load_model(f"models:/{REGISTRY_MODEL}/Production")

inference_spark_df = spark.table(INFERENCE_TABLE)
model_feature_cols = X_train.columns.tolist()

inference_pd = inference_spark_df.select(
    ["recording_id"] + model_feature_cols
).toPandas()

preds = prod_model.predict(inference_pd[model_feature_cols])

inference_pd["prediction"]            = preds
inference_pd["prediction_timestamp"]  = pd.Timestamp.utcnow()
inference_pd["model_version"]         = model_version.version
inference_pd["model_name"]            = REGISTRY_MODEL

predictions_spark_df = spark.createDataFrame(inference_pd)

(
    predictions_spark_df
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PREDICTIONS_TABLE)
)

total     = predictions_spark_df.count()
positives = int(inference_pd["prediction"].sum())
print(f"Predictions written to : {PREDICTIONS_TABLE}")
print(f"Total records          : {total:,}")
print(f"Predicted seizures     : {positives:,} ({positives / total:.2%})")
```

### Step 13 — Monitor Prediction Distribution

```python
# Cell 13: log prediction distribution metrics to MLflow
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

with mlflow.start_run(run_name="Inference_Distribution_Monitor"):
    total_preds    = len(inference_pd)
    positive_preds = int(inference_pd["prediction"].sum())
    positive_rate  = positive_preds / total_preds if total_preds > 0 else 0.0

    mlflow.log_metrics({
        "total_predictions"  : total_preds,
        "positive_predictions": positive_preds,
        "positive_rate"      : positive_rate,
    })
    mlflow.log_param("model_version", model_version.version)

    counts = inference_pd["prediction"].value_counts().sort_index()
    labels = ["No Seizure (0)", "Seizure (1)"]
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.bar(labels[:len(counts)], counts.values, color=["#2196F3", "#F44336"][:len(counts)])
    ax.set_title("Batch Inference — Prediction Distribution")
    ax.set_ylabel("Record Count")
    for i, v in enumerate(counts.values):
        ax.text(i, v + max(counts.values) * 0.01, f"{v:,}", ha="center", fontsize=10)
    plt.tight_layout()
    mlflow.log_figure(fig, "inference_distribution.png")
    plt.close(fig)

print(f"Total predictions : {total_preds:,}")
print(f"Positive rate     : {positive_rate:.2%}")
```

---

## Section 12: Part 9 — Automated Retraining

### Step 14 — Implement a Scheduled Retraining Pipeline

```python
# Cell 14: automated retraining pipeline with champion/challenger comparison
def automated_retraining_pipeline(days_of_data: int = 30, f1_improvement_threshold: float = 0.01) -> None:
    """
    Retrain the model on recent data and promote only if F1 improves
    beyond f1_improvement_threshold over the current Production model.
    """
    with mlflow.start_run(run_name="Automated_Retraining") as retrain_run:
        # 1. Load recent data
        recent_pd = spark.sql(f"""
            SELECT f.*, g.has_seizure AS label
            FROM {FEATURE_TABLE} f
            INNER JOIN eeg_lakehouse.gold.eeg_features g USING (recording_id)
            WHERE f.processed_timestamp >= current_timestamp() - INTERVAL {days_of_data} DAYS
        """).toPandas()

        if len(recent_pd) < 100:
            print(f"Insufficient data for retraining ({len(recent_pd)} rows). Aborting.")
            return

        X_new = recent_pd[model_feature_cols]
        y_new = recent_pd["label"]
        X_tr, X_te, y_tr, y_te = train_test_split(X_new, y_new, test_size=0.2, random_state=42, stratify=y_new)

        # 2. Train challenger model
        challenger = RandomForestClassifier(
            n_estimators=200, max_depth=12, random_state=42, class_weight="balanced",
        )
        challenger.fit(X_tr, y_tr)
        challenger_f1 = f1_score(y_te, challenger.predict(X_te), zero_division=0)

        mlflow.log_metrics({
            "challenger_f1"   : challenger_f1,
            "training_samples": len(X_tr),
        })

        # 3. Evaluate champion model on the same test set
        try:
            champion = mlflow.sklearn.load_model(f"models:/{REGISTRY_MODEL}/Production")
            champion_f1 = f1_score(y_te, champion.predict(X_te), zero_division=0)
        except Exception:
            champion_f1 = 0.0

        mlflow.log_metric("champion_f1", champion_f1)

        print(f"Champion F1   : {champion_f1:.4f}")
        print(f"Challenger F1 : {challenger_f1:.4f}")

        # 4. Promote challenger if it improves F1 by the required margin
        if challenger_f1 > champion_f1 + f1_improvement_threshold:
            mlflow.sklearn.log_model(
                challenger,
                artifact_path="model",
                registered_model_name=REGISTRY_MODEL,
            )
            print(f"Challenger promoted to Production (F1 improved by {challenger_f1 - champion_f1:.4f}).")
        else:
            print("Champion retained. Challenger did not exceed the improvement threshold.")


automated_retraining_pipeline(days_of_data=30, f1_improvement_threshold=0.01)
```

---

## Section 13: Part 10 — End-to-End Verification

### Step 15 — Run the Pipeline Smoke Test

```python
# Cell 15: end-to-end pipeline smoke test
import sys

failures = []

# Test 1 — Feature table accessible
try:
    ft_count = spark.table(FEATURE_TABLE).count()
    assert ft_count > 0, "Feature table is empty."
    print(f"[PASS] Feature table '{FEATURE_TABLE}': {ft_count:,} rows")
except AssertionError as e:
    failures.append(f"Feature table: {e}")

# Test 2 — Production model loadable
try:
    pm = mlflow.pyfunc.load_model(f"models:/{REGISTRY_MODEL}/Production")
    assert pm is not None
    print(f"[PASS] Production model '{REGISTRY_MODEL}' loaded successfully")
except Exception as e:
    failures.append(f"Production model: {e}")

# Test 3 — Prediction schema correct
try:
    sample_pd = spark.table(FEATURE_TABLE).limit(5).toPandas()[model_feature_cols]
    sample_preds = pm.predict(sample_pd)
    assert len(sample_preds) == 5, f"Expected 5 predictions, got {len(sample_preds)}"
    assert set(sample_preds).issubset({0, 1}), "Predictions contain values outside {0, 1}"
    print(f"[PASS] Prediction schema: {sample_preds.tolist()}")
except Exception as e:
    failures.append(f"Prediction schema: {e}")

# Test 4 — Predictions table populated
try:
    pt_count = spark.table(PREDICTIONS_TABLE).count()
    assert pt_count > 0, "Predictions table is empty."
    print(f"[PASS] Predictions table '{PREDICTIONS_TABLE}': {pt_count:,} rows")
except AssertionError as e:
    failures.append(f"Predictions table: {e}")

# Test 5 — MLflow experiment has runs
try:
    exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
    runs = mlflow.search_runs(experiment_ids=[exp.experiment_id], max_results=1)
    assert len(runs) > 0, "No runs found in experiment."
    print(f"[PASS] MLflow experiment has {len(runs)}+ run(s)")
except AssertionError as e:
    failures.append(f"MLflow experiment: {e}")

if failures:
    print("\n[FAIL] The following checks failed:")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print("\nAll smoke tests passed.")
```

---

## Section 14: Exam Reference Tables

### MLflow Tracking — Key API Methods

| Method | Purpose | Exam consideration |
|---|---|---|
| `mlflow.set_experiment(path)` | Sets the active experiment by path | Path is workspace-relative for managed MLflow |
| `mlflow.start_run(run_name)` | Opens a new run context | Nested runs are created via `nested=True` |
| `mlflow.log_params(dict)` | Logs a dictionary of hyperparameters | Use for model configuration |
| `mlflow.log_metrics(dict)` | Logs numeric evaluation metrics | Can be called multiple times per run |
| `mlflow.log_figure(fig, path)` | Logs a Matplotlib/Plotly figure as an artifact | Path is relative to the run artifact root |
| `mlflow.sklearn.log_model(model, path, registered_model_name)` | Logs and optionally registers a sklearn model | `registered_model_name` triggers registry creation |
| `mlflow.search_runs(experiment_ids, order_by)` | Returns a pandas DataFrame of runs | Use `order_by=["metrics.f1_score DESC"]` to find the champion |

### Model Registry — Transition API

| Action | MlflowClient method | Notes |
|---|---|---|
| Register model version | `mlflow.register_model(uri, name)` | Returns `ModelVersion` object |
| Transition stage | `client.transition_model_version_stage(name, version, stage)` | Valid stages: `Staging`, `Production`, `Archived` |
| Archive previous versions | Pass `archive_existing_versions=True` | Prevents accumulation of active Production versions |
| Load from registry | `mlflow.pyfunc.load_model(f"models:/{name}/{stage}")` | `pyfunc` is framework-agnostic |

### Feature Store — Key Concepts for the Exam

| Concept | Key detail |
|---|---|
| Primary key | Must be declared at table creation; used for point-in-time lookups and merge writes |
| Timestamp key | Enables point-in-time feature retrieval to prevent training/serving skew |
| `FeatureLookup` | Declared in training; Feature Store replays the same logic during batch or online serving |
| `create_training_set` | Joins labels with feature lookups; produces a `TrainingSet` object |
| `load_df()` | Materialises the training set as a Spark DataFrame; call `.toPandas()` for sklearn |

### Certified Professional Exam Domain Mapping — Day 10 Topics

| Topic | Professional exam domain |
|---|---|
| Feature Store table creation and writes | Domain 5 — ML with Databricks |
| MLflow autologging for sklearn | Domain 5 — MLflow tracking |
| `GridSearchCV` with nested MLflow runs | Domain 5 — MLflow tracking |
| Model Registry lifecycle transitions | Domain 5 — Model Registry |
| Batch inference with `pyfunc` | Domain 5 — Model deployment |
| Champion/challenger retraining pattern | Domain 5 — ML workflow automation |
| Feature engineering with Window functions | Domain 2 — ELT with Apache Spark |

---

## Section 15: Self-Check Questions

1. What is the difference between `mlflow.sklearn.log_model()` and `mlflow.register_model()`?
2. Why must a Feature Store table declare a `timestamp_key` for EEG data?
3. What stage must a model be in before it can be transitioned to `Production`?
4. How does `archive_existing_versions=True` affect the Model Registry when promoting a new version to Production?
5. In the retraining pipeline, why is the champion model evaluated on the **challenger's** test split rather than a fixed historical hold-out?
6. What is the purpose of the `pyfunc` model flavour in batch inference?

**Reference answers:**

1. `mlflow.sklearn.log_model()` serialises and saves the model as an artifact inside a run. `mlflow.register_model()` creates a versioned entry in the Model Registry pointing to an artifact URI; the two calls are often chained but are independent operations.
2. The `timestamp_key` enables point-in-time feature retrieval during training set construction, preventing feature leakage by ensuring only features that were available at the exact recording timestamp are joined to each label.
3. There is no strict requirement to pass through `Staging` before `Production`; the stage transition API allows direct transitions. However, the recommended governance pattern promotes to `Staging` first for validation before promoting to `Production`.
4. Setting `archive_existing_versions=True` automatically moves all other versions that are currently in the target stage (`Production`) to `Archived`, ensuring only one version is active at a time.
5. Evaluating both champion and challenger on the same held-out slice from the challenger's recent data ensures a fair comparison on the distribution of current data, rather than penalising the challenger for a distribution shift that the champion was originally trained on.
6. The `pyfunc` flavour provides a unified `predict()` interface regardless of the underlying framework (sklearn, XGBoost, TensorFlow, etc.), enabling framework-agnostic batch inference code that requires no changes when the model implementation changes.

---

## Section 16: Day 10 Summary

| Artifact | Tool | Medallion layer | Exam domain |
|---|---|---|---|
| Engineered feature DataFrame | PySpark Window functions | Gold → Feature Store | Domain 2 |
| `eeg_ml_features` Feature Store table | Databricks Feature Store | Feature Store | Domain 5 |
| MLflow experiment with three classifier runs | MLflow autologging | — | Domain 5 |
| Random Forest GridSearch run | MLflow + sklearn | — | Domain 5 |
| Feature importance chart | Matplotlib + MLflow artifact | — | Domain 5 |
| Registered model — `eeg_seizure_production` | MLflow Model Registry | — | Domain 5 |
| Staging → Production promotion | `MlflowClient.transition_model_version_stage` | — | Domain 5 |
| `seizure_predictions` batch inference table | `mlflow.pyfunc` + Delta write | Gold/Predictions | Domain 5 |
| Champion/challenger retraining pipeline | MLflow run + registry update | — | Domain 5 |
| Smoke test suite | Python assertions | All layers | Domain 5 |

**Next**: Day 11 covers real-time streaming inference, Structured Streaming with Delta Live Tables, and Kafka/Auto Loader event-driven architectures for continuous EEG signal processing.
