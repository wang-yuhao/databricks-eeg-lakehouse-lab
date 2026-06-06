# Databricks notebook source
# Day 10: MLflow XGBoost Training + SHAP — EEG Memory Predictor
# ================================================================
# Exam domains covered:
#   - MLflow experiment tracking (autolog, manual logging)
#   - Model registry: register, stage transitions, load for inference
#   - Feature importance with SHAP
#   - H3 hypothesis test for memory consolidation proxy
#
# Research context:
#   Trains an XGBoost classifier on Gold EEG features to predict
#   memory consolidation proxy (high/low SWS ratio).  SHAP values
#   reveal which spectral features drive predictions — directly
#   testable against the H3 hypothesis in the research proposal.
#
# Databricks exam tip:
#   mlflow.xgboost.autolog() logs params, metrics, model, and feature
#   importance automatically.  For custom metrics, use mlflow.log_metric().

# COMMAND ----------

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import shap
import xgboost as xgb
from mlflow.models.signature import infer_signature
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import LabelEncoder

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Gold Feature Table

# COMMAND ----------

CATALOG = "eeg_catalog"
GOLD_TABLE = f"{CATALOG}.gold.subject_features"

gold_df = spark.table(GOLD_TABLE).toPandas()
print(f"Loaded {len(gold_df)} subjects from {GOLD_TABLE}")
gold_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering & Label Creation
# MAGIC
# MAGIC **H3 hypothesis:** Subjects with high SWS ratio (>= median) have better
# MAGIC memory consolidation.  We create a binary label as proxy.

# COMMAND ----------

FEATURE_COLS = [
    "mean_delta_power",
    "mean_theta_power",
    "mean_alpha_power",
    "mean_delta_theta_ratio",
    "mean_spindle_density",
    "total_spindle_count",
    "sws_ratio",
    "n3_epochs",
    "rem_epochs",
]

# Binary label: high SWS ratio = likely better memory consolidation
median_sws = gold_df["sws_ratio"].median()
gold_df["memory_proxy"] = (gold_df["sws_ratio"] >= median_sws).astype(int)

X = gold_df[FEATURE_COLS].fillna(0)
y = gold_df["memory_proxy"]

print(f"Class distribution:\n{y.value_counts()}")
print(f"SWS ratio median threshold: {median_sws:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MLflow Experiment Setup

# COMMAND ----------

EXPERIMENT_NAME = "/Users/wang-yuhao/eeg-memory-consolidation"
mlflow.set_experiment(EXPERIMENT_NAME)
print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train XGBoost with MLflow Autolog

# COMMAND ----------

# Enable autologging — logs params, metrics, model artifact, feature importance
mlflow.xgboost.autolog(log_input_examples=True, log_model_signatures=True)

XGB_PARAMS = {
    "n_estimators": 200,
    "max_depth": 4,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "use_label_encoder": False,
    "eval_metric": "logloss",
    "random_state": 42,
}

with mlflow.start_run(run_name="xgb_eeg_memory_v1") as run:
    model = xgb.XGBClassifier(**XGB_PARAMS)

    # Cross-validation
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")

    mlflow.log_metric("cv_roc_auc_mean", cv_scores.mean())
    mlflow.log_metric("cv_roc_auc_std", cv_scores.std())
    mlflow.log_param("sws_median_threshold", median_sws)
    mlflow.log_param("n_subjects", len(gold_df))
    mlflow.log_param("n_features", len(FEATURE_COLS))

    # Full fit for registration
    model.fit(X, y)

    # Manual metrics on full training set (for registry validation)
    y_pred_proba = model.predict_proba(X)[:, 1]
    train_auc = roc_auc_score(y, y_pred_proba)
    mlflow.log_metric("train_roc_auc", train_auc)

    # Log model with signature
    signature = infer_signature(X, model.predict(X))
    mlflow.xgboost.log_model(
        model,
        artifact_path="eeg_memory_model",
        signature=signature,
        registered_model_name="eeg-memory-consolidation-xgb",
    )

    run_id = run.info.run_id
    print(f"Run ID: {run_id}")
    print(f"CV ROC-AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    print(f"Train ROC-AUC: {train_auc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SHAP Feature Importance
# MAGIC
# MAGIC SHAP values explain WHICH features drive each prediction —
# MAGIC key for validating H3 (spindle density and delta power should rank highest).

# COMMAND ----------

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X)

# Summary plot (in notebook: renders inline)
shap.summary_plot(shap_values, X, plot_type="bar", show=False)

# Log SHAP importance as MLflow metric
shap_importance = pd.DataFrame({
    "feature": FEATURE_COLS,
    "mean_abs_shap": np.abs(shap_values).mean(axis=0),
}).sort_values("mean_abs_shap", ascending=False)

print("\nSHAP Feature Importance (H3 validation):")
print(shap_importance.to_string(index=False))

with mlflow.start_run(run_id=run_id):
    for _, row in shap_importance.iterrows():
        mlflow.log_metric(f"shap_{row['feature']}", row["mean_abs_shap"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Model Registry — Stage Transitions
# MAGIC
# MAGIC **Exam pattern:** Register → Staging → Production → Archived
# MAGIC Use `MlflowClient` for programmatic stage management.

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
MODEL_NAME = "eeg-memory-consolidation-xgb"

# Get latest version
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Latest model version: {latest_version}")

# Promote to Staging
client.transition_model_version_stage(
    name=MODEL_NAME,
    version=latest_version,
    stage="Staging",
    archive_existing_versions=False,
)
print(f"Model v{latest_version} promoted to Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Load Model from Registry for Inference

# COMMAND ----------

# Load from Staging for batch inference
model_uri = f"models:/{MODEL_NAME}/Staging"
loaded_model = mlflow.xgboost.load_model(model_uri)

predictions = loaded_model.predict(X)
prediction_proba = loaded_model.predict_proba(X)[:, 1]

results_df = gold_df[["subject_id", "sws_ratio"]].copy()
results_df["predicted_label"] = predictions
results_df["memory_proxy_prob"] = prediction_proba.round(4)
results_df["predicted_consolidation"] = results_df["predicted_label"].map(
    {1: "HIGH", 0: "LOW"}
)

print("Inference results sample:")
print(results_df.head(10).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. H3 Hypothesis Test Summary
# MAGIC
# MAGIC **H3:** Topological features (Betti numbers, persistence entropy) from
# MAGIC sleep EEG networks predict memory consolidation better than classical
# MAGIC spectral features alone.
# MAGIC
# MAGIC **This notebook (Day 10) baseline:**
# MAGIC - Classical features only: `mean_delta_power`, `spindle_density`, `sws_ratio`
# MAGIC - Baseline ROC-AUC from CV will serve as comparison point for TDA-enriched model

# COMMAND ----------

print("=" * 60)
print("H3 BASELINE RESULT (classical spectral features only)")
print("=" * 60)
print(f"  CV ROC-AUC (5-fold): {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
print(f"  Top SHAP feature: {shap_importance.iloc[0]['feature']}")
print(f"  Model registered as: {MODEL_NAME} v{latest_version} (Staging)")
print()
print("Next step: Add TDA Betti/persistence features and re-run to test H3")
print("See: docs/research/ for TDA feature extraction plan")
