# =============================================================================
# src/gold/train_ml_model.py
# Day 10: MLflow Training - XGBoost + SHAP on EEG Gold Features
# =============================================================================
"""
Train a binary classifier to predict memory consolidation (high/low spindle
density proxy) from Gold EEG features. Logs everything to MLflow:
  - Parameters (hyperparameters)
  - Metrics (AUC-ROC, F1, accuracy per fold)
  - Model artifact (XGBoost + sklearn pipeline)
  - SHAP feature importance plot
  - Gold table version used (for reproducibility)

Databricks exam relevance:
  MLflow autologging, model registry, run tagging, artifact logging.
Research relevance:
  H3 test: does spindle density predict memory score? (Delta R2 > 0.10)
"""

from __future__ import annotations

import json
import logging
import warnings
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Feature / target configuration
# ---------------------------------------------------------------------------
FEATURE_COLS: List[str] = [
    "spindle_density",
    "spindle_duration_mean",
    "spindle_amplitude_mean",
    "so_density",
    "so_neg_peak_mean",
    "pac_mi",
    "sigma_power_mean",
    "delta_power_mean",
    "beta_power_mean",
    "theta_power_mean",
]
TARGET_COL: str = "memory_score"


def load_gold_features(spark, table_name: str) -> pd.DataFrame:
    """
    Load Gold feature table from Delta into a pandas DataFrame.
    Returns only complete rows (no nulls in FEATURE_COLS).
    """
    df = (
        spark.table(table_name)
        .select(FEATURE_COLS + [TARGET_COL])
        .dropna()
        .toPandas()
    )
    logger.info("Loaded %d rows from %s", len(df), table_name)
    return df


def create_mock_gold_data(n: int = 200, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic Gold feature data for local testing.
    Mirrors the real Gold schema so the full pipeline is testable offline.
    """
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({
        "spindle_density":         rng.uniform(0.5, 8.0, n),
        "spindle_duration_mean":   rng.uniform(0.5, 1.5, n),
        "spindle_amplitude_mean":  rng.uniform(20, 80, n),
        "so_density":              rng.uniform(0.2, 3.0, n),
        "so_neg_peak_mean":        rng.uniform(-120, -40, n),
        "pac_mi":                  rng.uniform(0.0, 2.0, n),
        "sigma_power_mean":        rng.uniform(0.1, 0.8, n),
        "delta_power_mean":        rng.uniform(0.3, 0.9, n),
        "beta_power_mean":         rng.uniform(0.05, 0.3, n),
        "theta_power_mean":        rng.uniform(0.1, 0.5, n),
    })
    # memory_score = 1 if spindle_density AND sigma_power above median
    med_sp = df["spindle_density"].median()
    med_si = df["sigma_power_mean"].median()
    df[TARGET_COL] = (
        (df["spindle_density"] > med_sp) & (df["sigma_power_mean"] > med_si)
    ).astype(int)
    return df


def train_and_log(
    df: pd.DataFrame,
    experiment_name: str = "/Shared/eeg_memory_prediction",
    n_folds: int = 5,
    xgb_params: Dict | None = None,
) -> str:
    """
    Train XGBoost with stratified k-fold CV, log to MLflow, return run_id.

    Steps
    -----
    1. mlflow.set_experiment() - creates or reuses experiment
    2. mlflow.start_run() - creates a new run
    3. mlflow.log_params() - hyperparameters
    4. Cross-validation -> mlflow.log_metrics() per fold
    5. Final fit on full data -> mlflow.sklearn.log_model()
    6. SHAP feature importance -> mlflow.log_figure()
    7. mlflow.set_tags() - research metadata
    """
    try:
        import mlflow
        import mlflow.sklearn
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        from xgboost import XGBClassifier
    except ImportError as e:
        logger.error("Missing dependency: %s. Install requirements.txt", e)
        raise

    if xgb_params is None:
        xgb_params = {
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "use_label_encoder": False,
            "eval_metric": "logloss",
            "random_state": 42,
        }

    X = df[FEATURE_COLS].values
    y = df[TARGET_COL].values

    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="xgb_eeg_memory") as run:
        # --- log params ---
        mlflow.log_params(xgb_params)
        mlflow.log_param("n_folds", n_folds)
        mlflow.log_param("n_features", len(FEATURE_COLS))
        mlflow.log_param("n_samples", len(df))
        mlflow.log_param("target_col", TARGET_COL)

        # --- cross-validation ---
        pipe = Pipeline([
            ("scaler", StandardScaler()),
            ("clf", XGBClassifier(**xgb_params)),
        ])
        cv = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=42)
        auc_scores = cross_val_score(pipe, X, y, cv=cv, scoring="roc_auc")
        f1_scores  = cross_val_score(pipe, X, y, cv=cv, scoring="f1")
        acc_scores = cross_val_score(pipe, X, y, cv=cv, scoring="accuracy")

        mlflow.log_metric("cv_auc_mean",  float(np.mean(auc_scores)))
        mlflow.log_metric("cv_auc_std",   float(np.std(auc_scores)))
        mlflow.log_metric("cv_f1_mean",   float(np.mean(f1_scores)))
        mlflow.log_metric("cv_acc_mean",  float(np.mean(acc_scores)))
        for i, (auc, f1, acc) in enumerate(zip(auc_scores, f1_scores, acc_scores)):
            mlflow.log_metric("fold_auc", float(auc), step=i)
            mlflow.log_metric("fold_f1",  float(f1),  step=i)

        logger.info("CV AUC: %.3f +/- %.3f", np.mean(auc_scores), np.std(auc_scores))

        # --- final fit on full data ---
        pipe.fit(X, y)
        mlflow.sklearn.log_model(
            pipe,
            artifact_path="model",
            registered_model_name="eeg_memory_predictor",
            input_example=df[FEATURE_COLS].head(3),
        )

        # --- SHAP feature importance ---
        try:
            import shap
            import matplotlib.pyplot as plt
            xgb_model = pipe.named_steps["clf"]
            explainer = shap.TreeExplainer(xgb_model)
            X_scaled = pipe.named_steps["scaler"].transform(X)
            shap_values = explainer.shap_values(X_scaled)
            fig, ax = plt.subplots(figsize=(8, 6))
            shap.summary_plot(shap_values, X_scaled,
                              feature_names=FEATURE_COLS, show=False)
            mlflow.log_figure(fig, "shap_summary.png")
            plt.close(fig)
        except Exception as shap_err:
            logger.warning("SHAP plot failed: %s", shap_err)

        # --- research tags ---
        mlflow.set_tags({
            "project": "eeg-tda-memory",
            "hypothesis": "H3-spindle-density-predicts-memory",
            "dataset": "Sleep-EDF-Expanded",
            "layer": "gold",
            "author": "wang-yuhao",
        })

        run_id = run.info.run_id
        logger.info("MLflow run_id: %s", run_id)
        return run_id


def load_best_model(run_id: str):
    """Load the registered model from a specific MLflow run."""
    try:
        import mlflow.sklearn
        model_uri = f"runs:/{run_id}/model"
        return mlflow.sklearn.load_model(model_uri)
    except Exception as e:
        logger.error("Failed to load model: %s", e)
        raise
