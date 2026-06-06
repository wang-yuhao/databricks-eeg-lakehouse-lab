# Day 10 – MLflow: Experiment Tracking, Model Registry & Feature Store

## Overview
MLflow is the end-to-end ML lifecycle platform deeply integrated into Databricks.
Today covers experiment tracking, the Model Registry, Feature Store, and deploying
models as batch/streaming/REST endpoints. Key Databricks DE exam topic.

---

## Core Concepts

### 1. Experiment Tracking
```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier

mlflow.set_experiment("/Users/you@company.com/eeg-sleep-staging")

with mlflow.start_run(run_name="rf_spindle_classifier_v1"):
    # Log parameters
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("features", "spindle_density,so_coupling,sleep_efficiency")

    # Train
    model = RandomForestClassifier(n_estimators=100, max_depth=10)
    model.fit(X_train, y_train)

    # Log metrics
    mlflow.log_metric("accuracy", 0.87)
    mlflow.log_metric("f1_macro", 0.84)

    # Log model
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        registered_model_name="eeg-sleep-stage-classifier"
    )

    # Log artefacts
    mlflow.log_artifact("confusion_matrix.png")
```

### 2. Model Registry
```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Transition model to Staging
client.transition_model_version_stage(
    name="eeg-sleep-stage-classifier",
    version=3,
    stage="Staging",
    archive_existing_versions=True,
)

# Promote to Production
client.transition_model_version_stage(
    name="eeg-sleep-stage-classifier",
    version=3,
    stage="Production",
)

# Load production model
model = mlflow.sklearn.load_model("models:/eeg-sleep-stage-classifier/Production")
```

**Model stages:** None → Staging → Production → Archived

### 3. `mlflow.pyfunc` for Custom Models
```python
class EEGSleepStager(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        import joblib
        self.model = joblib.load(context.artifacts["model_path"])

    def predict(self, context, model_input):
        return self.model.predict(model_input)

mlflow.pyfunc.log_model(
    artifact_path="eeg_stager",
    python_model=EEGSleepStager(),
    artifacts={"model_path": "model.pkl"},
    registered_model_name="eeg-custom-stager",
)
```

### 4. Batch Scoring with Spark
```python
import mlflow.spark

# Load model as Spark UDF
predict_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/eeg-sleep-stage-classifier/Production",
    result_type="string"
)

# Apply to Gold feature table
scored = gold_features.withColumn(
    "predicted_sleep_stage",
    predict_udf(*feature_columns)
)
```

### 5. Auto ML & Hyperparameter Tuning
```python
# Hyperopt for distributed hyperparameter search
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from pyspark.ml.tuning import CrossValidator

def objective(params):
    with mlflow.start_run(nested=True):
        mlflow.log_params(params)
        model = RandomForestClassifier(**params)
        score = cross_val_score(model, X_train, y_train, cv=5).mean()
        mlflow.log_metric("cv_accuracy", score)
        return {"loss": -score, "status": STATUS_OK}

best = fmin(fn=objective, space=search_space, algo=tpe.suggest, max_evals=20)
```

---

## EEG / Neuroscience Context

### ML Pipeline for Sleep Staging
1. **Features (Gold):** spindle density, SO-spindle coupling, spectral power bands.
2. **Labels:** Manual sleep staging (PSG gold standard: AASM guidelines).
3. **Model:** Random Forest or LSTM for epoch classification.
4. **Tracking:** Each experiment run logs subject cohort, feature set, and model version.
5. **Registry:** Production model used for automated batch scoring of new nights.

### EEG-Specific MLflow Artefacts
- Confusion matrix per sleep stage (Wake/N1/N2/N3/REM)
- Cohen's Kappa score (inter-rater reliability metric for sleep staging)
- Per-subject accuracy breakdown to detect subject-specific drift

---

## Exam-Focused Summary

| Topic | Key Fact |
|---|---|
| `mlflow.start_run()` | Context manager for logging params/metrics/artefacts |
| `log_model` | Saves model with MLflow flavour (sklearn, spark, pyfunc) |
| Model stages | None → Staging → Production → Archived |
| `spark_udf` | Load registered model for batch scoring on DataFrames |
| `nested=True` | Required for hyperparameter search runs inside parent run |
| Feature Store | Centralised feature repo; point-in-time correct lookups |

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `notebooks/day10_mlflow.py` | End-to-end MLflow training and registry notebook |
| `src/ml/sleep_stager.py` | `EEGSleepStager` pyfunc model class |
| `tests/test_mlflow.py` | MLflow logging and registry tests |

---

## Self-Check Questions
1. What is the difference between `log_metric` and `log_param`?
2. How do you load a Production-stage model for batch scoring?
3. What does `archive_existing_versions=True` do when transitioning a model?
4. Why is Cohen's Kappa preferable to accuracy for sleep staging evaluation?
5. How would you use `mlflow.spark_udf` to score a large Gold features table?
6. What is a nested run and when would you use one?

---

## Further Reading
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Databricks MLflow Guide](https://docs.databricks.com/en/mlflow/index.html)
- Rechtschaffen, A. & Kales, A. (1968). *A Manual of Standardized Terminology, Techniques and Scoring System for Sleep Stages of Human Subjects.*
