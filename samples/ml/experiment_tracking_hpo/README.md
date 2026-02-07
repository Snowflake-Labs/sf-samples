# Distributed Hyperparameter Optimization and Experiment Tracking in Snowflake

This example demonstrates how to combine two integrated Snowflake ML capabilities:

- **Distributed Hyperparameter Optimization (HPO)** – Run model tuning in parallel on Snowpark Container Runtime  
- **Experiment Tracking** – Automatically log parameters, metrics, and model artifacts for every run  

Together, these tools let you move from one-off experiments to scalable, reproducible ML workflows — all within Snowflake.

---

## Overview

**Challenges addressed**
- Sequential hyperparameter tuning is slow  
- Manual experiment tracking is error-prone  
- Distributed infrastructure setup is complex  
- Reproducing past experiments requires detailed documentation  

**What you’ll learn**
- Setting up experiment tracking for ML runs  
- Running distributed HPO across multiple nodes  
- Logging and comparing experiment results  
- Viewing experiment history in Snowsight  

---

## Example Flow

### 1. Define the Training Function

Each HPO trial trains a model and logs its run.

```python
def train_function():
    tuner_context = tune.get_tuner_context()
    params = tuner_context.get_hyper_params()

    exp = ExperimentTracking(session=get_active_session())
    exp.set_experiment("Wine_Quality_Classification")

    with exp.start_run():
        exp.log_params(params)
        model = XGBClassifier(**params)
        model.fit(X_train, y_train)
        
        y_val_pred = model.predict(X_val)
        y_val_proba = model.predict_proba(X_val)[:, 1]
        
        val_metrics = {
            "val_accuracy": metrics.accuracy_score(y_val, y_val_pred),
            "val_precision": metrics.precision_score(y_val, y_val_pred, zero_division=0),
            "val_recall": metrics.recall_score(y_val, y_val_pred, zero_division=0),
            "val_f1": metrics.f1_score(y_val, y_val_pred, zero_division=0),
            "val_roc_auc": metrics.roc_auc_score(y_val, y_val_proba)
        }
        
        exp.log_metrics(val_metrics)
        tuner_context.report(metrics=val_metrics, model=model)
```
---

### 2. Configure the Search Space

```python
search_space = {
    "n_estimators": tune.randint(50, 300),
    "max_depth": tune.randint(3, 15),
    "learning_rate": tune.loguniform(0.01, 0.3),
    "subsample": tune.uniform(0.5, 1.0),
    "colsample_bytree": tune.uniform(0.5, 1.0)
}

tuner_config = tune.TunerConfig(
    metric="f1_score",
    mode="max",
    search_alg=RandomSearch(),
    num_trials=50
)
```
---

### 3. Run Distributed Tuning

```python
tuner = tune.Tuner(
    train_func=train_function,
    search_space=search_space,
    tuner_config=tuner_config
)

scale_cluster(10)  # Scale out to multiple nodes
results = tuner.run(dataset_map=dataset_map)
```
Each container runs one trial in parallel and logs metrics to Experiment Tracking.

---

### 4. View and Compare Results

- In **Snowsight → AI & ML → Experiments**, select the experiment to:
  - Compare runs
  - View metrics and charts
  - Inspect logged models and artifacts

---

## Prerequisites

- Snowflake account with a database and schema
- CREATE EXPERIMENT privilege on your schema
- snowflake-ml-python >= 1.9.1
- Notebook configured for Container Runtime on SPCS (Compute Pool with instance type `CPU_X64_S`)

---

## Resources

- https://docs.snowflake.com/en/developer-guide/snowpark-ml/overview
- https://docs.snowflake.com/en/developer-guide/snowpark-ml/experiment-tracking
- https://docs.snowflake.com/en/developer-guide/snowpark-ml/container-hpo

---

## License

Provided as-is for educational use.
