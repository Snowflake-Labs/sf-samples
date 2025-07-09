# End-to-End ML Pipeline with Snowflake Task Graphs

This example demonstrates how to build a complete machine learning pipeline using [Snowflake ML Jobs](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview)
and [Snowflake Task Graphs](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/snowflake-python-managing-tasks).
The pipeline includes data preparation, model training, evaluation, conditional promotion, and cleanup — all automated and scheduled using a Task Graph.

## What you'll learn

- How to create ML pipelines with **Snowflake ML Jobs** for distributed training
- How to orchestrate workflows using **Snowflake Task Graphs** with the Python API
- How to implement conditional logic in task graphs using **DAGTaskBranch**
- How to integrate with **Snowflake Model Registry** for model lifecycle management
- How to build production-ready ML workflows with automatic cleanup and monitoring

## Architecture Overview

The pipeline consists of two main components:

1. [model_pipeline.py](src/model_pipeline.py) - Core ML pipeline that can run standalone or as part of a task graph
2. [model_dag.py](src/model_dag.py) - Task graph orchestration using Snowflake's Python API

### Task Graph Structure

```
prepare_data → train_model → check_model_quality → promote_model (if quality >= threshold)
                                                 → send_alert (if quality < threshold)
cleanup_task (finalizer - always runs)
```

## Prerequisites

1. All steps assume your working directory is the `e2e_task_graph/` folder
    ```bash
    cd samples/ml/ml_jobs/e2e_task_graph
    ```
2. Configure your default Snowflake connection following the [connection configuration guide](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections)
3. Set up your development environments using the [setup_env.sh](scripts/setup_env.sh) helper script.
    The script will create a new virtual environment (if needed), install all the necessary Python packages,
    and create the necessary resources in your default Snowflake account from step 2.
    ```bash
    bash scripts/setup_env.sh
    ```

## Running the Pipeline

### Standalone Pipeline (Local Testing)

Run the ML pipeline locally without task graph orchestration:

```bash
python src/model_pipeline.py
python src/model_pipeline.py --no-register  # Skip model registration for faster experimentation
```

### Task Graph Orchestration

#### One-Time Execution

Deploy and immediately execute the task graph:

```bash
python src/model_dag.py --run-dag
```

The script will:
- Deploy the task graph to Snowflake
- Execute it immediately
- Monitor execution progress
- Display the final status

#### Scheduled Execution

Deploy the task graph with a recurring schedule:

```bash
python src/model_dag.py --schedule 1d   # Daily execution
python src/model_dag.py --schedule 12h  # Every 12 hours
python src/model_dag.py --schedule 30m  # Every 30 minutes
```

## Key Features

### Distributed Training with ML Jobs
The `train_model` function uses the `@remote` decorator to execute training on Snowpark Container Services:

```python
@remote(COMPUTE_POOL, stage_name=JOB_STAGE)
def train_model(session: Session, input_data: DataSource) -> XGBClassifier:
    # Training logic runs on distributed compute
```

### Conditional Model Promotion
The task graph includes branching logic that only promotes models meeting quality thresholds:

```python
def check_model_quality(session: Session) -> str:
    if metrics[config.metric_name] >= threshold:
        return "promote_model"  # High quality → promote
    else:
        return "send_alert"     # Low quality → alert
```

### Model Registry Integration
Successful models are automatically registered and promoted to production:

```python
mv = register_model(session, model, model_name, version, train_ds, metrics)
promote_model(session, mv)  # Sets as default version
```

### Automatic Cleanup
The pipeline includes a finalizer task that removes obsolete artifacts:

```python
cleanup_task = DAGTask("cleanup_task", definition=cleanup, is_finalizer=True)
```

## Task Graph Concepts

This example demonstrates key [Snowflake Task Graph](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/snowflake-python-managing-tasks) concepts:

- **DAG Creation**: Using `DAG` context manager to define workflow structure
- **Task Dependencies**: Using `>>` operator to define execution order
- **Branching Logic**: Using `DAGTaskBranch` for conditional execution paths
- **Task Context**: Accessing configuration and predecessor outputs via `TaskContext`
- **Finalizer Tasks**: Ensuring cleanup always runs regardless of success/failure

## Next Steps

- Modify the model training logic in `train_model()` for your use case
- Adjust quality thresholds and metrics in the DAG configuration
- Add notification integrations for the alert task
- Customize the feature engineering pipeline in `prepare_datasets()`
- Scale compute resources by modifying the compute pool configuration

For more information on Snowflake Task Graphs, see the [Python API documentation](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/snowflake-python-managing-tasks).