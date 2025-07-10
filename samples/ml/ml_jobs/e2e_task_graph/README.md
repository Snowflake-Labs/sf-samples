# End-to-End ML Pipeline with Snowflake Task Graphs

This example demonstrates how to build a complete machine learning pipeline using [Snowflake ML Jobs](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview)
and [Snowflake Task Graphs](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/snowflake-python-managing-tasks).
The pipeline includes data preparation, model training, evaluation, conditional promotion, and cleanup — all automated and scheduled using a Task Graph.

## Prerequisites

### Snowflake Account Setup

Work with your account administrator to provision the required resources in your Snowflake account as needed.

> NOTE: The steps below use role name `ENGINEER`. Replace this with the role name you will be using to
  work through the example.

1. Create a compute pool if you don't already have one:

```sql
CREATE COMPUTE POOL IF NOT EXISTS DEMO_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_S;
GRANT USAGE ON COMPUTE POOL TO ROLE ENGINEER;
```

Note: `MAX_NODES` should be at least equal to `target_instances` (2 in this example).

2. Create a virtual warehouse if you don't already have one:

```sql
CREATE WAREHOUSE IF NOT EXISTS DEMO_WH;  -- Default settings are fine
GRANT USAGE ON WAREHOUSE DEMO_WH TO ROLE ENGINEER;
```

3. Configure database privileges for the demo. Subsequent steps will create resources inside this database.

```sql
-- OPTIONAL: Create a separate database for easy cleanup
CREATE DATABASE IF NOT EXISTS SNOWBANK;

GRANT USAGE ON DATABASE SNOWBANK TO ROLE ENGINEER;
GRANT CREATE SCHEMA ON DATABASE SNOWBANK TO ROLE ENGINEER;
```

4. Grant other required [access control privileges](https://docs.snowflake.com/en/user-guide/security-access-control-privileges)
  needed to execute the sample:

```sql
EXECUTE TASK ON ACCOUNT TO ROLE ENGINEER;
```

5. (Optional) Configure a [notification integration](https://docs.snowflake.com/en/user-guide/notifications/webhook-notifications)
  to enable [sending notifications](https://docs.snowflake.com/en/user-guide/notifications/snowflake-notifications)
  from Task Graph executions.
   1. Create a webhook with your desired notification channel (e.g. [Slack Webhook](https://api.slack.com/messaging/webhooks))
   2. Configure notification integration with your webhook

    ```sql
    CREATE SECRET IF NOT EXISTS DEMO_WEBHOOK_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = 'T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX'; -- (ACTION NEEDED) Put your webhook secret here
    CREATE NOTIFICATION INTEGRATION IF NOT EXISTS DEMO_WEBHOOK_INTEGRATION
    TYPE=WEBHOOK
    ENABLED=TRUE
    WEBHOOK_URL='https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET=DEMO_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE='{"text": "SNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS=('Content-Type'='application/json');
    GRANT USAGE ON INTEGRATION DEMO_WEBHOOK_INTEGRATION TO ROLE ENGINEER;
    ```

### Local Setup

1. All steps assume your working directory is the `e2e_task_graph/` folder

    ```bash
    cd samples/ml/ml_jobs/e2e_task_graph
    ```

2. Configure your default Snowflake connection following the [connection configuration guide](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections)
3. Set up your development environments using the [setup_env.sh](scripts/setup_env.sh) helper script.
    The script will create a new virtual environment (if needed), install all the necessary Python packages,
    and create the necessary resources in your default Snowflake account from step 2.

    ```bash
    bash scripts/setup_env.sh -r ENGINEER  # Change ENGINEER to your role name
    ```

    > Modify `-r ENGINEER` to match the role name used in [Snowflake Account Setup](#snowflake-account-setup)

4. Update the values in [constants.py](src/constants.py) to match your Snowflake environment as configured
    in [Snowflake Account Setup](#snowflake-account-setup) and any modifications made to step 3 above.

## How to Run

### Standalone Pipeline (Local Testing)

[model_pipeline.py](src/model_pipeline.py) provides the core ML pipeline and can be executed locally
for testing purposes.

Run the ML pipeline locally without task graph orchestration:

```bash
python src/model_pipeline.py
python src/model_pipeline.py --no-register  # Skip model registration for faster experimentation
```

### Task Graph Orchestration

[model_dag.py](src/model_dag.py) contains the Task Graph definition and can be used to trigger
[one-off executions](#one-time-execution) or [scheduled runs](#scheduled-execution).

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

The `train_model` function uses the `@remote` decorator to run multi-node training on Snowpark Container Services:

```python
@remote(COMPUTE_POOL, stage_name=JOB_STAGE, target_instances=2)
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