## Prerequisites

1. All steps here assume your working directory is the `dag/` folder
    ```bash
    cd dag
    ```
2. Install necessary Python packages
    ```bash
    pip install -r requirements.txt
    ```
3. Set up default Snowflake connection: [Configure Connections](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections)
4. Run the setup commands in [setup_env.sh](scripts/setup_env.sh)

## (Optional) Run model pipeline locally

```bash
python src/model_pipeline.py
```

## Create the Task Graph in Snowflake

### Ad Hoc execution

Use the `--run-dag` argument to trigger a one-off DAG execution. The script will monitor the DAG execution and display the final execution status.

```bash
python src/model_dag.py --run-dag
```

### Scheduled Executions

Use the `--schedule` argument to configured scheduled DAG executions.

```bash
python src/model_dag.py --schedule 1d  # Daily runs
python src/model_dag.py --schedule 12h # Run every 12 hours
```