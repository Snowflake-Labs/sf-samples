import json
import time
from dataclasses import asdict
from datetime import timedelta
from typing import Any, Optional
import uuid

import cloudpickle as cp
from snowflake.core import CreateMode, Root
from snowflake.core.task.context import TaskContext
from snowflake.core.task.dagv1 import DAG, DAGOperation, DAGTask, DAGTaskBranch
from snowflake.ml.data import DatasetInfo
from snowflake.ml.dataset import load_dataset
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark import Session
from snowflake.ml.jobs import remote
import modeling
import data
import ops
import run_config

import cli_utils
from constants import (COMPUTE_POOL, DAG_STAGE, DATA_TABLE_NAME, DB_NAME, JOB_STAGE, SCHEMA_NAME,
                       WAREHOUSE)

session = Session.builder.getOrCreate()
def ensure_environment(session: Session):
    """
    Ensure the environment is properly set up for DAG execution.

    This function sets up the necessary environment by calling the shared
    ensure_environment function, creating the raw data table if it doesn't exist,
    and registering local modules for inclusion in ML Job payloads.

    Args:
        session (Session): Snowflake session object
    """
    modeling.ensure_environment(session)
    cp.register_pickle_by_value(modeling)

    # Ensure the raw data table exists
    _ = data.get_raw_data(session, DATA_TABLE_NAME, create_if_not_exists=True)

    # Register local modules for inclusion in ML Job payloads


def _wait_for_run_to_complete(session: Session, dag: DAG) -> str:
    """
    Wait for a DAG run to complete and return the final status.

    This function monitors the most recent DAG run and waits for it to complete.
    It uses exponential backoff to poll the task graph status and returns the final result.

    Args:
        session (Session): Snowflake session object
        dag (DAG): The DAG object to monitor

    Returns:
        str: The final status of the DAG run (e.g., "SUCCEEDED", "FAILED")

    Raises:
        RuntimeError: If no recent runs are found for the DAG
    """
    # NOTE: We assume the most recent run is our run
    # It would be better to add some unique identifier to the DAG to make it easier to identify the run
    recent_runs = session.sql(
        f"""
        select run_id
            from table({DB_NAME}.information_schema.current_task_graphs(
                root_task_name => '{dag.name.upper()}'
            ))
            where database_name = '{DB_NAME}'
            and schema_name = '{SCHEMA_NAME}'
            and scheduled_from = 'EXECUTE TASK';
        """,
    ).collect()
    if len(recent_runs) == 0:
        raise RuntimeError("No recent runs found. Did the DAG fail to run?")
    run_id = recent_runs[0][0]
    print(f"DAG runId: {run_id}")

    start_time = time.time()
    dag_result = None
    while dag_result is None:
        result = session.sql(
            f"""
            select state
                from table({DB_NAME}.information_schema.complete_task_graphs(
                    root_task_name=>'{dag.name.upper()}'
                ))
                where database_name = '{DB_NAME}'
                and schema_name = '{SCHEMA_NAME}'
                and run_id = {run_id};
            """,
        ).collect()

        if len(result) > 0:
            dag_result = result[0][0]
            print(
                f"DAG completed after {(time.time() - start_time):.2f} seconds with result {dag_result}"
            )
            break

        wait_time = min(
            2 ** ((time.time() - start_time) / 10), 5
        )  # Exponential backoff capped at 5 seconds
        time.sleep(wait_time)

    return dag_result

def prepare_datasets(session: Session) -> str:
    """
    DAG task to prepare datasets for model training.

    This function is executed as part of the DAG workflow to prepare the training and test datasets.
    It retrieves the configuration from the task context and calls the shared prepare_datasets
    function to generate the necessary dataset splits.

    Args:
        session (Session): Snowflake session object

    Returns:
        str: JSON string containing serialized dataset information for downstream tasks
    """
    ctx = TaskContext(session)
    config = run_config.RunConfig.from_task_context(ctx)

    ds, train_ds, test_ds = modeling.prepare_datasets(
        session, DATA_TABLE_NAME, config.dataset_name
    )

    dataset_info = {
        "full": asdict(ds.read.data_sources[0]),
        "train": asdict(train_ds.read.data_sources[0]),
        "test": asdict(test_ds.read.data_sources[0]),
    }
    return json.dumps(dataset_info)

@remote(COMPUTE_POOL, stage_name=JOB_STAGE, database=DB_NAME, schema=SCHEMA_NAME)
def train_model(dataset_info: Optional[str] = None) -> Optional[str]:
    '''
    ML Job to train a model on the training dataset and register it in the model registry.

    This function trains an XGBoost classifier on the provided training data and registers it in the model registry. 
    This function is executed remotely on Snowpark Container Services.

    Args:
        dataset_info (Optional[str]): JSON string containing serialized dataset information for training. If this function is called in a DAG task, 
        this argument is passed from the previous DAG task, otherwise it is passed manually.

    Returns:
        Optional[str]: JSON string containing serialized model information for registration. If this function is called in a DAG task, 
        this return value is passed to the next DAG task, otherwise it is as ML Job result.
    '''
    session = Session.builder.getOrCreate()
    ctx = None
    config = None

    if dataset_info:
        dataset_info_dicts = json.loads(dataset_info)
    try:
        ctx = TaskContext(session)
        config = run_config.RunConfig.from_task_context(ctx)
        dataset_info_dicts = json.loads(ctx.get_predecessor_return_value("PREPARE_DATA"))
    except SnowparkSQLException:
        print("there is no predecessor return value, fallback to local mode")

    datasets = {
        key: DatasetInfo(**info_dict) for key, info_dict in dataset_info_dicts.items()
    }
    train_ds=load_dataset(
            session,
            datasets["full"].fully_qualified_name,
            datasets["full"].version,
    )
    model_obj = modeling.train_model(session, datasets["train"])
    train_metrics = modeling.evaluate_model(
        session, model_obj, train_ds.read.data_sources[0], prefix="train"
    )
    version = f"v{uuid.uuid4().hex}"
    mv = modeling.register_model(session, model_obj, config.model_name if config and config.model_name else "mortgage_model", version, train_ds, metrics={}) if config else modeling.register_model(session, model_obj, "mortgage_model", version, train_ds, metrics=train_metrics)
    if ctx and config:
        ctx.set_return_value(json.dumps({"model_name": mv.fully_qualified_model_name, "version_name": mv.version_name}))
    return json.dumps({"model_name": mv.fully_qualified_model_name, "version_name": mv.version_name})

def evaluate_model(session: Session) -> Optional[str]:
    '''
    Evaluate a model on the training and test datasets.

    This function evaluates a trained model on the training and test datasets and returns the performance metrics.

    Args:
        session (Session): Snowflake session object

    Returns:
        Optional[str]: JSON string containing serialized performance metrics for the model. If this function is called in a DAG task, 
        this return value is passed to the next DAG task.
    '''
    ctx = TaskContext(session)
    serialized = json.loads(ctx.get_predecessor_return_value("PREPARE_DATA"))
    dataset_info = {
        key: DatasetInfo(**obj_dict) for key, obj_dict in serialized.items()
    }
    model_info = json.loads(ctx.get_predecessor_return_value("TRAIN_MODEL"))
    model = ops.get_model(session, model_info["model_name"], model_info["version_name"])
    train_metrics = modeling.evaluate_model(
        session, model, dataset_info["train"], prefix="train"
    )
    test_metrics = modeling.evaluate_model(
        session, model, dataset_info["test"], prefix="test"
    )
    metrics = {**train_metrics, **test_metrics}

    return json.dumps(metrics)


def check_model_quality(session: Session) -> str:
    """
    DAG task to check model quality and determine next action.

    This function evaluates the trained model's performance against a configured threshold
    and returns the appropriate next action for the DAG workflow. If the model meets the
    quality threshold, it returns "promote_model", otherwise "send_alert".

    Args:
        session (Session): Snowflake session object

    Returns:
        str: "promote_model" if model meets threshold, "send_alert" otherwise
    """
    ctx = TaskContext(session)
    config = run_config.RunConfig.from_task_context(ctx)

    metrics = json.loads(ctx.get_predecessor_return_value("EVALUATE_MODEL"))

    threshold = config.metric_threshold
    if metrics[config.metric_name] >= threshold:
        return "promote_model"
    else:
        return "send_alert"


def promote_model(session: Session) -> str:
    """
    DAG task to promote a trained model to production.

    This function registers the trained model in the model registry and promotes it
    to production status. It retrieves the model from the stage, loads the dataset
    information, and uses the model pipeline functions to complete the promotion.

    Args:
        session (Session): Snowflake session object

    Returns:
        str: Tuple of (fully_qualified_model_name, version_name) as string
    """
    ctx = TaskContext(session)
    model_info = json.loads(ctx.get_predecessor_return_value("TRAIN_MODEL"))
    mv = ops.get_model(session, model_info["model_name"], model_info["version_name"])
    modeling.promote_model(session, mv)

    return (mv.fully_qualified_model_name, mv.version_name)


def cleanup(session: Session) -> None:
    """
    DAG task to clean up temporary artifacts and obsolete resources.

    This function is executed as a finalizer task in the DAG workflow to clean up
    temporary files, artifacts, and obsolete dataset/model versions. It removes
    the artifact directory from the stage and calls the shared cleanup function.

    Args:
        session (Session): Snowflake session object
    """
    ctx = TaskContext(session)
    config = run_config.RunConfig.from_task_context(ctx)

    modeling.clean_up(session, config.dataset_name, config.model_name)


def create_dag(name: str, schedule: Optional[timedelta] = None, **config: Any) -> DAG:
    """
    Create a DAG for the machine learning model training workflow.

    This function creates a complete DAG that includes data preparation, model training,
    quality checking, model promotion, and cleanup tasks. The DAG is configured with
    the necessary packages and stages for execution.

    Args:
        name (str): Name of the DAG
        schedule (Optional[timedelta], optional): Schedule interval for the DAG.
            Defaults to None (no schedule).
        **config (Any): Additional configuration parameters to override defaults

    Returns:
        DAG: Configured DAG object ready for deployment
    """
    with DAG(
        name,
        warehouse=WAREHOUSE,
        schedule=schedule,
        use_func_return_value=True,
        stage_location=DAG_STAGE,
        packages=["snowflake-snowpark-python", "snowflake-ml-python", "xgboost"],
        config={
            "dataset_name": "mortgage_dataset",
            "model_name": "mortgage_model",
            "metric_name": "test_accuracy",
            "metric_threshold": 0.7,
            **config,
        },
    ) as dag:
        # Need to wrap first function in a DAGTask to make >> operator work properly
        prepare_data = DAGTask("prepare_data", definition=prepare_datasets)
        train_model_task = DAGTask("TRAIN_MODEL", definition=train_model)
        evaluate_model_task = DAGTask("EVALUATE_MODEL", definition=evaluate_model)
        check_model_quality_task = DAGTaskBranch(
            "check_model_quality", definition=check_model_quality
        )
        promote_model_task = DAGTask("promote_model", definition=promote_model)
        alert_task = DAGTask(
            "send_alert",
            definition="""
                CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                    SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
                        SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT(
                            'Model quality check failed to meet the threshold.'
                        )
                    ),
                    SNOWFLAKE.NOTIFICATION.INTEGRATION('DEMO_NOTIFICATION_INTEGRATION')
                );
            """,
        )
        cleanup_task = DAGTask("cleanup_task", definition=cleanup, is_finalizer=True)

        # Build the DAG
        prepare_data >> train_model_task >> evaluate_model_task >> check_model_quality_task >> [promote_model_task, alert_task]

    return dag


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        "Create and deploy a modeling DAG",
        description="Create and deploy a machine learning DAG that automates data preparation, model training, quality checking, and promotion workflow.",
    )
    parser.add_argument(
        "--schedule",
        type=cli_utils.validate_schedule,
        default=None,
        help='Schedule interval for automatic DAG execution in format: <number><unit>. Examples: "1d" for daily, "12h" for every 12 hours, "30m" for every 30 minutes. Use None or omit for manual execution only.',
    )
    parser.add_argument(
        "--run-dag",
        action="store_true",
        default=False,
        help="Execute the DAG immediately after deployment. If not specified, the DAG will only be deployed and can be run manually or according to the schedule.",
    )
    parser.add_argument(
        "-c",
        "--connection",
        type=str,
        help="Name of the Snowflake connection profile to use for authentication. If not specified, uses default connection configuration.",
    )
    args = parser.parse_args()

    session_builder = Session.builder
    if args.connection:
        session_builder = session_builder.config("connection_name", args.connection)
    session = session_builder.getOrCreate()
    ensure_environment(session)

    api_root = Root(session)
    db = api_root.databases[DB_NAME]
    schema = db.schemas[SCHEMA_NAME]

    dag_op = DAGOperation(schema)
    dag = create_dag(name="dag", schedule=args.schedule)
    dag_op.deploy(dag, mode=CreateMode.or_replace)

    if args.run_dag:
        dag_op.run(dag)
        result = _wait_for_run_to_complete(session, dag)
        if result != "SUCCEEDED":
            raise Exception(f"DAG failed with result {result}")
