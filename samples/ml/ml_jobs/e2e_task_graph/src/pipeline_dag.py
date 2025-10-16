import io
import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

import cloudpickle as cp
from snowflake.core import CreateMode, Root
from snowflake.core.task.context import TaskContext
from snowflake.core.task.dagv1 import DAG, DAGOperation, DAGTask, DAGTaskBranch
from snowflake.ml.data import DatasetInfo
from snowflake.ml.dataset import load_dataset
from snowflake.ml.jobs import MLJob
from snowflake.snowpark import Session

import cli_utils
import data
import modeling
from constants import (DAG_STAGE, DATA_TABLE_NAME, DB_NAME, SCHEMA_NAME,
                       WAREHOUSE)

ARTIFACT_DIR = "run_artifacts"


def _ensure_environment(session: Session):
    """
    Ensure the environment is properly set up for DAG execution.

    This function sets up the necessary environment by calling the shared
    ensure_environment function, creating the raw data table if it doesn't exist,
    and registering local modules for inclusion in ML Job payloads.

    Args:
        session (Session): Snowflake session object
    """
    modeling.ensure_environment(session)

    # Ensure the raw data table exists
    _ = data.get_raw_data(session, DATA_TABLE_NAME, create_if_not_exists=True)

    # Register local modules for inclusion in ML Job payloads
    cp.register_pickle_by_value(modeling)


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


@dataclass(frozen=True)
class RunConfig:
    run_id: str
    dataset_name: str
    model_name: str
    metric_name: str
    metric_threshold: float

    @property
    def artifact_dir(self) -> str:
        return os.path.join(DAG_STAGE, ARTIFACT_DIR, self.run_id)

    @classmethod
    def from_task_context(cls, ctx: TaskContext, **kwargs: Any) -> "RunConfig":
        run_schedule = ctx.get_current_task_graph_original_schedule()
        run_id = "v" + (
            run_schedule.strftime("%Y%m%d_%H%M%S")
            if isinstance(run_schedule, datetime)
            else str(run_schedule)
        )
        run_config = dict(run_id=run_id)

        graph_config = ctx.get_task_graph_config()
        merged = run_config | graph_config | kwargs

        # Get expected fields from RunConfig
        expected_fields = set(cls.__annotations__)

        # Find unexpected keys
        unexpected_keys = [key for key in merged.keys() if key not in expected_fields]
        for key in unexpected_keys:
            print(f"Warning: Unexpected config key '{key}' will be ignored")

        filtered = {k: v for k, v in merged.items() if k in expected_fields}
        return cls(**filtered)

    @classmethod
    def from_session(cls, session: Session) -> "RunConfig":
        ctx = TaskContext(session)
        return cls.from_task_context(ctx)


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
    config = RunConfig.from_task_context(ctx)

    ds, train_ds, test_ds = modeling.prepare_datasets(
        session, DATA_TABLE_NAME, config.dataset_name
    )

    dataset_info = {
        "full": asdict(ds.read.data_sources[0]),
        "train": asdict(train_ds.read.data_sources[0]),
        "test": asdict(test_ds.read.data_sources[0]),
    }
    return json.dumps(dataset_info)


def train_model(session: Session) -> str:
    """
    DAG task to train a machine learning model.

    This function is executed as part of the DAG workflow to train a model using the prepared datasets.
    It retrieves dataset information from the previous task, trains the model, evaluates it on both
    training and test sets, and saves the model to a stage for later use.

    Args:
        session (Session): Snowflake session object

    Returns:
        str: JSON string containing model path and evaluation metrics
    """
    ctx = TaskContext(session)
    config = RunConfig.from_task_context(ctx)

    # Load the datasets
    serialized = json.loads(ctx.get_predecessor_return_value("PREPARE_DATA"))
    dataset_info = {
        key: DatasetInfo(**obj_dict) for key, obj_dict in serialized.items()
    }

    # Train the model
    model = modeling.train_model(session, dataset_info["train"])
    if isinstance(model, MLJob):
        model = model.result()

    # Evaluate the model
    train_metrics = modeling.evaluate_model(
        session, model, dataset_info["train"], prefix="train"
    )
    test_metrics = modeling.evaluate_model(
        session, model, dataset_info["test"], prefix="test"
    )
    metrics = {**train_metrics, **test_metrics}

    # Save model to stage and return the metrics as a JSON string
    model_pkl = cp.dumps(model)
    model_path = os.path.join(config.artifact_dir, "model.pkl")
    put_result = session.file.put_stream(
        io.BytesIO(model_pkl), model_path, overwrite=True
    )

    result_dict = {
        "model_path": os.path.join(config.artifact_dir, put_result.target),
        "metrics": metrics,
    }
    return json.dumps(result_dict)


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
    config = RunConfig.from_task_context(ctx)

    metrics = json.loads(ctx.get_predecessor_return_value("TRAIN_MODEL"))["metrics"]

    # If model is good, promote model
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
    config = RunConfig.from_task_context(ctx)

    train_result = json.loads(ctx.get_predecessor_return_value("TRAIN_MODEL"))
    model_path = train_result["model_path"]
    with session.file.get_stream(model_path, decompress=True) as stream:
        model = cp.loads(stream.read())

    serialized = json.loads(ctx.get_predecessor_return_value("PREPARE_DATA"))
    source_data = {key: DatasetInfo(**obj_dict) for key, obj_dict in serialized.items()}
    mv = modeling.register_model(
        session,
        model,
        model_name=config.model_name,
        version_name=config.run_id,
        train_ds=load_dataset(
            session,
            source_data["full"].fully_qualified_name,
            source_data["full"].version,
        ),
        metrics=train_result["metrics"],
    )

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
    config = RunConfig.from_task_context(ctx)

    session.sql(f"REMOVE {config.artifact_dir}").collect()
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
        packages=["snowflake-snowpark-python", "snowflake-ml-python<1.9.0", "xgboost"],  # NOTE: Temporarily pinning to <1.9.0 due to compatibility issues
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
        evaluate_model = DAGTaskBranch(
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
        prepare_data >> train_model >> evaluate_model >> [promote_model_task, alert_task]

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
    _ensure_environment(session)

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
