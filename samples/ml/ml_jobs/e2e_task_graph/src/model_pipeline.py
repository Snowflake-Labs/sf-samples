import logging
from datetime import datetime, timedelta, timezone

import cloudpickle as cp
import data
import ops
from constants import (
    COMPUTE_POOL,
    DAG_STAGE,
    DATA_TABLE_NAME,
    DB_NAME,
    JOB_STAGE,
    ROLE_NAME,
    SCHEMA_NAME,
    WAREHOUSE,
)
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from snowflake.ml.data import DataConnector, DatasetInfo, DataSource
from snowflake.ml.dataset import Dataset, load_dataset
from snowflake.ml.jobs import remote
from snowflake.ml.model import ModelVersion
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from xgboost import XGBClassifier

logging.getLogger().setLevel(logging.ERROR)


def _try_run_query(session: Session, query: str) -> bool:
    """
    Try to execute a SQL query and return whether it succeeded.

    This function attempts to execute a SQL query and returns True if successful,
    False if it fails with certain error codes. It re-raises the exception for
    critical errors (error code 1003).

    Args:
        session (Session): Snowflake session object
        query (str): SQL query to execute

    Returns:
        bool: True if query executed successfully, False if it failed gracefully

    Raises:
        SnowparkSQLException: If the query fails with error code 1003 (critical error)
    """
    try:
        session.sql(query).collect()
        return True
    except SnowparkSQLException as e:
        if e.sql_error_code == 1003:
            raise
        return False


def ensure_environment(session: Session):
    """
    Ensure the environment is set up for pipeline execution.

    This function configures the Snowflake session with the necessary role, warehouse, database,
    and schema. It also creates required stages and registers local modules for ML Job execution.

    Args:
        session (Session): Snowflake session object to configure
    """
    # Set role and warehouse
    session.use_role(ROLE_NAME)
    session.use_warehouse(WAREHOUSE)

    # Configure session database and schema
    _try_run_query(session, f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    session.use_database(DB_NAME)
    _try_run_query(session, f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}")
    session.use_schema(SCHEMA_NAME)

    # Create stages if needed
    _try_run_query(session, f"CREATE STAGE IF NOT EXISTS {DAG_STAGE.lstrip('@')}")
    _try_run_query(session, f"CREATE STAGE IF NOT EXISTS {JOB_STAGE.lstrip('@')}")

    # Register local modules for inclusion in ML Job payloads
    cp.register_pickle_by_value(ops)
    cp.register_pickle_by_value(data)


def register_model(
    session: Session,
    model: XGBClassifier,
    model_name: str,
    version_name: str,
    train_ds: Dataset,
    metrics: dict,
) -> ModelVersion:
    """
    Register a model in the model registry.

    This function registers a trained model in the Snowflake model registry with the specified
    name and version. It also associates the model with training data and performance metrics.

    Args:
        session (Session): Snowflake session object
        model (XGBClassifier): Trained XGBoost model to register
        model_name (str): Name for the model in the registry
        version_name (str): Version identifier for this model instance
        train_ds (Dataset): Training dataset used to train the model
        metrics (dict): Dictionary of performance metrics for the model

    Returns:
        ModelVersion: The registered model version object
    """
    mv = ops.register_model(
        session,
        model,
        model_name=model_name,
        version_name=version_name,
        train_data=train_ds,
        metrics=metrics,
    )
    print(f"Registered model {mv.fully_qualified_model_name} version {mv.version_name}")

    return mv


def promote_model(session: Session, mv: ModelVersion) -> None:
    """
    Promote a model version to production.

    This function promotes a specific model version to production status in the model registry,
    making it the default version for inference operations.

    Args:
        session (Session): Snowflake session object
        mv (ModelVersion): Model version object to promote to production
    """
    ops.promote_model(session, mv)
    print(
        f"Promoted model {mv.fully_qualified_model_name} version {mv.version_name} to production"
    )


def clean_up(
    session: Session, dataset_name: str, model_name: str, expiry_days: int = 7
) -> None:
    """
    Clean up obsolete artifacts.

    This function removes obsolete model versions and dataset versions that are older than
    the specified expiry period. It helps maintain a clean workspace by removing outdated
    artifacts while preserving active models and datasets that are still in use.

    Args:
        session (Session): Snowflake session object
        dataset_name (str): Name of the dataset to clean up
        model_name (str): Name of the model to clean up
        expiry_days (int, optional): Number of days after which artifacts are considered obsolete.
            Defaults to 7.
    """
    # Delete obsolete models
    mr = ops.get_model_registry(session)
    model = mr.get_model(model_name=model_name)
    for _, mv_info in model.show_versions().iterrows():
        if (
            mv_info["created_on"]
            < datetime.now(timezone.utc) - timedelta(days=expiry_days)
            and mv_info["is_default_version"].lower() == "false"
        ):
            model.delete_version(mv_info["name"])
            print(f"Deleted obsolete model version {mv_info['name']}")

    # Delete obsolete datasets
    # Only consider the "main" dataset version, but retain any datasets where
    # the training split is still used by any active models.
    ds = Dataset.load(session, dataset_name)
    versions = [
        v
        for v in ds.list_versions()
        if not v.endswith("_train") and not v.endswith("_test")
    ]
    for version in versions:
        dsv = ds.select_version(version)
        if dsv.selected_version.created_on < datetime.now(timezone.utc) - timedelta(
            days=expiry_days
        ) and not ds.selected_version(f"{version}_train").lineage(
            "downstream", domain_filter={"model"}
        ):
            ds.delete_version(version)
            print(f"Deleted obsolete dataset version {version}")


def prepare_datasets(
    session: Session,
    source_table: str,
    name: str,
    *,
    create_assets: bool = False,
    force_refresh: bool = False,
) -> tuple[Dataset, Dataset, Dataset]:
    """
    Prepare datasets for training and evaluation with feature engineering and splitting.

    This function creates or loads datasets for machine learning, including feature engineering
    through the feature store. It handles both creation of new datasets and loading of existing
    ones, and automatically splits the data into training and test sets.

    Args:
        session (Session): Snowflake session object
        source_table (str): Name of the source table containing raw data
        name (str): Name for the dataset to create or load
        create_assets (bool, optional): Whether to create necessary assets if they don't exist.
            Defaults to False.
        force_refresh (bool, optional): Whether to force refresh by deleting existing datasets.
            Defaults to False.

    Returns:
        tuple[Dataset, Dataset, Dataset]: Tuple containing (full_dataset, train_dataset, test_dataset)
    """
    version = data.get_data_last_altered_timestamp(session, source_table)

    if force_refresh:
        data.delete_dataset_versions(
            session, name, version, f"{version}_train", f"{version}_test"
        )

    try:
        ds = load_dataset(session, name, version)
        train_ds = ds.select_version(f"{version}_train")
        test_ds = ds.select_version(f"{version}_test")
        print(
            f"Loaded existing dataset {ds.fully_qualified_name} version {ds.selected_version.name}"
        )
    except Exception:
        ds = data.generate_feature_dataset(
            session,
            source_table=source_table,
            name=name,
            create_assets=create_assets,
            force_refresh=force_refresh,
        )
        print(
            f"Generated dataset {ds.fully_qualified_name} version {ds.selected_version.name}"
        )

        train_ds, test_ds = data.split_dataset(ds)
        print(
            f"Generated train and test dataset versions {train_ds.selected_version.name} and {test_ds.selected_version.name}"
        )

    return (ds, train_ds, test_ds)


@remote(COMPUTE_POOL, stage_name=JOB_STAGE)
def train_model(session: Session, input_data: DataSource) -> XGBClassifier:
    """
    Train a model on the training dataset.

    This function trains an XGBoost classifier on the provided training data. It extracts
    features and labels from the input data, configures the model with predefined parameters,
    and trains the model. This function is executed remotely on Snowpark Container Services.

    Args:
        session (Session): Snowflake session object
        input_data (DataSource): Data source containing training data with features and labels

    Returns:
        XGBClassifier: Trained XGBoost classifier model
    """
    input_data_df = DataConnector.from_sources(session, [input_data]).to_pandas()

    assert isinstance(input_data, DatasetInfo), "Input data must be a DatasetInfo"
    exclude_cols = input_data.exclude_cols
    label_col = exclude_cols[0]

    X_train = input_data_df.drop(exclude_cols, axis=1)
    y_train = input_data_df[label_col].squeeze()

    model_params = dict(
        max_depth=50,
        n_estimators=3,
        learning_rate=0.75,
        objective="binary:logistic",
        booster="gbtree",
    )
    estimator = XGBClassifier(**model_params)
    estimator.fit(X_train, y_train)

    return estimator


def evaluate_model(
    session: Session,
    model: XGBClassifier,
    input_data: DataSource,
    *,
    prefix: str = None,
) -> dict:
    """
    Evaluate a model on the training and test datasets.

    This function evaluates a trained model's performance by calculating various metrics
    including F1 score, accuracy, precision, and recall. It can optionally add a prefix
    to metric names to distinguish between training and test metrics.

    Args:
        session (Session): Snowflake session object
        model (XGBClassifier): Trained XGBoost model to evaluate
        input_data (DataSource): Data source containing evaluation data with features and labels
        prefix (str, optional): Prefix to add to metric names (e.g., "train_", "test_").
            Defaults to None.

    Returns:
        dict: Dictionary containing evaluation metrics with metric names as keys and scores as values
    """
    input_data_df = DataConnector.from_sources(session, [input_data]).to_pandas()

    assert isinstance(input_data, DatasetInfo), "Input data must be a DatasetInfo"
    exclude_cols = input_data.exclude_cols
    label_col = exclude_cols[0]

    X_test = input_data_df.drop(exclude_cols, axis=1)
    expected = input_data_df[label_col].squeeze()
    actual = model.predict(X_test)

    metric_types = [
        f1_score,
        accuracy_score,
        precision_score,
        recall_score,
    ]

    metrics = {
        m.__name__.strip("_score"): round(m(expected, actual), 4) for m in metric_types
    }

    if prefix:
        metrics = {f"{prefix}_{k}": v for k, v in metrics.items()}

    return metrics


def run_pipeline(
    session: Session,
    source_table: str,
    dataset_name: str,
    model_name: str,
    *,
    force_refresh: bool = False,
    no_register: bool = False,
):
    """
    Run the complete machine learning pipeline from data preparation to model deployment.

    This function orchestrates the entire ML pipeline including data preparation, model training,
    evaluation, and optional registration/promotion. It's designed to be run as a standalone
    pipeline or as part of a larger workflow.

    Args:
        session (Session): Snowflake session object
        source_table (str): Name of the source table containing raw data
        dataset_name (str): Name for the generated dataset
        model_name (str): Name for the model to be trained
        force_refresh (bool, optional): Whether to force refresh of datasets. Defaults to False.
        no_register (bool, optional): Whether to skip model registration. Defaults to False.
    """
    _, train_ds, test_ds = prepare_datasets(
        session,
        source_table,
        name=dataset_name,
        create_assets=False,
        force_refresh=force_refresh,
    )

    print("Training model...")
    model = train_model(session, train_ds.read.data_sources[0]).result()

    print("Evaluating model...")
    train_metrics = evaluate_model(
        session, model, train_ds.read.data_sources[0], prefix="train"
    )
    test_metrics = evaluate_model(
        session, model, test_ds.read.data_sources[0], prefix="test"
    )
    metrics = {**train_metrics, **test_metrics}

    key_metric = "test_accuracy"
    threshold = 0.7
    current_score = metrics[key_metric]
    print(f"Current score: {current_score}. Threshold for promotion: {threshold}.")

    if no_register:
        print("Model registration disabled via --no-register flag.")
    elif current_score > threshold:
        # If model is good, register and promote model
        version = datetime.now().strftime("v%Y%m%d_%H%M%S")
        mv = register_model(session, model, model_name, version, train_ds, metrics)
        promote_model(session, mv)

    clean_up(session, dataset_name, model_name, expiry_days=1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--source-table",
        type=str,
        default=DATA_TABLE_NAME,
        help="Source table name",
    )
    parser.add_argument(
        "-d",
        "--dataset-name",
        type=str,
        default="mortgage_dataset",
        help="Dataset name",
    )
    parser.add_argument(
        "-m", "--model-name", type=str, default="mortgage_model", help="Model name"
    )
    parser.add_argument(
        "-f",
        "--force-refresh",
        action="store_true",
        help="Force refresh of datasets and models",
    )
    parser.add_argument(
        "--no-register", action="store_true", help="Disable model registration"
    )
    parser.add_argument("-c", "--connection", type=str, help="Connection name")
    args = parser.parse_args()

    session_builder = Session.builder
    if args.connection:
        session_builder = session_builder.config("connection_name", args.connection)
    session = session_builder.getOrCreate()
    ensure_environment(session)

    run_pipeline(
        session,
        source_table=args.source_table,
        dataset_name=args.dataset_name or args.source_table,
        model_name=args.model_name or args.source_table,
        force_refresh=args.force_refresh,
        no_register=args.no_register,
    )
