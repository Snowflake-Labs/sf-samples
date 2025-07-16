import logging
from datetime import datetime
from snowflake.snowpark import Session

import modeling
from constants import DATA_TABLE_NAME

logging.getLogger().setLevel(logging.ERROR)


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
    ds, train_ds, test_ds = modeling.prepare_datasets(
        session,
        source_table,
        name=dataset_name,
        create_assets=False,
        force_refresh=force_refresh,
    )

    print("Training model...")
    model_obj = modeling.train_model(session, train_ds.read.data_sources[0]).result()

    print("Evaluating model...")
    train_metrics = modeling.evaluate_model(
        session, model_obj, train_ds.read.data_sources[0], prefix="train"
    )
    test_metrics = modeling.evaluate_model(
        session, model_obj, test_ds.read.data_sources[0], prefix="test"
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
        mv = modeling.register_model(session, model_obj, model_name, version, ds, metrics)
        modeling.promote_model(session, mv)

    modeling.clean_up(session, dataset_name, model_name, expiry_days=1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run a complete machine learning pipeline including data preparation, model training, evaluation, and optional registration/promotion."
    )
    parser.add_argument(
        "-s",
        "--source-table",
        type=str,
        default=DATA_TABLE_NAME,
        help="Fully qualified name of the source table containing raw data for model training. Default uses DATA_TABLE_NAME from environment configuration.",
    )
    parser.add_argument(
        "-d",
        "--dataset-name",
        type=str,
        default="mortgage_dataset",
        help="Name for the dataset to create or load. This will be used to create feature-engineered datasets with train/test splits.",
    )
    parser.add_argument(
        "-m",
        "--model-name",
        type=str,
        default="mortgage_model",
        help="Name for the model to train and register in the model registry. Used for model versioning and deployment.",
    )
    parser.add_argument(
        "-f",
        "--force-refresh",
        action="store_true",
        help="Force recreation of datasets and models, deleting any existing versions. Use this to start fresh or update with new data.",
    )
    parser.add_argument(
        "--no-register",
        action="store_true",
        help="Skip model registration and promotion even if the model meets quality thresholds. Useful for testing or experimental runs.",
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
    modeling.ensure_environment(session)

    run_pipeline(
        session,
        source_table=args.source_table,
        dataset_name=args.dataset_name or args.source_table,
        model_name=args.model_name or args.source_table,
        force_refresh=args.force_refresh,
        no_register=args.no_register,
    )
