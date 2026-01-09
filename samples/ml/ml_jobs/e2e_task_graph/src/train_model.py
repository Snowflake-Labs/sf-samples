from snowflake.ml.data import DataConnector, DatasetInfo, DataSource
from snowflake.core.task.context import TaskContext
from snowflake.snowpark import Session
from xgboost import XGBClassifier
import os
import json
import cloudpickle as cp
import io

from pipeline_dag import RunConfig
from modeling import evaluate_model

session = Session.builder.getOrCreate()

def train_model(session: Session, input_data: DataSource) -> XGBClassifier:
    """
    Train a model on the training dataset and evaluate it on the test dataset.

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

    # Retrieve the number of nodes from environment variable
    if int(os.environ.get("SNOWFLAKE_JOBS_COUNT", 1)) > 1:
        # Distributed training - use ML Runtime distributor APIs
        from snowflake.ml.modeling.distributors.xgboost.xgboost_estimator import (
            XGBEstimator,
            XGBScalingConfig,
        )
        estimator = XGBEstimator(
            params=model_params,
            scaling_config=XGBScalingConfig(),
        )
    else:
        # Single node training - can use standard XGBClassifier
        estimator = XGBClassifier(**model_params)

    estimator.fit(X_train, y_train)

    # Convert distributed estimator to standard XGBClassifier if needed
    return getattr(estimator, '_sklearn_estimator', estimator)


if __name__ == "__main__":
    index = int(os.environ.get("SNOWFLAKE_JOB_INDEX", 0))
    if index != 0:
        print(f"Worker node (index {index}) - exiting")
        exit(0)

    ctx = TaskContext(session)
    config = RunConfig.from_task_context(ctx)

    # Load the datasets
    serialized = json.loads(ctx.get_predecessor_return_value("PREPARE_DATA"))
    dataset_info = {
        key: DatasetInfo(**obj_dict) for key, obj_dict in serialized.items()
    }
    artifact_dir = config.artifact_dir
    model_obj = train_model(session, dataset_info["train"])
    if not hasattr(model_obj, 'feature_weights'):
        model_obj.feature_weights = None
    train_metrics = evaluate_model(
        session, model_obj, dataset_info["train"], prefix="train"
    )
    test_metrics = evaluate_model(
        session, model_obj, dataset_info["test"], prefix="test"
    )
    metrics = {**train_metrics, **test_metrics}

    model_pkl = cp.dumps(model_obj)
    model_path = os.path.join(config.artifact_dir, "model.pkl")
    put_result = session.file.put_stream(
        io.BytesIO(model_pkl), model_path, overwrite=True
    )
    result_dict = {
        "model_path": os.path.join(config.artifact_dir, put_result.target),
        "metrics": metrics,
    }
    ctx.set_return_value(json.dumps(result_dict))