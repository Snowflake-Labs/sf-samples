from snowflake.ml.data import DatasetInfo
from snowflake.core.task.context import TaskContext
from snowflake.snowpark import Session
import os
import json
import cloudpickle as cp
import io

from pipeline_dag import RunConfig
from modeling import evaluate_model, train_model

session = Session.builder.getOrCreate()


if __name__ == "__main__":
    index = int(os.environ.get("SNOWFLAKE_JOB_INDEX", 0))

    # Only head node saves and returns results
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