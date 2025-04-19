import json
import os
import pickle
from typing import Any, Dict
from time import perf_counter
import fsspec

from snowflake.snowpark import Session

from model_utils import create_data_connector, evaluate_model

def load_model(session: Session, model_path: str) -> Any:
    if model_path.startswith("@"):
        model_path = "sfc://" + model_path
    with fsspec.open(model_path, snowpark_session=session) as f:
        model = pickle.load(f)
    return model

def do_eval(session: Session, source_data: str, model_path: str) -> Dict[str, Any]:
    # Load data
    dc = create_data_connector(session, table_name=source_data)
    print("Loading data...", end="", flush=True)
    start = perf_counter()
    df = dc.to_pandas()
    elapsed = perf_counter() - start
    print(f" done! Loaded {len(df)} rows, elapsed={elapsed:.3f}s")

    # Split data
    X = df.drop("IS_DEFAULT", axis=1)
    y = df["IS_DEFAULT"]

    # Load model
    print("Loading model...", end="", flush=True)
    start = perf_counter()
    with open(os.path.join(model_path, "model.pkl"), "rb") as f:
        model = pickle.load(f)
    elapsed = perf_counter() - start
    print(f" done! Elapsed={elapsed:.3f}s")

    # Run evaluation
    print("Running evaluation...", end="", flush=True)
    start = perf_counter()
    metrics = evaluate_model(model, X, y)
    elapsed = perf_counter() - start
    print(f" done! Elapsed={elapsed:.3f}s")

    print(json.dumps(metrics, indent=2))
    return metrics


if __name__ == "__main__":
    import argparse
    from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source_data", default="loan_applications", help="Name of input data table"
    )
    parser.add_argument(
        "--model_path", type=str, help="Path to model file(s)",
    )
    args = parser.parse_args()

    session = Session.builder.configs(SnowflakeLoginOptions()).create()
    do_eval(session, **vars(args))
