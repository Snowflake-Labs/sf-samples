import json
import os
import pickle
import cloudpickle as cp
from time import perf_counter

from sklearn.model_selection import train_test_split
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.ml.jobs import decorators as jd, manager as jm
from snowflake.snowpark import Session

import model_utils
cp.register_pickle_by_value(model_utils)

@jd.remote_ml_job("DEMO_POOL_GPU", stage_name="payload_stage")
def train_model(source_data: str, save_mode: str = "local", output_dir: str = None):
    # Initialize Snowflake session
    # See https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections
    # for how to define default connections in a config.toml file
    session = Session.builder.configs(SnowflakeLoginOptions()).create()

    # Load data
    dc = model_utils.create_data_connector(session, table_name=source_data)
    print("Loading data...", end="", flush=True)
    start = perf_counter()
    df = dc.to_pandas()
    elapsed = perf_counter() - start
    print(f" done! Loaded {len(df)} rows, elapsed={elapsed:.3f}s")

    # Split data
    X = df.drop("IS_DEFAULT", axis=1)
    y = df["IS_DEFAULT"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train model
    model = model_utils.build_pipeline()
    print("Training model...", end="")
    start = perf_counter()
    model.fit(X_train, y_train)
    elapsed = perf_counter() - start
    print(f" done! Elapsed={elapsed:.3f}s")

    # Evaluate model
    print("Evaluating model...", end="")
    start = perf_counter()
    metrics = model_utils.evaluate_model(
        model,
        X_test,
        y_test,
    )
    elapsed = perf_counter() - start
    print(f" done! Elapsed={elapsed:.3f}s")

    # Print evaluation results
    print("\nModel Performance Metrics:")
    print(f"Accuracy: {metrics['accuracy']:.4f}")
    print(f"ROC AUC: {metrics['roc_auc']:.4f}")
    # Uncomment below for full classification report
    # print("\nClassification Report:")
    # print(metrics["classification_report"])

    start = perf_counter()
    if save_mode == "local":
        # Save model locally
        print("Saving model to disk...", end="")
        output_dir = output_dir or os.path.dirname(__file__)
        model_subdir = os.environ.get("SNOWFLAKE_SERVICE_NAME", "output")
        model_dir = os.path.join(output_dir, model_subdir) if not output_dir.endswith(model_subdir) else output_dir
        os.makedirs(model_dir, exist_ok=True)
        with open(os.path.join(model_dir, "model.pkl"), "wb") as f:
            pickle.dump(model, f)
        with open(os.path.join(model_dir, "metrics.json"), "w") as f:
            json.dump(metrics, f, indent=2)
    elif save_mode == "registry":
        # Save model to registry
        print("Logging model to Model Registry...", end="")
        model_utils.save_to_registry(
            session,
            model=model,
            model_name="loan_default_predictor",
            metrics=metrics,
            sample_input_data=X_train,
        )
    elapsed = perf_counter() - start
    print(f" done! Elapsed={elapsed:.3f}s")

    # Close Snowflake session
    session.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source_data", default="loan_applications", help="Name of input data table"
    )
    parser.add_argument(
        "--save_mode",
        choices=["local", "registry"],
        default="local",
        help="Model save mode",
    )
    parser.add_argument(
        "--output_dir", type=str, help="Local save path. Only relevant if save_mode=local"
    )
    args = parser.parse_args()

    # We need a Snowflake session to submit jobs
    session = Session.builder.configs(SnowflakeLoginOptions("preprod8")).create()

    # Kick off training using decorated function
    job1 = train_model(**vars(args))
    print("Submitted job from decorated function:", job1.id)

    # Alternatively, submit job from files using job manager
    arg_list = ["--source_data", args.source_data, "--save_mode", args.save_mode]
    if args.output_dir:
        arg_list.extend(["--output_dir", args.output_dir])
    job2 = jm.submit_job(
        "/Users/dhung/repos/mlruntimes-samples/headless/single-node-xgb/src",
        compute_pool="DEMO_POOL_GPU",
        stage_name="payload_stage",
        entrypoint="train.py",
        args=arg_list,
        session=session,
    )
    print("Submitted job from files using job manager:", job2.id)

    # Show job results on completion
    job1.wait()
    print("============= Job 1 =============")
    job1.show_logs()
    print("============= End job 1 =============")

    job2.wait()
    print("============= Job 2 =============")
    job2.show_logs()
    print("============= End job 2 =============")
