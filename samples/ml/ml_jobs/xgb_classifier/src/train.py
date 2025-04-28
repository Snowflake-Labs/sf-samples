import json
import os
import pickle
from time import perf_counter

from sklearn.model_selection import train_test_split
from snowflake.snowpark import Session

from model_utils import create_data_connector, build_pipeline, evaluate_model, save_to_registry

def do_train(session: Session, source_data: str, save_mode: str = "local", output_dir: str = None):
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
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train model
    model = build_pipeline()
    print("Training model...", end="")
    start = perf_counter()
    model.fit(X_train, y_train)
    elapsed = perf_counter() - start
    print(f" done! Elapsed={elapsed:.3f}s")

    # Evaluate model
    print("Evaluating model...", end="")
    start = perf_counter()
    metrics = evaluate_model(
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
        save_to_registry(
            session,
            model=model,
            model_name="loan_default_predictor",
            metrics=metrics,
            sample_input_data=X_train,
        )
    elapsed = perf_counter() - start
    print(f" done! Elapsed={elapsed:.3f}s")


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

    session = Session.builder.getOrCreate()
    do_train(session, **vars(args))
