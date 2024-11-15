import json
import os
import pickle
from time import perf_counter
from typing import Literal, Optional

import pandas as pd
import xgboost as xgb
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from snowflake.ml.data.data_connector import DataConnector
from snowflake.ml.registry import Registry as ModelRegistry
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
from snowflake.snowpark import Session


def create_data_connector(session, table_name: str) -> DataConnector:
    """Load data from Snowflake table"""
    # Example query - modify according to your schema
    query = f"""
    SELECT
        age,
        income,
        credit_score,
        employment_length,
        loan_amount,
        debt_to_income,
        number_of_credit_lines,
        previous_defaults,
        loan_purpose,
        is_default
    FROM {table_name}
    """
    sp_df = session.sql(query)
    return DataConnector.from_dataframe(sp_df)


def build_pipeline(model_params: dict = None) -> Pipeline:
    """Create pipeline with preprocessors and model"""
    # Define column types
    categorical_cols = ["LOAN_PURPOSE"]
    numerical_cols = [
        "AGE",
        "INCOME",
        "CREDIT_SCORE",
        "EMPLOYMENT_LENGTH",
        "LOAN_AMOUNT",
        "DEBT_TO_INCOME",
        "NUMBER_OF_CREDIT_LINES",
        "PREVIOUS_DEFAULTS",
    ]

    # Numerical preprocessing pipeline
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    # Categorical preprocessing pipeline
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    # Combine transformers
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numerical_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    # Define model parameters
    default_params = {
        "objective": "binary:logistic",
        "eval_metric": "auc",
        "max_depth": 6,
        "learning_rate": 0.1,
        "n_estimators": 100,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
    }
    model = xgb.XGBClassifier(**(model_params or default_params))

    return Pipeline([("preprocessor", preprocessor), ("classifier", model)])


def evaluate_model(model: Pipeline, X_test: pd.DataFrame, y_test: pd.DataFrame):
    """Evaluate model performance"""
    # Make predictions
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # Calculate metrics
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_pred_proba),
        "classification_report": classification_report(y_test, y_pred),
    }

    return metrics


def save_to_registry(
    session: Session,
    model: Pipeline,
    model_name: str,
    metrics: dict,
    sample_input_data: pd.DataFrame,
):
    """Save model and artifacts to Snowflake Model Registry"""
    # Initialize model registry
    registry = ModelRegistry(session)

    # Save to registry
    registry.log_model(
        model=model,
        model_name=model_name,
        metrics=metrics,
        sample_input_data=sample_input_data[:5],
        conda_dependencies=["xgboost"],
    )


def main(source_data: str, save_mode: Literal["local", "registry"] = "local", output_dir: Optional[str] = None):
    # Initialize Snowflake session
    # See https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections
    # for how to define default connections in a config.toml file
    session = Session.builder.configs(SnowflakeLoginOptions()).create()

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

    main(**vars(args))
