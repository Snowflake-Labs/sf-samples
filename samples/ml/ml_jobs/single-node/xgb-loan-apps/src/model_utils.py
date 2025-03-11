import pandas as pd
import xgboost as xgb
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from snowflake.ml.data.data_connector import DataConnector
from snowflake.ml.registry import Registry as ModelRegistry
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