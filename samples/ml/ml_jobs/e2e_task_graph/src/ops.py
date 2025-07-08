import warnings
from typing import Any, Dict, Optional, Union

from snowflake.ml.dataset import Dataset
from snowflake.ml.model import ModelVersion
from snowflake.ml.registry import Registry
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.exceptions import SnowparkSQLException


def get_model_registry(
    session: Session,
    db: Optional[str] = None,
    schema: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
) -> Registry:
    """
    Create a Snowflake model registry object.
    """
    default_options = {
        "enable_monitoring": True,
    }
    options = {**default_options, **(options or {})}
    return Registry(
        session=session, database_name=db, schema_name=schema, options=options
    )


def register_model(
    session: Session,
    model: Any,
    model_name: str,
    version_name: str,
    train_data: Optional[Union[DataFrame, Dataset]] = None,
    metrics: Optional[Dict[str, float]] = None,
    options: Optional[Dict[str, Any]] = None,
    registry: Optional[Registry] = None,
) -> ModelVersion:
    """
    Register a model in the Snowflake model registry.
    """
    registry = registry or get_model_registry(session)

    sample_data = (
        train_data.read.to_snowpark_dataframe(only_feature_cols=True).limit(100)
        if isinstance(train_data, Dataset)
        else train_data
    )
    default_options = {
        "enable_explainability": False,
    }
    options = {**default_options, **(options or {})}

    with warnings.catch_warnings():  # Ignore warning about relax_versions from model registration
        warnings.simplefilter("ignore", UserWarning)
        mv = registry.log_model(
            model_name=model_name,
            model=model,
            version_name=version_name,
            sample_input_data=sample_data,
            comment=f"{type(model).__name__} for predicting loan approval likelihood.",
            target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
            options=options,
        )

    if metrics:
        update_metrics(session, mv, metrics)

    return mv


def update_metrics(
    session: Session, model: ModelVersion, metrics: Dict[str, float]
) -> None:
    """
    Set metrics for a model version in the Snowflake model registry.

    Args:
        session (Session): Snowflake session object.
        model (ModelVersion): Model version object.
        metrics (Dict[str, float]): Dictionary of metrics to set.
    """
    if not metrics:
        return

    for metric_name, value in metrics.items():
        model.set_metric(metric_name=metric_name, value=value)


def get_prod_model(
    session: Session, model_name: str, registry: Optional[Registry] = None
) -> ModelVersion:
    """
    Get the production model version from the Snowflake model registry.

    Args:
        session (Session): Snowflake session object.
        model_name (str): Name of the model.
        registry (Optional[Registry]): Snowflake model registry object.

    Returns:
        ModelVersion: Production model version object.
    """
    registry = registry or get_model_registry(session)
    base_model = registry.get_model(model_name)

    try:
        return base_model.default
    except SnowparkSQLException:
        return None


def promote_model(
    session: Session, model: ModelVersion, registry: Optional[Registry] = None
) -> None:
    """
    Promote a model to production in the Snowflake model registry.

    Args:
        session (Session): Snowflake session object.
        model_name (str): Name of the model.
        version_name (str): Version name of the model.
        registry (Optional[Registry]): Snowflake model registry object.
    """
    registry = registry or get_model_registry(session)

    # Set model as default
    base_model = registry.get_model(model.model_name)
    base_model.default = model
