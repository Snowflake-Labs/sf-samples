import os
from datetime import datetime
from typing import Any, Optional, Tuple

import pandas as pd
from constants import (
    DAG_STAGE,
    DATA_TABLE_NAME,
    DB_NAME,
    FEATURE_STORE_NAME,
    SCHEMA_NAME,
    WAREHOUSE,
)
from snowflake.ml import dataset, feature_store
from snowflake.ml._internal.exceptions.dataset_errors import (
    DatasetError,
    DatasetNotExistError,
)
from snowflake.ml._internal.utils.identifier import (
    parse_schema_level_object_identifier,
    resolve_identifier,
)
from snowflake.snowpark import DataFrame, Session, Window
from snowflake.snowpark import functions as F
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.types import IntegerType


def get_data_last_altered_timestamp(session: Session, source_table: str) -> str:
    """
    Get the last altered timestamp for a table and format it as a version string.

    This function queries the Snowflake INFORMATION_SCHEMA to find when a table was last modified
    and returns it in a version-friendly format suitable for dataset versioning.

    Args:
        session (Session): Snowflake session object
        source_table (str): Fully qualified or partially qualified table name

    Returns:
        str: Version string in format "v%Y%m%d_%H%M%S" based on last altered timestamp

    Raises:
        ValueError: If the table is not found in the database
    """
    db, schema, table = (
        resolve_identifier(id) if id else id
        for id in parse_schema_level_object_identifier(source_table)
    )
    if db is None:
        db = session.get_current_database()
    if schema is None:
        schema = session.get_current_schema()
    assert db is not None and schema is not None and table is not None

    table_info = session.sql(
        f"""
        SELECT LAST_ALTERED FROM {db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = '{db}'
            AND TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
        """
    ).collect()

    if len(table_info) == 0:
        raise ValueError(f"Table {source_table} not found")
    last_altered = datetime.fromisoformat(str(table_info[0][0]))

    return last_altered.strftime("v%Y%m%d_%H%M%S")


def get_raw_data(
    session: Session,
    table_name: str = DATA_TABLE_NAME,
    create_if_not_exists: bool = True,
    verbose: bool = False,
) -> DataFrame:
    """
    Prepares the raw data for feature engineering and model training.
    The function first attempts to load the table from Snowflake, and if it doesn't exist and
    create_if_not_exists is True, it will upload the data from a local CSV file.

    Args:
        session (Session): Snowflake session object
        table_name (str, optional): Name of the table to load or create. Defaults to DATA_TABLE_NAME.
        create_if_not_exists (bool, optional): Whether to create the table if it doesn't exist.
            Defaults to True.
        verbose (bool, optional): Whether to print detailed information about the loaded data.
            Defaults to False.

    Returns:
        DataFrame: Snowpark DataFrame containing the loaded data

    Raises:
        SnowparkSQLException: If the table doesn't exist and create_if_not_exists is False
        AssertionError: If the table exists but contains no data
    """
    try:
        df = session.table(table_name)
        assert df.count() > 0
    except (SnowparkSQLException, AssertionError):
        if not create_if_not_exists:
            raise
        print("Table not found! Uploading data to snowflake table")
        local_data_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "data",
            "MORTGAGE_LENDING_DEMO_DATA.csv.zip",
        )
        df_pandas = pd.read_csv(local_data_path)
        db, schema, table = parse_schema_level_object_identifier(table_name)
        session.write_pandas(
            df_pandas,
            table,
            database=db,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
        )
        df = session.table(table_name)
        assert df.count() > 0

    if verbose:
        print("Data loaded from Snowflake table:")
        print(f"Number of rows: {df.count()}")
        print(f"Number of columns: {len(df.columns)}")
        print(f"Columns: {df.columns}")
        print("Sample data:")
        df.show(5)

    return df


def get_feature_store(
    session: Session,
    name: str = FEATURE_STORE_NAME,
    warehouse: str = WAREHOUSE,
    *,
    create_if_not_exists: bool = True,
) -> feature_store.FeatureStore:
    """
    This function creates or retrieves a feature store instance for managing feature definitions
    and feature views. It configures the feature store with the appropriate database, schema,
    and warehouse settings.

    Args:
        session (Session): Snowflake session object
        name (str, optional): Fully qualified name of the feature store in format "database.schema".
            Defaults to FEATURE_STORE_NAME.
        warehouse (str, optional): Name of the warehouse to use for feature store operations.
            Defaults to WAREHOUSE.
        create_if_not_exists (bool, optional): Whether to create the feature store if it doesn't exist.
            Defaults to True.

    Returns:
        feature_store.FeatureStore: Configured feature store instance
    """
    _, db, schema = parse_schema_level_object_identifier(name)
    fs = feature_store.FeatureStore(
        session=session,
        database=db or session.get_current_database(),
        name=schema or session.get_current_schema(),
        default_warehouse=warehouse or session.get_current_warehouse(),
        creation_mode=(
            feature_store.CreationMode.CREATE_IF_NOT_EXIST
            if create_if_not_exists
            else feature_store.CreationMode.FAIL_IF_NOT_EXIST
        ),
    )
    return fs


def get_feature_view(
    session: Session,
    fs: Optional[feature_store.FeatureStore] = None,
    source_table: str = DATA_TABLE_NAME,
    name: str = "MORTGAGE_FEATURE_VIEW",
    force_refresh: bool = False,
    udf_stage: str = DAG_STAGE,
) -> feature_store.FeatureView:
    """
    This function creates a feature view from the data and registers it in the Snowflake Feature Store.
    It includes comprehensive feature engineering such as timestamp features, income/loan ratios,
    aggregated features, and categorical encoding. The feature view includes preprocessing steps
    like mean imputation and label encoding as part of the feature engineering pipeline.

    Args:
        session (Session): Snowflake session object
        fs (Optional[feature_store.FeatureStore], optional): Feature store instance to use.
            If None, creates a new one. Defaults to None.
        source_table (str, optional): Name of the source table containing raw data.
            Defaults to DATA_TABLE_NAME.
        name (str, optional): Name for the feature view. Defaults to "MORTGAGE_FEATURE_VIEW".
        force_refresh (bool, optional): Whether to force refresh by deleting existing feature view.
            Defaults to False.
        udf_stage (str, optional): Stage location for storing UDF code. Defaults to DAG_STAGE.

    Returns:
        feature_store.FeatureView: Registered feature view with engineered features
    """
    # Prepare the feature store
    fs = fs or get_feature_store(session, create_if_not_exists=True)
    version = get_data_last_altered_timestamp(session, source_table)

    # Try to get the existing feature view
    try:
        if force_refresh:
            fs.delete_feature_view(name, version)
        fv = fs.get_feature_view(name, version)
        return fv
    except ValueError:
        pass

    # Get the latest data from the table
    df = get_raw_data(session, source_table)

    # Feature engineering
    feature_eng_dict = dict()

    # Timestamp features
    feature_eng_dict["TIMESTAMP"] = F.to_timestamp("TS")
    feature_eng_dict["MONTH"] = F.month("TIMESTAMP")
    feature_eng_dict["DAY_OF_YEAR"] = F.dayofyear("TIMESTAMP")
    feature_eng_dict["DOTW"] = F.dayofweek("TIMESTAMP")

    # Income and loan features
    feature_eng_dict["LOAN_AMOUNT"] = F.col("LOAN_AMOUNT_000s") * 1000
    feature_eng_dict["INCOME"] = F.col("APPLICANT_INCOME_000s") * 1000
    feature_eng_dict["INCOME_LOAN_RATIO"] = F.col("INCOME") / F.col("LOAN_AMOUNT")

    county_window_spec = Window.partition_by("COUNTY_NAME")
    feature_eng_dict["MEAN_COUNTY_INCOME"] = F.avg("INCOME").over(county_window_spec)
    feature_eng_dict["HIGH_INCOME_FLAG"] = (
        F.col("INCOME") > F.col("MEAN_COUNTY_INCOME")
    ).astype(IntegerType())

    feature_eng_dict["AVG_THIRTY_DAY_LOAN_AMOUNT"] = F.sql_expr(
        """
        AVG(LOAN_AMOUNT) OVER (PARTITION BY COUNTY_NAME ORDER BY TIMESTAMP
        RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND CURRENT ROW)
        """
    )

    # Apply the initial feature engineering transformations
    df = df.with_columns(feature_eng_dict.keys(), feature_eng_dict.values())

    # Identify numeric columns that need imputation (those from feature engineering)
    numeric_feature_cols = [
        "MONTH",
        "DAY_OF_YEAR",
        "DOTW",
        "LOAN_AMOUNT",
        "INCOME",
        "INCOME_LOAN_RATIO",
        "MEAN_COUNTY_INCOME",
        "HIGH_INCOME_FLAG",
        "AVG_THIRTY_DAY_LOAN_AMOUNT",
    ]

    # Apply mean imputation to numeric features
    for col in numeric_feature_cols:
        # Calculate mean for imputation, handling nulls
        mean_value = df.select(F.avg(F.col(col)).alias("mean_val")).collect()[0][
            "MEAN_VAL"
        ]
        feature_eng_dict[col] = F.when(
            F.col(col).isNull(), F.lit(mean_value or 0)
        ).otherwise(F.col(col))

    # Apply the preprocessing transformations
    df = df.with_columns(feature_eng_dict.keys(), feature_eng_dict.values())

    # Label encode the categorical columns using custom UDF with sklearn LabelEncoder
    categorical_cols = {"LOAN_PURPOSE_NAME": "LOAN_PURPOSE_ENCODED"}

    # Create and apply label encoder UDF for each categorical column
    for cat_col, output_col in categorical_cols.items():
        print(f"Creating label encoder UDF for {cat_col}...")

        # Create the UDF based on unique values in the source table
        label_encoder_udf = create_label_encoder_udf(
            session,
            cat_col,
            source_table,
            prefix=f"label_encode_{version}",
            stage_location=udf_stage,
        )

        # Apply the UDF to encode the categorical column
        df = df.with_column(output_col, label_encoder_udf(F.col(cat_col)))

    # First try to retrieve an existing entity definition, if not define a new one and register
    try:
        # retrieve existing entity
        loan_id_entity = fs.get_entity("LOAN_ENTITY")
    except ValueError:
        # define new entity
        loan_id_entity = feature_store.Entity(
            name="LOAN_ENTITY",
            join_keys=["LOAN_ID"],
            desc="Features defined on a per loan level",
        )
        # register
        fs.register_entity(loan_id_entity)

    # Create a dataframe with just the ID, timestamp, and engineered features. We will use this to define our feature view
    feature_df = df.select(
        ["LOAN_ID"] + list(feature_eng_dict.keys()) + list(categorical_cols.values())
    )

    # define and register feature view
    loan_fv = feature_store.FeatureView(
        name=name,
        entities=[loan_id_entity],
        feature_df=feature_df,
        timestamp_col="TIMESTAMP",
        refresh_freq="1 day",
    )

    # add feature level descriptions

    loan_fv = loan_fv.attach_feature_desc(
        {
            "MONTH": "Month of loan",
            "DAY_OF_YEAR": "Day of calendar year of loan",
            "DOTW": "Day of the week of loan",
            "LOAN_AMOUNT": "Loan amount in $USD",
            "INCOME": "Household income in $USD",
            "INCOME_LOAN_RATIO": "Ratio of LOAN_AMOUNT/INCOME",
            "MEAN_COUNTY_INCOME": "Average household income aggregated at county level",
            "HIGH_INCOME_FLAG": "Binary flag to indicate whether household income is higher than MEAN_COUNTY_INCOME",
            "AVG_THIRTY_DAY_LOAN_AMOUNT": "Rolling 30 day average of LOAN_AMOUNT",
            "LOAN_PURPOSE_ENCODED": "Loan purpose name encoded using sklearn LabelEncoder",
        }
    )

    loan_fv = fs.register_feature_view(loan_fv, version=version, overwrite=True)
    print(f"Feature View created: {loan_fv.fully_qualified_name()}")

    return loan_fv


def generate_feature_dataset(
    session: Session,
    source_table: str = DATA_TABLE_NAME,
    name: str = "MORTGAGE_DATASET_EXTENDED_FEATURES",
    version: Optional[str] = None,
    *,
    create_assets: bool = True,
    force_refresh: bool = False,
) -> dataset.Dataset:
    """
    Generate a feature dataset with engineered features from the feature store.

    This function creates a dataset by combining raw data with features from the feature store.
    It uses the feature view to perform feature engineering and creates a complete dataset
    ready for machine learning model training.

    Args:
        session (Session): Snowflake session object
        source_table (str, optional): Name of the source table containing raw data.
            Defaults to DATA_TABLE_NAME.
        name (str, optional): Name for the generated dataset.
            Defaults to "MORTGAGE_DATASET_EXTENDED_FEATURES".
        version (Optional[str], optional): Version identifier for the dataset.
            If None, uses timestamp from source table. Defaults to None.
        create_assets (bool, optional): Whether to create necessary assets (feature store, tables).
            Defaults to True.
        force_refresh (bool, optional): Whether to force refresh of the feature view.
            Defaults to False.

    Returns:
        dataset.Dataset: Generated dataset with engineered features
    """
    fs = get_feature_store(session, create_if_not_exists=create_assets)
    df = get_raw_data(
        session, table_name=source_table, create_if_not_exists=create_assets
    )
    fv = get_feature_view(
        session, fs=fs, source_table=source_table, force_refresh=force_refresh
    )

    # Generate a dataset to pickup later for ML Modeling
    ds = fs.generate_dataset(
        name=session.get_fully_qualified_name_if_possible(name),
        version=version or get_data_last_altered_timestamp(session, source_table),
        spine_df=df.select(
            "LOAN_ID",
            F.col("TS").as_("TIMESTAMP"),
            "MORTGAGERESPONSE",
        ),  # only need the features used to fetch rest of feature view
        features=[fv],
        spine_timestamp_col="TIMESTAMP",
        spine_label_cols=["MORTGAGERESPONSE"],
    )
    print(
        f"Dataset created: {ds.fully_qualified_name} version {ds.selected_version.name}"
    )

    return ds


def split_dataset(ds: dataset.Dataset) -> Tuple[dataset.Dataset, dataset.Dataset]:
    """
    Split a dataset into training and test sets.

    This function takes a dataset and creates two new dataset versions: one for training
    and one for testing. The split is done randomly with 80% of data for training and 20% for testing.

    Args:
        ds (dataset.Dataset): Input dataset to split

    Returns:
        Tuple[dataset.Dataset, dataset.Dataset]: A tuple containing (train_dataset, test_dataset)
    """
    # Read the dataset into a Snowpark DataFrame and apply any transformations
    df = ds.read.to_snowpark_dataframe()

    # Split the dataset into training and test sets
    train_df, test_df = df.random_split(weights=[0.8, 0.2])

    train_ds = ds.create_version(
        f"{ds.selected_version.name}_train",
        train_df,
        exclude_cols=ds.selected_version.exclude_cols,
        label_cols=ds.selected_version.label_cols,
        comment=f"Training split for {ds.selected_version.name}",
    )

    test_ds = ds.create_version(
        f"{ds.selected_version.name}_test",
        test_df,
        exclude_cols=ds.selected_version.exclude_cols,
        label_cols=ds.selected_version.label_cols,
        comment=f"Testing split for {ds.selected_version.name}",
    )

    return train_ds, test_ds


def delete_dataset_versions(session: Session, name: str, *versions: str) -> None:
    """
    Delete specific versions of a dataset.

    This function attempts to delete one or more versions of a dataset. It gracefully handles
    cases where the dataset doesn't exist or specific versions cannot be deleted.

    Args:
        session (Session): Snowflake session object
        name (str): Name of the dataset
        *versions (str): Variable number of version names to delete
    """
    try:
        ds = dataset.Dataset.load(session, name)
    except DatasetNotExistError:
        return

    for version in versions:
        try:
            ds.delete_version(version)
        except DatasetError:
            pass


def create_label_encoder_udf(
    session: Session,
    column_name: str,
    table_name: str,
    stage_location: str = None,
    prefix: str = "label_encode",
) -> F.UserDefinedFunction:
    """
    This function analyzes the unique values in a categorical column and creates a UDF
    that maps each unique value to an integer. The UDF is registered in Snowflake and
    can be used for consistent label encoding across different operations.

    Args:
        session (Session): Snowpark session object
        column_name (str): Name of the categorical column to analyze
        table_name (str): Name of the table containing the data
        stage_location (str, optional): Stage location for permanent UDF storage.
            If None, creates a temporary UDF. Defaults to None.
        prefix (str, optional): Prefix for the UDF name. Defaults to "label_encode".

    Returns:
        F.UserDefinedFunction: Registered Snowpark UDF for label encoding
    """
    # Query to get unique categorical values
    query = f"""
        SELECT DISTINCT {column_name} as category_value
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        ORDER BY {column_name}
    """

    unique_values_df = session.sql(query)
    unique_values = [row["CATEGORY_VALUE"] for row in unique_values_df.collect()]

    label_mapping = {category: int(i) for i, category in enumerate(unique_values)}
    label_mapping[None] = -1  # Add mapping for null/missing values

    print(f"Label encoding mapping for column {column_name}: {label_mapping}")

    # Create the UDF function with embedded mapping
    def label_encode_categorical(value: str) -> int:
        """Label encode a categorical value using predefined mapping."""
        return label_mapping.get(value, -1)  # Return -1 for unknown values

    # Register the UDF
    label_encoder_udf = session.udf.register(
        label_encode_categorical,
        name=f"{DB_NAME}.{SCHEMA_NAME}.{prefix}_{column_name.lower()}",
        replace=True,
        immutable=True,
        is_permanent=bool(stage_location),
        stage_location=stage_location,
    )

    return label_encoder_udf
