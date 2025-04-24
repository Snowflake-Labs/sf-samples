"""
Distributed XGBoost Training Example using multiple nodes
"""

import time
from snowflake.snowpark import Session
from snowflake.ml.modeling.distributors.xgboost import XGBEstimator, XGBScalingConfig
from snowflake.ml.data.data_connector import DataConnector

# Generate a synthetic dataset for demo purposes
def generate_dataset_sql(db, schema, table_name, num_rows, num_cols) -> str:
    sql_script = f"CREATE TABLE IF NOT EXISTS {db}.{schema}.{table_name} AS \n"
    sql_script += f"SELECT \n"
    for i in range(1, num_cols):
        sql_script += f"uniform(0::FLOAT, 10::FLOAT, random()) AS FEATURE_{i}, \n"
    sql_script += f"FEATURE_1 + FEATURE_1 AS TARGET_1 \n"
    sql_script += f"FROM TABLE(generator(rowcount=>({num_rows})));"
    return sql_script

# Define a function to train XGBoost with different scaling configurations
def xgb_train(cpu_train_df, input_cols, label_col, num_workers, num_cpu_per_worker):
    print(f"Training with num_workers={num_workers}, num_cpu_per_worker={num_cpu_per_worker}")

    # XGBoost parameters
    params = {
        "tree_method": "hist",
        "objective": "reg:pseudohubererror",
        "eta": 1e-4,
        "subsample": 0.5,
        "max_depth": 50,
        "max_leaves": 1000,
        "max_bin": 63,
    }

    # Configure Ray scaling for XGBoost
    scaling_config = XGBScalingConfig(
        num_workers=num_workers,
        num_cpu_per_worker=num_cpu_per_worker,
        use_gpu=False
    )

    # Create and configure the estimator
    estimator = XGBEstimator(
        n_estimators=100,
        params=params,
        scaling_config=scaling_config,
    )

    # Create a data connector using our custom Ray ingester
    data_connector = DataConnector.from_dataframe(
        cpu_train_df
    )

    # Train the model
    print("Starting training...")
    start_time = time.time()
    xgb_model = estimator.fit(
        data_connector, input_cols=input_cols, label_col=label_col
    )
    end_time = time.time()
    print(f"Training completed in {end_time - start_time:.2f} seconds")

    return xgb_model

def main():
    # Configure dataset parameters
    num_rows = 1000 * 1000
    num_cols = 100
    table_name = "MULTINODE_CPU_TRAIN_DS"

    # Create the dataset in Snowflake
    session = Session.builder.getOrCreate()
    session.sql(generate_dataset_sql(session.get_current_database(), session.get_current_schema(),
                            table_name, num_rows, num_cols)).collect()

    # Load the dataset into a dataframe
    cpu_train_df = session.table(table_name)
    feature_list = [f'FEATURE_{num}' for num in range(1, num_cols)]
    INPUT_COLS = feature_list
    LABEL_COL = "TARGET_1"

    # Try different scaling configurations
    print("Testing automatic worker allocation")
    auto_model = xgb_train(cpu_train_df, INPUT_COLS, LABEL_COL, -1, -1)  # Auto-determine workers based on cluster

    return auto_model

if __name__ == "__main__":
    main()
