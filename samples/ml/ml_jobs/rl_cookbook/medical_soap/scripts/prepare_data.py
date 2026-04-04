#!/usr/bin/env python3
"""Upload medical SOAP training data to Snowflake tables.

Usage:
    SNOWFLAKE_DEFAULT_CONNECTION_NAME=preprod8 python scripts/prepare_data.py \
        --data-dir /code/users/thonguyen/sf-samples/samples/ml/ml_jobs/rl_finetune/data
"""
import argparse
import json
import os

import pandas as pd
from snowflake.snowpark import Session


def upload_json_to_table(session, json_path, table_name, database=None, schema=None):
    """Upload a JSON file to a Snowflake table."""
    with open(json_path) as f:
        data = json.load(f)

    print(f"  Loaded {len(data)} records from {json_path}")
    print(f"  Keys: {list(data[0].keys())}")

    df = pd.DataFrame(data)
    # Uppercase column names for Snowflake
    df.columns = [c.upper() for c in df.columns]

    print(f"  Uploading to {table_name}...")
    session.write_pandas(
        df,
        table_name,
        auto_create_table=True,
        overwrite=True,
        database=database,
        schema=schema,
    )
    print(f"  Uploaded {len(df)} rows to {table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload medical SOAP data to Snowflake")
    parser.add_argument(
        "--data-dir",
        default="/code/users/thonguyen/sf-samples/samples/ml/ml_jobs/rl_finetune/data",
        help="Directory containing synthetic_train_data.json and synthetic_test_data.json",
    )
    parser.add_argument("--database", default="RL_TRAINING_DB")
    parser.add_argument("--schema", default="RL_SCHEMA")
    parser.add_argument("--train-table", default="MEDICAL_SOAP_TRAIN")
    parser.add_argument("--test-table", default="MEDICAL_SOAP_TEST")
    args = parser.parse_args()

    connection_name = os.getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
    builder = Session.builder
    if connection_name:
        builder = builder.config("connection_name", connection_name)
    if args.database:
        builder = builder.config("database", args.database)
    if args.schema:
        builder = builder.config("schema", args.schema)
    builder = builder.config("role", "SYSADMIN")
    session = builder.create()

    if args.database:
        session.sql(f"USE DATABASE {args.database}").collect()
    if args.schema:
        session.sql(f"USE SCHEMA {args.schema}").collect()

    print("Uploading medical SOAP training data...")

    train_path = os.path.join(args.data_dir, "synthetic_train_data.json")
    test_path = os.path.join(args.data_dir, "synthetic_test_data.json")

    upload_json_to_table(session, train_path, args.train_table, args.database, args.schema)
    upload_json_to_table(session, test_path, args.test_table, args.database, args.schema)

    # Verify
    for table in [args.train_table, args.test_table]:
        result = session.sql(f"SELECT COUNT(*) FROM {table}").collect()
        print(f"  {table}: {result[0][0]} rows")

    print("Done.")
