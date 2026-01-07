import os
import sys
import argparse

from snowflake.snowpark import Session
from datasets import load_dataset, DatasetDict

# Allow imports from ../src (sibling directory)
# This is needed because Python only searches the script's own directory by default
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from prompt_utils import extract_SOAP_response


def load_data() -> DatasetDict:
    dataset = load_dataset("omi-health/medical-dialogue-to-soap-summary")
    for split in dataset.keys():
        dataset[split] = dataset[split].map(
            lambda x: extract_SOAP_response(x["soap"]),
            remove_columns=["soap"],
        )
    return dataset


def upload_data(session: Session, dataset: DatasetDict, prefix: str = "soap_data_", database: str = None, schema: str = None) -> None:
    for split, ds in dataset.items():
        df = session.write_pandas(
            ds.data.to_pandas(),
            prefix + split,
            auto_create_table=True,
            overwrite=True,
            use_logical_type=True,
            use_vectorized_scanner=True,
            database=database,
            schema=schema,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare dataset for fine tuning')
    parser.add_argument('-m', '--mode', choices=['local', 'snowflake'], default='local', help='Mode to run in')
    parser.add_argument('--database', help='Snowflake database (defaults to session default database)')
    parser.add_argument('--schema', help='Snowflake schema (defaults to session default schema)')
    args = parser.parse_args()

    dataset = load_data()
    if args.mode == 'snowflake':
        session = Session.builder.getOrCreate()
        upload_data(session, dataset, database=args.database, schema=args.schema)
    else:
        dataset.save_to_disk("soap_dataset")