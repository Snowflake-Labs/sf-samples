"""
Setup feature store for Snowflake ML Demo

This script performs one-time setup tasks required for the demo:
- Creates feature store
- Creates feature views
- Grants privileges to service user

Usage:
    python setup_feature_store.py

Required Environment Variables:
    SNOWFLAKE_ROLE - Role to be used (default: SYSADMIN)
    SNOWFLAKE_DATABASE - Database name (default: DEMO_DB)
    SNOWFLAKE_SCHEMA - Schema name (default: DAG_DEMO)
    SNOWFLAKE_WAREHOUSE - Warehouse name (default: DEMO_WH)
"""

import os
import sys

from snowflake.ml._internal.utils.sql_identifier import SqlIdentifier
from snowflake.ml.feature_store.access_manager import _ALL_OBJECTS
from snowflake.ml.feature_store.access_manager import \
    _PRE_INIT_PRIVILEGES as FS_PRIVILEGES
from snowflake.ml.feature_store.access_manager import \
    _FeatureStoreRole as FSRole
from snowflake.ml.feature_store.access_manager import _grant_privileges
from snowflake.ml.feature_store.access_manager import _Privilege as FSPrivilege
from snowflake.ml.feature_store.access_manager import _SessionInfo as FSInfo
from snowflake.snowpark import Session

sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "src")))

from constants import (DATA_TABLE_NAME, DB_NAME, FEATURE_STORE_NAME, ROLE_NAME,
                       SCHEMA_NAME, WAREHOUSE)
from data import get_feature_store, get_feature_view, get_raw_data


UDF_STAGE = f"@{DB_NAME}.{SCHEMA_NAME}.UDF_STAGE"


def main(connection_name: str, reader_roles: list[str]):
    session_builder = Session.builder
    if connection_name:
        session_builder = session_builder.config("connection_name", connection_name)
    session = session_builder.getOrCreate()
    session.use_role(ROLE_NAME)
    session.use_warehouse(WAREHOUSE)

    # Create UDF stage
    session.sql(f"CREATE STAGE IF NOT EXISTS {UDF_STAGE.lstrip('@')}").collect()

    fs = get_feature_store(session, name=FEATURE_STORE_NAME, warehouse=WAREHOUSE, create_if_not_exists=True)
    df = get_raw_data(session, table_name=DATA_TABLE_NAME, create_if_not_exists=True, verbose=False)
    _ = get_feature_view(session, fs=fs, source_table=DATA_TABLE_NAME, udf_stage=UDF_STAGE)

    if reader_roles:
        fs_info = FSInfo(
            database=SqlIdentifier(DB_NAME),
            schema=SqlIdentifier(SCHEMA_NAME),
            warehouse=SqlIdentifier(WAREHOUSE)
        )
        privileges = [
            # HACK: Skip grants on FUTURE objects to avoid need for ACCOUNTADMIN privileges
            FSPrivilege("ALL", p.object_type + "S", p.privileges, p.scope, p.optional)
            if p.object_name == _ALL_OBJECTS
            else p
            for p in FS_PRIVILEGES[FSRole.CONSUMER]
        ]
        for role in reader_roles:
            print(f"Granting Feature Store Consumer access to role {role}")
            _grant_privileges(session, role, privileges, fs_info)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Setup feature store and grant reader access')
    parser.add_argument("-c", "--connection", type=str, help="Connection name")
    parser.add_argument('--reader-roles', nargs='*', help='List of role names to grant reader access', default=[])
    args = parser.parse_args()

    main(args.connection, args.reader_roles)