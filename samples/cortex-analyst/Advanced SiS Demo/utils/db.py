"""
This module provides utility functions for interacting with a Snowflake database using Snowpark.

It includes functions to check if a table exists, retrieve the current timestamp, execute SQL queries,
and cache query results using Streamlit's caching mechanism.
"""

from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

from constants import DEV_SNOWPARK_CONNECTION_NAME
from utils.misc import is_local


def get_sf_connection() -> Session:
    """Get Snowpark Session objects. Works in both local and SiS setups."""
    if is_local():
        return _get_session_local()
    else:
        return get_active_session()


def _get_session_local() -> Session:
    return Session.builder.config(
        "connection_name", DEV_SNOWPARK_CONNECTION_NAME
    ).create()


def table_exists(session: Session, table_path: str) -> bool:
    """
    Check if a table exists in Snowflake.

    This function attempts to query the table with a limit of 0 rows to determine if it exists.

    Args:
        session (snowflake.snowpark.Session): Snowpark session object.
        table_path (str): Full path to the table (e.g. "mydatabase.myschema.mytable").

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        session.table(table_path).limit(0).collect()
        return True
    except SnowparkSQLException:
        return False


def get_current_timestamp(session: Session) -> datetime:
    """
    Retrieve the current timestamp from the Snowflake database.

    This function executes a SQL query to get the current timestamp from the database.

    Args:
        session (snowflake.snowpark.Session): Snowpark session object.

    Returns:
        datetime: The current timestamp from the database.

    Raises:
        ValueError: If the query fails to retrieve the current timestamp.
    """
    query = "SELECT CURRENT_TIMESTAMP"
    result = session.sql(query).collect()
    if result:
        timestamp = result[0][0]
        return timestamp
    else:
        raise ValueError("Failed to retrieve current timestamp from the database")


def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute the SQL query and convert the results to a pandas DataFrame.

    This function uses the Snowflake session to execute the query and handle any exceptions.

    Args:
        query (str): The SQL query.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results as a pandas DataFrame and an error message if any.
    """
    session = get_sf_connection()
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


@st.cache_data(show_spinner=False, ttl=3600)
def cached_get_query_exec_result(
    query: str,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Get cached version of get_query_exec_result to improve performance.

    This function uses Streamlit's caching mechanism to cache the results of the query execution.

    Args:
        query (str): The SQL query.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results as a pandas DataFrame and an error message if any.
    """
    return get_query_exec_result(query)
