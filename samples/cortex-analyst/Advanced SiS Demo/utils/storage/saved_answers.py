"""
This module provides utility functions for handling actions related to adding, deleting, retrieving, and editing analyst responses and user charts in a Snowflake database.

It includes a data class for saved queries and functions to build SQL queries for various operations,
as well as functions to interact with the Snowflake database.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.row import Row

from constants import APP_SCHEMA_PATH, SAVED_QUERIES_TABLE_NAME
from utils.db import get_sf_connection, table_exists


@dataclass
class SavedQuery:
    """
    A data class to store details of a saved query.

    Attributes:
        id (int): The unique identifier of the saved query.
        added_on (datetime): The timestamp when the query was added.
        user (str): The user who saved the query.
        prompt (str): The prompt associated with the query.
        sql (str): The SQL query.
        plot_config (Dict): The configuration for the plot.
        serialized_plot (Dict): The serialized plot data.
        is_shared (bool): Whether the query is shared.
        semantic_model_path (str): The path to the semantic model.
        plot_rendered_on (datetime): The timestamp when the plot was rendered.
    """

    id: int
    added_on: datetime
    user: str
    prompt: str
    sql: str
    plot_config: Dict
    serialized_plot: Dict
    is_shared: bool
    semantic_model_path: str
    plot_rendered_on: datetime

    @classmethod
    def from_data_row(cls, row: Row) -> "SavedQuery":
        """
        Create a SavedQuery instance from a data row.

        Args:
            row (Dict[str, str]): A dictionary representing a row of data.

        Returns:
            SavedQuery: An instance of SavedQuery.
        """
        return cls(
            id=row["ID"],
            added_on=row["ADDED_ON"],
            user=row["USER"],
            prompt=row["PROMPT"],
            sql=row["SQL"],
            plot_config=json.loads(row["PLOT_CONFIG"]),
            serialized_plot=json.loads(row["SERIALIZED_PLOT"]),
            is_shared=row["IS_SHARED"],
            semantic_model_path=row["SEMANTIC_MODEL_PATH"],
            plot_rendered_on=row["PLOT_RENDERED_ON"],
        )


def _build_create_saved_queries_table_sql(table_path: str) -> str:
    return f"""
    CREATE TABLE {table_path} (
        id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
        added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        USER STRING,
        PROMPT STRING,
        SQL STRING,
        PLOT_CONFIG STRING,
        SERIALIZED_PLOT STRING,
        IS_SHARED BOOLEAN,
        SEMANTIC_MODEL_PATH STRING,
        PLOT_RENDERED_ON TIMESTAMP
    );
    """


def _build_insert_to_saved_queries_table_sql(
    table_path: str,
    user: str,
    prompt: str,
    sql: str,
    serialized_plot_cfg: str,
    serialized_plot: str,
    is_shared: bool,
    semantic_model_path: str,
) -> str:
    return f"""
    INSERT INTO {table_path} (
        USER,
        PROMPT,
        SQL,
        PLOT_CONFIG,
        SERIALIZED_PLOT,
        IS_SHARED,
        SEMANTIC_MODEL_PATH,
        PLOT_RENDERED_ON
    ) VALUES (
        '{user}',
        '{prompt}',
        $${sql}$$,
        $${serialized_plot_cfg}$$,
        $${serialized_plot}$$,
        {is_shared},
        '{semantic_model_path}',
        CURRENT_TIMESTAMP
    );
    """


def _build_retrieve_shared_queries_sql(table_path: str) -> str:
    return f"""
    SELECT * FROM {table_path}
    WHERE IS_SHARED = TRUE
    ORDER BY ADDED_ON DESC;
    """


def _build_retrieve_user_queries_sql(table_path: str, user: str) -> str:
    return f"""
    SELECT * FROM {table_path}
    WHERE USER = '{user}'
    ORDER BY ADDED_ON DESC;
    """


def _build_delete_query_by_id_sql(table_path: str, id: int) -> str:
    return f"""
    DELETE FROM {table_path}
    WHERE ID = {id};
    """


def _build_update_query_by_id_sql(table_path: str, saved_query: SavedQuery) -> str:
    plot_cfg_as_str = json.dumps(saved_query.plot_config)
    serialized_plot_as_str = json.dumps(saved_query.serialized_plot)
    return f"""
    UPDATE {table_path}
    SET
        USER = '{saved_query.user}',
        PROMPT = '{saved_query.prompt}',
        SQL = $${saved_query.sql}$$,
        PLOT_CONFIG = $${plot_cfg_as_str}$$,
        SERIALIZED_PLOT = $${serialized_plot_as_str}$$,
        IS_SHARED = {saved_query.is_shared},
        SEMANTIC_MODEL_PATH = '{saved_query.semantic_model_path}',
        PLOT_RENDERED_ON = '{saved_query.plot_rendered_on}'
    WHERE ID = {saved_query.id};
    """


def save_analyst_answer(
    user: str,
    prompt: str,
    sql: str,
    serialized_plot_cfg: str,
    serialized_plot: str,
    semantic_model_path: str,
    is_shared: bool = False,
) -> Tuple[bool, str]:
    """
    Save the analyst's answer to a question in the form of a SQL query and an optional plot.

    Args:
        user (str): The username of the analyst.
        prompt (str): The question prompt.
        sql (str): The SQL query.
        serialized_plot_cfg (str): The serialized plot configuration.
        serialized_plot (str): The serialized plot.
        semantic_model_path (str): The path to the semantic model.
        is_shared (bool, optional): Whether the query is shared. Defaults to False.

    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating success and a string containing an error message if applicable.
    """
    session: Session = get_sf_connection()
    table_path = f"{APP_SCHEMA_PATH}.{SAVED_QUERIES_TABLE_NAME}"
    if not table_exists(session, table_path):
        create_table_query = _build_create_saved_queries_table_sql(table_path)
        try:
            _ = session.sql(create_table_query).collect()
        except SnowparkSQLException as e:
            return False, str(e)

    insert_data_query = _build_insert_to_saved_queries_table_sql(
        table_path=table_path,
        user=user,
        prompt=prompt,
        sql=sql,
        serialized_plot_cfg=serialized_plot_cfg,
        serialized_plot=serialized_plot,
        is_shared=is_shared,
        semantic_model_path=semantic_model_path,
    )
    try:
        _ = session.sql(insert_data_query).collect()
    except SnowparkSQLException as e:
        return False, str(e)

    return True, ""


def get_user_saved_queries(session: Session, user: str) -> List[SavedQuery]:
    """
    Get a list of saved queries for a specific user.

    Args:
        session (Session): The Snowflake session.
        user (str): The username of the analyst.

    Returns:
        List[SavedQuery]: A list of saved queries for the specified user.
    """
    table_path = f"{APP_SCHEMA_PATH}.{SAVED_QUERIES_TABLE_NAME}"
    if not table_exists(session=session, table_path=table_path):
        return []
    query = _build_retrieve_user_queries_sql(table_path, user)
    result = session.sql(query).collect()

    saved_queries = [SavedQuery.from_data_row(row) for row in result]

    return saved_queries


def get_all_shared_queries(session: Session) -> List[SavedQuery]:
    """
    Get a list of all shared queries.

    Args:
        session (Session): The Snowflake session.

    Returns:
        List[SavedQuery]: A list of all shared queries.
    """
    table_path = f"{APP_SCHEMA_PATH}.{SAVED_QUERIES_TABLE_NAME}"
    if not table_exists(session=session, table_path=table_path):
        return []
    query = _build_retrieve_shared_queries_sql(table_path)
    result = session.sql(query).collect()

    saved_queries = [SavedQuery.from_data_row(row) for row in result]

    return saved_queries


def delete_saved_query_by_id(session: Session, id: int) -> Tuple[bool, str]:
    """
    Delete a saved query by its ID.

    Args:
        session (Session): The Snowflake session.
        id (int): The ID of the saved query.

    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating success and a string containing an error message if applicable.
    """
    table_path = f"{APP_SCHEMA_PATH}.{SAVED_QUERIES_TABLE_NAME}"
    delete_query = _build_delete_query_by_id_sql(table_path, id)

    try:
        _ = session.sql(delete_query).collect()
    except SnowparkSQLException as e:
        return False, str(e)

    return True, ""


def update_saved_query_by_id(
    session: Session, saved_query: SavedQuery
) -> Tuple[bool, str]:
    """
    Update a saved query by its ID.

    Args:
        session (Session): The Snowflake session.
        saved_query (SavedQuery): The updated saved query.

    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating success and a string containing an error message if applicable.
    """
    table_path = f"{APP_SCHEMA_PATH}.{SAVED_QUERIES_TABLE_NAME}"
    update_query = _build_update_query_by_id_sql(table_path, saved_query)
    try:
        _ = session.sql(update_query).collect()
    except SnowparkSQLException as e:
        return False, str(e)

    return True, ""
