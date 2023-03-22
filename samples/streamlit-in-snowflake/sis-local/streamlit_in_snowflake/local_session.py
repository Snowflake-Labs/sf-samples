from __future__ import annotations

from functools import reduce
from typing import Mapping

import streamlit as st
from snowflake.snowpark import Session


# deep_get can can iterate through nested dictionaries, used to get credentials in the
# case of a multi-level secrets.toml hierarchy
def deep_get(dictionary, keys, default={}):
    return reduce(
        lambda d, key: d.get(key, default) if isinstance(d, Mapping) else default,
        keys.split("."),
        dictionary,
    )


# Initialize the connectionoptionally provide the connection_name
# (used to look up credentials in secrets.toml)
# connect() returns a snowpark session object -
# it checks session state for an existing object and tries to initialize
# one if not yet created. It checks secrets.toml for credentials and provides a
# more descriptive error if credentials are missing or path is misconfigured
# (otherwise you get a "User is empty" error)
def get_local_session(connection_name="connections.snowflake") -> Session:
    """
    Get a local Snowpark session from session_state. Builds one from
    secrets.toml values if it doesn't yet exist. Intended to replace
    `snowflake.snowpark.context.get_active_session()` when developing
    locally. Returns an exception and halts execution if credentials
    are not found.

    Parameters
    ----------
    connection_name: str
        The heading to use for looking up Snowflake connection
        configuration from secrets.toml. Defaults to
        "connections.snowflake".
    
    Example
    -------
    >>> from streamlit_in_snowflake.local_session import get_local_session
    >>>
    >>> session = get_local_session()
    >>> df = session.sql("select 1/2")
    """

    if "snowpark_session" not in st.session_state:
        creds = deep_get(st.secrets, connection_name)

        if not creds:
            st.exception(
                ValueError(
                    "Unable to initialize connection to Snowflake, did not "
                    "find expected credentials in secret "
                    f"{connection_name}. "
                    "Try updating your secrets.toml"
                )
            )
            st.stop()
        st.session_state.snowpark_session = Session.builder.configs(creds).create()
    return st.session_state.snowpark_session
