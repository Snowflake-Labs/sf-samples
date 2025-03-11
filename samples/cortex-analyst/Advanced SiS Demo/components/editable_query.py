from typing import Tuple

import streamlit as st


def editable_query(sql: str, unique_key_suffix: str) -> Tuple[bool, str]:
    """
    Provides a simple interface for editing an SQL query within a Streamlit application.

    This function displays the provided SQL query and allows the user to toggle
    an option to edit the query. If the user chooses to edit the query, a text
    area is provided for editing, and a button is available to save the edits.

    Args:
        sql (str): The original SQL query to be displayed and potentially edited.
        unique_key_suffix (str): A unique suffix to be appended to the Streamlit widget keys to ensure
                                 uniqueness in the Streamlit app.

    Returns:
        Tuple[bool, str]: A tuple containing:
            - A boolean indicating whether the query was edited and saved
            - The SQL query string, which will be the edited query if changes were saved,
              otherwise the original query.
    """
    allow_query_edit = st.toggle(
        "Edit query", key=f"allow_query_edit_toggle_{unique_key_suffix}"
    )
    if allow_query_edit:
        edited_sql = st.text_area("Edit SQL", value=sql, height=300)
        rerun_btn = st.button("Save query edits 💾", use_container_width=True)
        if rerun_btn:
            return True, edited_sql
        return False, sql
    else:
        st.code(sql, language="sql")
        return False, sql
