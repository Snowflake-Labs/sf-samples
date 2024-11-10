"""This module provides utility functions for managing Streamlit session state, specifically for handling and updating messages in the session state."""

import streamlit as st


def message_idx_to_question(idx: int) -> str:
    """
    Retrieve the question text from a message in the session state based on its index.

    This function checks the role of the message and returns the appropriate text:
    * If the message is from the user, it returns the prompt content
    * If the message is from the analyst and contains an interpretation of the question, it returns
    the interpreted question
    * Otherwise, it returns the previous user prompt

    Args:
        idx (int): The index of the message in the session state.

    Returns:
        str: The question text extracted from the message.
    """
    msg = st.session_state.messages[idx]

    # if it's user message, just return prompt content
    if msg["role"] == "user":
        return str(msg["content"]["text"])

    # If it's analyst response, if it's possibleget question interpretation from Analyst
    if msg["content"][0]["text"].startswith(
        "This is our interpretation of your question:"
    ):
        return str(
            msg["content"][0]["text"]
            .strip("This is our interpretation of your question:\n")
            .strip("\n")
            .strip("_")
        )

    # Else just return previous user prompt
    return str(st.session_state.messages[idx - 1]["content"]["text"])


def update_analysts_sql_response_message_in_state(new_sql: str, idx: int):
    """
    Update the SQL statement in an analyst's response message in the session state.

    This function locates the analyst's message by its index and updates the SQL
    statement within the message content. It assumes that the SQL statement is
    the first content of type 'sql' in the message.

    Args:
        new_sql (str): The new SQL statement to be updated in the message.
        idx (int): The index of the analyst's message in the session state.
    """
    all_messages = st.session_state.get("messages", [])
    try:
        message = all_messages[idx]
    except IndexError:
        return

    # Check if this message is from Analyst, and if it contains sql content
    if message["role"] == "analyst":
        for content_idx, content_obj in enumerate(message["content"]):
            # For now it's save to assume that we want to change only first sql content
            if content_obj["type"] == "sql":
                st.session_state.messages[idx]["content"][content_idx][
                    "statement"
                ] = new_sql
