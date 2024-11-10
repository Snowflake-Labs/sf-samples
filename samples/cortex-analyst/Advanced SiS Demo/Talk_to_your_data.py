"""
Cortex Analyst App
====================
This app allows users to interact with their data using natural language.
"""

import json
import time
from typing import Dict, List

import pandas as pd
import streamlit as st
from plotly.io import to_json

from components.chart_picker import chart_picker
from components.editable_query import editable_query
from constants import AVAILABLE_SEMANTIC_MODELS_PATHS
from utils.analyst import get_send_analyst_request_fnc
from utils.db import cached_get_query_exec_result
from utils.plots import plotly_fig_from_config
from utils.session_state import (
    message_idx_to_question,
    update_analysts_sql_response_message_in_state,
)
from utils.storage.saved_answers import save_analyst_answer


def reset_session_state():
    """Reset important session state elements for this page."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    # Set the title and introductory text of the app
    st.title("Cortex Analyst")
    st.markdown(
        "Welcome to Cortex Analyst! Type your questions below to interact with your data. "
    )

    # Sidebar with a reset button
    with st.sidebar:
        st.selectbox(
            "Selected semantic model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        # Center this button
        if st.button("Clear Chat History", type="primary", use_container_width=True):
            reset_session_state()


def handle_user_inputs():
    """Handle user inputs from the chat interface."""
    # Handle chat input
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    # Handle suggested question click
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def process_user_input(prompt: str):
    """
    Process user input and update the conversation history.

    Args:
        prompt (str): The user's input.
    """
    # Create a new message, append to history and display imidiately
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        display_message(new_user_message["content"], len(st.session_state.messages) - 1)

    # Show progress indicator inside analyst chat message while waiting for response
    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):
            time.sleep(1)
            response, error_msg = get_send_analyst_request_fnc()(
                st.session_state.messages
            )
            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": response["message"]["content"],
                    "request_id": response["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response["request_id"],
                }
            st.session_state.messages.append(analyst_message)
            display_message(analyst_message["content"], 100)


def display_message(content: List[Dict[str, str]], message_index: int):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
    """
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            # Display suggestions as buttons
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion,
                    key=f"suggestion_{message_index}_{suggestion_index}",
                    use_container_width=True,
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            # Display the SQL query and results
            display_sql_query(item["statement"], message_index)
        else:
            # Handle other content types if necessary
            pass


def display_conversation():
    """Display the conversation history between the user and the assistant."""
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            display_message(content, idx)


def save_query(
    prompt: str,
    sql: str,
    df: pd.DataFrame,
    plot_config: Dict,
):
    """Save the query in user's "favorites" collection."""
    if plot_config.get("type") is not None:
        fig = plotly_fig_from_config(df, plot_config)
        serialized_plot = json.dumps(to_json(fig))
    else:
        serialized_plot = "{}"

    serialized_plot_cfg = json.dumps(plot_config)
    semantic_model_path = st.session_state.selected_semantic_model_path
    success, err_msg = save_analyst_answer(
        user=st.experimental_user.get("user_name", "UNKNOWN USER"),
        prompt=prompt,
        sql=sql,
        serialized_plot_cfg=serialized_plot_cfg,
        serialized_plot=serialized_plot,
        semantic_model_path=semantic_model_path,
    )
    if success:
        st.toast("Query saved!", icon="ℹ️")
    else:
        st.toast(f"Could not save the query, error msg: {err_msg}", icon="🚨")


def display_sql_query(sql: str, message_index: int):
    """
    Displat a component representing SQL message.

    It includes:
    * Generated SQL code
    * Data frame with execution results
    * Chart creator
    * Button to save this query

    Args:
        sql (str): The SQL query.
        message_index (int): The index of the message.
    """
    # Display the SQL query
    with st.expander("SQL Query", expanded=False):
        query_edit_btn, edited_sql = editable_query(sql, f"chat_{message_index}")
        if query_edit_btn:
            # We need to update source message object in order to persist edits app between rerenders
            update_analysts_sql_response_message_in_state(edited_sql, message_index)
            sql = edited_sql

    # Display the results of the SQL query
    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            df, err_msg = cached_get_query_exec_result(sql)
            if df is None:
                st.error(f"Could not execute generated SQL query. Error: {err_msg}")
                return

            if df.empty:
                st.write("Query returned no data")
                return

            # Show query results in two tabs
            data_tab, chart_tab = st.tabs(["Data 📄", "Chart 📉"])
            with data_tab:
                st.dataframe(df, use_container_width=True)

            with chart_tab:
                plot_cfg = chart_picker(df, component_idx=message_index)
                if plot_cfg.get("type") is not None:
                    plotly_fig = plotly_fig_from_config(df, plot_cfg)
                    st.plotly_chart(plotly_fig)

    save_to_favorites = st.button(
        "⭐ Save this query and chart ⭐",
        key=f"sql_msg__save_query_btn__{message_index}",
        type="secondary",
        use_container_width=True,
    )
    if save_to_favorites:
        question = message_idx_to_question(message_index)
        save_query(prompt=question, sql=sql, df=df, plot_config=plot_cfg)


st.set_page_config(layout="centered")

# Initialize session state
if "messages" not in st.session_state:
    reset_session_state()

show_header_and_sidebar()
display_conversation()
if len(st.session_state.messages) == 0:
    process_user_input("What questions can I ask?")

handle_user_inputs()
# handle_data2answer_request()
# handle_followup_questions_request()
