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
from constants import (
    ENABLE_SMART_CHART_SUGGESTION,
    ENABLE_SMART_DATA_SUMMARY,
    ENABLE_SMART_FOLLOWUP_QUESTIONS_SUGGESTIONS,
)
from utils.analyst import get_send_analyst_request_fnc
from utils.db import cached_get_query_exec_result
from utils.llm import (
    get_chart_suggestion,
    get_question_suggestions,
    get_results_summary,
)
from utils.notifications import add_to_notification_queue, handle_notification_queue
from utils.plots import ChartConfigDict, plotly_fig_from_config
from utils.session_state import (
    get_last_chat_message_idx,
    get_semantic_model_desc_from_messages,
    last_chat_message_contains_sql,
    message_idx_to_question,
    update_analysts_sql_response_message_in_state,
)
from utils.storage.saved_answers import save_analyst_answer

# List of available semantic model paths in the format: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>
# Each path points to a YAML file defining a semantic model
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.RAW_DATA/revenue_timeseries.yaml"
]


def reset_session_state():
    """Reset important session state elements for this page."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.suggested_charts_memory = (
        {}
    )  # Stores chart suggestion for each message


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    # Set the title and introductory text of the app
    st.title("💬 Talk to your data")
    st.markdown(
        "Welcome to Cortex Analyst! Type your questions below to interact with your data. "
    )

    # Sidebar with a reset button
    with st.sidebar:
        st.selectbox(
            "Selected semantic model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],  # show only file name
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
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
        new_user_msg_idx = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], new_user_msg_idx)

    # Show progress indicator inside analyst chat message while waiting for response
    with st.chat_message("analyst"):
        # send prompt to Cortex Analyst and process the response
        get_and_display_analyst_response()

        # Show additional data summary if the last message contains SQL, and the proper feature flag is set
        if last_chat_message_contains_sql() and ENABLE_SMART_DATA_SUMMARY:
            get_and_display_smart_data_summary()

        # Show additional followup questions if the last message contains SQL, and the proper feature flag is set
        if (
            last_chat_message_contains_sql()
            and ENABLE_SMART_FOLLOWUP_QUESTIONS_SUGGESTIONS
        ):
            get_and_display_smart_followup_suggestions()

    # Rerun in order to refresh the whole UI
    st.rerun()


def get_and_display_analyst_response():
    """Send the promot to Cortex Analyst, display response and store it in session state as a new message."""
    with st.spinner("Waiting for Analyst's response..."):
        time.sleep(1)  # Spinner needs extra time to render properly
        response, error_msg = get_send_analyst_request_fnc()(st.session_state.messages)
        if error_msg is None:
            analyst_message = {
                "role": "analyst",
                "content": response["message"]["content"],
                "request_id": response["request_id"],
            }
        else:
            # If an error occured we treat it's content as analyst's message.
            analyst_message = {
                "role": "analyst",
                "content": [{"type": "text", "text": error_msg}],
                "request_id": response["request_id"],
            }
    # Update the session state by appending a new message object
    st.session_state.messages.append(analyst_message)

    # Display the message in UI
    display_message(analyst_message["content"], get_last_chat_message_idx())


def get_and_display_smart_data_summary():
    """Get data summary for the last message, display it and update the session state."""
    with st.spinner("Generating results summary..."):
        # Get cached SQL execution result
        sql_idx = [c["type"] for c in st.session_state.messages[-1]["content"]].index(
            "sql"
        )
        df, err_msg = cached_get_query_exec_result(
            st.session_state.messages[-1]["content"][sql_idx]["statement"]
        )
        # If query execution results in error, skip it
        if err_msg:
            return

        # Get get data summary response
        question = message_idx_to_question(get_last_chat_message_idx())
        results_summary, _ = get_results_summary(question, df)
        results_summary_text = f"__Results summary:__\n\n{results_summary}"

        # Update the last message in session state
        st.session_state.messages[-1]["content"].append(
            {"type": "text", "text": results_summary_text}
        )

        # Display in the UI
        st.divider()
        st.markdown(results_summary_text)


def get_and_display_smart_followup_suggestions():
    """Get smart followup questions for the last message and update the session state."""
    with st.spinner("Generating followup questions..."):
        question = message_idx_to_question(get_last_chat_message_idx())
        sm_description = get_semantic_model_desc_from_messages()
        suggestions, error_msg = get_question_suggestions(question, sm_description)
        # If suggestions were successfully generated update the session state
        if error_msg is None:
            st.session_state.messages[-1]["content"].append(
                {"type": "text", "text": "__Suggested followups:__"}
            )
            st.session_state.messages[-1]["content"].append(
                {"type": "suggestions", "suggestions": suggestions}
            )
        # Else show notification containing error message
        else:
            add_to_notification_queue(error_msg, "error")


def display_suggestions_buttons(suggestions: List[str], unique_key_suffix: str):
    """Display suggestions as buttons."""
    for suggestion_index, suggestion in enumerate(suggestions):
        if st.button(
            suggestion,
            key=f"suggestion_{suggestion_index}_{unique_key_suffix}",
            use_container_width=True,
            disabled=(
                st.session_state.messages[-1]["role"] == "user"
            ),  # Do not allow for clickig if the last message is chat is from user
        ):
            st.session_state.active_suggestion = suggestion


def display_message(content: List[Dict[str, str]], message_index: int):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
    """
    for item in content:
        if item["type"] == "text":
            # Add an extra divider before "Suggested followups" section.
            if item["text"] == "__Suggested followups:__" or item["text"].startswith(
                "__Results summary:__"
            ):
                st.divider()
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            display_suggestions_buttons(item["suggestions"], f"{message_index}")
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
                st.dataframe(df)

            with chart_tab:
                plot_cfg = show_chart_tab(df, message_index)

    save_to_favorites = st.button(
        "⭐ Save this query and chart ⭐",
        key=f"sql_msg__save_query_btn__{message_index}",
        type="secondary",
        use_container_width=True,
    )
    if save_to_favorites:
        question = message_idx_to_question(message_index)
        save_query(prompt=question, sql=sql, df=df, plot_config=plot_cfg)


@st.experimental_fragment
def show_chart_tab(df: pd.DataFrame, message_index: int) -> ChartConfigDict:
    """
    Render the chart tab containing chart picker controls and a chart preview.

    This function displays a chart tab with controls for selecting a chart type and
    a preview of the selected chart. If the smart chart suggestion feature is enabled,
    it will use a suggested chart configuration as the default.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data to be visualized.
        message_index (int): The index of the message for which the chart is being generated.

    Returns:
        ChartConfigDict: The configuration dictionary for the selected chart.
    """
    default_plot_cfg = None

    # If smart chart suggestion is enabled, get the default chart-picker configuration
    if ENABLE_SMART_CHART_SUGGESTION:
        question = message_idx_to_question(message_index)
        default_plot_cfg = get_suggested_plot_config(question, df, message_index)

    # Display the chart picker and get the selected chart configuration
    plot_cfg = chart_picker(
        df, default_config=default_plot_cfg, component_idx=message_index
    )
    # If a chart type is selected, generate and display the chart
    if plot_cfg.get("type") is not None:
        plotly_fig = plotly_fig_from_config(df, plot_cfg)
        st.plotly_chart(plotly_fig)

    return plot_cfg


def get_suggested_plot_config(
    question: str, df: pd.DataFrame, message_index: int
) -> ChartConfigDict:
    """
    Get the suggested chart configuration and store it in the session state.

    This function retrieves the suggested chart configuration for a given question and
    DataFrame, and stores it in the session state to avoid repeated calls to the LLM.

    Parameters:
        question (str): The question for which the chart suggestion is being generated.
        df (pd.DataFrame): The DataFrame containing the data to be visualized.
        message_index (int): The index of the message for which the chart is being generated.

    Returns:
        ChartConfigDict: The suggested chart configuration dictionary.
    """
    # Initialize the session state for storing suggested chart configurations if not already present
    if "suggested_charts_memory" not in st.session_state:
        st.session_state["suggested_charts_memory"] = {}

    # Retrieve the suggested chart configuration from the session state if available
    suggested_config = st.session_state["suggested_charts_memory"].get(message_index)

    # If no configuration is stored, generate a new one and store it in the session state
    if suggested_config is None:
        suggested_config, _ = get_chart_suggestion(question, df)
        st.session_state["suggested_charts_memory"][message_index] = suggested_config

    return suggested_config


st.set_page_config(layout="centered")
handle_notification_queue()

# Initialize session state
if "messages" not in st.session_state:
    reset_session_state()

show_header_and_sidebar()
display_conversation()
handle_user_inputs()
if len(st.session_state.messages) == 0:
    process_user_input("What questions can I ask?")
