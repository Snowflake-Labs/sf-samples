import json
import pandas as pd
import requests
import snowflake.connector
import streamlit as st
import time

@st.cache_resource()
def get_snowflake_connection():
    # see this link for connection using connections.toml
    # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file
    CONN = snowflake.connector.connect()
    return CONN

CONN = get_snowflake_connection()

HOST = f"{CONN.account}.snowflakecomputing.com"
DATABASE = "TB_101"
SCHEMA = "SEMANTIC_MODELS"
STAGE = "SEMANTIC_MODELS"
FILE = "sales_detail_semantic_model.yaml"

def send_message(prompt: str) -> dict:
    """Calls the REST API and returns the response."""
    request_body = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
        "semantic_model": f"@{STAGE}/{FILE}",
    }
    resp = requests.post(
        (
            f"https://{HOST}/api/v2/databases/{DATABASE}/"
            f"schemas/{SCHEMA}/copilots/-/chats/-/messages"
        ),
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{CONN.rest.token}"',
            "Content-Type": "application/json",
        },
    )
    if resp.status_code < 400:
        return resp.json()
    else:
        raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")


def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt)
            content = response["messages"][-1]["content"]
            display_content(content=content)
    st.session_state.messages.append({"role": "assistant", "content": content})


def display_content(content: list, message_index: int = None) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(
                            suggestion, key=f"{message_index}_{suggestion_index}"
                    ):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    df = pd.read_sql(item["statement"], CONN)
                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab = st.tabs(
                            ["Data", "Line Chart", "Bar Chart"]
                        )
                        data_tab.dataframe(df)
                        if len(df.columns) > 1:
                            df = df.set_index(df.columns[0])
                        with line_tab:
                            st.line_chart(df)
                        with bar_tab:
                            st.bar_chart(df)
                    else:
                        st.dataframe(df)


st.title("Cortex Analyst")
st.markdown(f"Semantic Model: `{FILE}`")

if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None

for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        display_content(content=message["content"], message_index=message_index)

if user_input := st.chat_input("What is your question?"):
    process_message(prompt=user_input)

if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None