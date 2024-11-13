from dataclasses import replace

import streamlit as st
from plotly.io import to_json

from components.chart_picker import chart_picker
from components.editable_query import editable_query
from components.query_gallery_entry import query_gallery_entry
from utils.db import (
    cached_get_query_exec_result,
    get_current_timestamp,
    get_sf_connection,
)
from utils.notifications import add_to_notification_queue, handle_notification_queue
from utils.plots import plotly_fig_from_config
from utils.storage.saved_answers import (
    SavedQuery,
    delete_saved_query_by_id,
    get_user_saved_queries,
    update_saved_query_by_id,
)


@st.experimental_dialog("Edit", width="large")
def edit_query_dialog(obj: SavedQuery):
    """Dialog where one can edit all query details."""
    # We don't want to modify base object in session state here, so we need to store data on query edits separately
    if st.session_state.get("last_edited_id") != obj.id:
        st.session_state["edited_sql_tmp_storage"] = None
        st.session_state["last_edited_id"] = obj.id
    st.markdown(f"#### {obj.prompt}")
    is_shared = st.toggle("share this query", value=obj.is_shared)
    sql = (
        obj.sql
        if st.session_state.get("edited_sql_tmp_storage", obj.sql) is None
        else st.session_state["edited_sql_tmp_storage"]
    )

    with st.expander("SQL query", expanded=False):
        save_edit_btn, edited_sql = editable_query(sql, "")
        if save_edit_btn:
            st.session_state["edited_sql_tmp_storage"] = edited_sql
            sql = edited_sql

    with st.status("Running SQL query", expanded=False):
        df, err_msg = cached_get_query_exec_result(sql)
        if df is None:
            st.error(f"Could not execute SQL query. Error: {err_msg}")
            st.stop()
        else:
            st.dataframe(df)

    plot_cfg = chart_picker(df, default_config=obj.plot_config)
    if plot_cfg.get("type") is not None:
        fig = plotly_fig_from_config(df, plot_cfg)
        st.plotly_chart(fig)
        serialized_plot = to_json(fig)
    else:
        serialized_plot = obj.serialized_plot
    save_edits_btn = st.button("Save edits", use_container_width=True, type="primary")
    if save_edits_btn:
        session = get_sf_connection()
        updated_query = replace(obj)
        updated_query.prompt = obj.prompt
        updated_query.sql = sql
        updated_query.is_shared = is_shared
        updated_query.serialized_plot = serialized_plot
        updated_query.plot_config = plot_cfg
        updated_query.plot_rendered_on = get_current_timestamp(session=session)
        success, err_msg = update_saved_query_by_id(session, updated_query)
        if success:
            add_to_notification_queue("Query updated!", "info")
        else:
            add_to_notification_queue(
                f"Failed to update the query. Error msg: {err_msg}", "error"
            )
        _fetch_saved_queries_to_state()
        st.rerun()


@st.experimental_dialog("Delete this query", width="small")
def delete_query_dialog(obj: SavedQuery):
    """Dialog for confirming query deletion intention."""
    st.write("Are you sure you want to delete this query?")
    yes_btn = st.button("Yes, delete it", use_container_width=True, type="primary")
    if yes_btn:
        session = get_sf_connection()
        success, err_msg = delete_saved_query_by_id(session=session, id=obj.id)
        if success:
            add_to_notification_queue("Query deleted!", "info")
        else:
            add_to_notification_queue(
                f"Could not delete query. Error msg: {err_msg}", "error"
            )
        _fetch_saved_queries_to_state()
        st.rerun()


def _fetch_saved_queries_to_state():
    session = get_sf_connection()
    st.session_state["user_saved_queries"] = get_user_saved_queries(
        session=session, user=st.experimental_user.get("user_name", "UNKNOWN USER")
    )


def sidebar():
    """Render sidebar elements."""
    with st.sidebar:
        refresh_btn = st.button("Refresh", use_container_width=True, type="primary")
        if refresh_btn:
            _fetch_saved_queries_to_state()


st.set_page_config(layout="centered")
handle_notification_queue()
sidebar()

st.title("💾 Saved queries")
st.write(
    "On this page, you can browse through all your saved queries, preview and edit them as needed."
)
st.divider()

if "user_saved_queries" not in st.session_state:
    with st.spinner("Fetching saved queries..."):
        _fetch_saved_queries_to_state()


for idx, obj in enumerate(st.session_state["user_saved_queries"]):
    container = query_gallery_entry(
        obj, unique_id=f"page_1_{idx}", on_refresh=_fetch_saved_queries_to_state
    )
    with container:
        st.divider()
        col1, col2 = st.columns(2)
        edit_btn = col1.button(
            "Edit 📝", key=f"edit_query_btn_{idx}", use_container_width=True
        )
        delete_btn = col2.button(
            "Delete 🗑️",
            key=f"delete_query_btn_{idx}",
            use_container_width=True,
            type="primary",
        )

        if edit_btn:
            edit_query_dialog(obj)
        if delete_btn:
            delete_query_dialog(obj)


if not st.session_state["user_saved_queries"]:
    st.info(
        'It seems like there are no saved queries here. Go to "Talk to your data" page, and add some!'
    )
