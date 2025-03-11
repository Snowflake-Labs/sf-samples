from dataclasses import replace
from datetime import datetime
from typing import Callable, Optional

import streamlit as st
from plotly.io import from_json, to_json

from utils.db import (
    cached_get_query_exec_result,
    get_current_timestamp,
    get_sf_connection,
)
from utils.notifications import add_to_notification_queue
from utils.plots import plotly_fig_from_config
from utils.storage.saved_answers import SavedQuery, update_saved_query_by_id


def _refresh_chart(obj: SavedQuery):
    df, err_msg = cached_get_query_exec_result(obj.sql)
    if err_msg:
        add_to_notification_queue(
            f"Could not execute SQL query. Error: {err_msg}", "error"
        )
        return

    # regenerate plot
    if obj.plot_config.get("type") is not None:
        fig = plotly_fig_from_config(df, obj.plot_config)
        serialized_plot = to_json(fig)
    else:
        serialized_plot = obj.serialized_plot

    # save rerendered plot in db
    session = get_sf_connection()
    updated_query = replace(obj)
    updated_query.serialized_plot = serialized_plot
    updated_query.plot_rendered_on = get_current_timestamp(session=session)
    success, err_msg = update_saved_query_by_id(session, updated_query)
    if success:
        add_to_notification_queue("Chart refreshed", "info")
    else:
        add_to_notification_queue(
            f"Failed to store rerendered plot in db. Error msg: {err_msg}", "error"
        )


def _format_timestamp(t: datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M")


@st.experimental_fragment
def query_gallery_entry(
    obj: SavedQuery,
    show_share_status: bool = True,
    unique_id: str = "",
    on_refresh: Optional[Callable] = None,
):
    """
    Display a gallery entry component for a saved query, including its details and plot.

    This function creates a Streamlit container that displays the details of a saved query,
    such as the prompt, added date, user, semantic model, share status, SQL query, and plot.
    If a plot is available, it is rendered and the timestamp of the last render is shown.
    It also provides a button for refreshing the plot, i.e., running the query, regenerating the plot,
    and updating it in the database.

    Args:
        obj (SavedQuery): The saved query object containing the query details and plot.
        show_share_status (bool, optional): Whether to display the share status of the query. Default is True.
        unique_id (str, optional): Unique ID for the refresh chart button session state key in case this component is rendered multiple times. Default is an empty string.
        on_refresh (Callable, optional): An optional callback to call after refreshing the chart. Default is None.

    Returns:
        st.container: The Streamlit container handle for the gallery entry.
    """
    container_handle = st.container(border=True)
    with container_handle:
        st.markdown(f"#### {obj.prompt}")
        st.markdown(f"__Added on__: `{_format_timestamp(obj.added_on)}`")
        st.markdown(f"__Added by__: `{obj.user}`")
        st.markdown(f"__Semantic model__: `{obj.semantic_model_path}`")
        if show_share_status:
            if obj.is_shared:
                share_msg = "_is shared_ 🟩"
            else:
                share_msg = "_available only to you_ 🟥"
            st.markdown(f"__Share status__: {share_msg}")
        with st.expander("SQL query"):
            st.code(obj.sql, language="sql")
        placeholder = st.empty()
        if obj.serialized_plot:
            chart_fig = from_json(obj.serialized_plot)
            placeholder.plotly_chart(chart_fig)
            st.markdown(
                f"Query last run on: `{_format_timestamp(obj.plot_rendered_on)}`"
            )
        else:
            placeholder.info("There is no chart available for this query")

        # Handle refresh chart button
        if st.button("Refresh chart 🔄", key=unique_id, use_container_width=True):
            placeholder.empty()
            placeholder.info("Refreshing chart...")
            _refresh_chart(obj)
            if on_refresh:
                on_refresh()
            st.rerun()

    return container_handle
