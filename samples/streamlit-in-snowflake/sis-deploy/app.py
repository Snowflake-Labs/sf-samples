from __future__ import annotations

import streamlit as st
from get_data import get_events
from snowflake.snowpark.functions import count_distinct
from utils import (
    altair_time_series,
    format_sql_from_df,
    get_pandas_df,
    tile,
)

for key in ["date_range", "customers"]:
    if key in st.session_state:
        st.session_state[key] = st.session_state[key]

st.set_page_config(page_title="App Performance", page_icon="📈", layout="wide")
st.write("# 📈 App Performance ")

events = get_events()

col1, col2 = st.columns(2)

with col1:
    st.write("## Overall Usage")

    events_per_day = events.group_by("day").agg(
        count_distinct("event_id").alias("events"),
    )

    events_per_week = events.group_by("week").agg(
        count_distinct("event_id").alias("events"),
    )

    joined = events_per_day.join(
        events_per_week,
        events_per_day.day == events_per_week.week,
        how="left",
        lsuffix="_day",
        rsuffix="_week",
    ).drop("week")

    df = get_pandas_df(joined, lowercase_columns=True)
    df = df.melt("day").dropna()

    events_plot = altair_time_series(df, "day", "value", "Date", "Number of events")

    tile(df, "Events per day", events_plot, sql=format_sql_from_df(events_per_day))

with col2:
    st.write("## Active Customers")

    customers_per_day = events.group_by("day").agg(
        count_distinct("customer").alias("customers"),
    )

    customers_per_week = events.group_by("week").agg(
        count_distinct("customer").alias("customers"),
    )

    joined = customers_per_day.join(
        customers_per_week,
        customers_per_day.day == customers_per_week.week,
        how="left",
        lsuffix="_day",
        rsuffix="_week",
    ).drop("week")

    df = get_pandas_df(joined, lowercase_columns=True)
    df = df.melt("day").dropna()
    customers_plot = altair_time_series(
        df,
        "day",
        "value",
        "Date",
        "Number of customers",
    )

    tile(
        df,
        "Customers per day",
        customers_plot,
        sql=format_sql_from_df(customers_per_day),
    )
