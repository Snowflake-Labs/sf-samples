import altair as alt
import streamlit as st
from get_data import TABLE_NAME, get_events
from snowflake.snowpark.functions import count_distinct
from utils import (
    SnowparkConnection,
    altair_time_series,
    format_sql_from_df,
    get_pandas_df,
    tile,
)

for key in ["date_range", "customers"]:
    if key in st.session_state:
        st.session_state[key] = st.session_state[key]

st.set_page_config(page_title="App Users", page_icon="👥", layout="wide")
st.write("# 👥 App Users")

events = get_events()

col1, col2 = st.columns(2)

with col1:
    st.write("## Users per day")

    users_per_day = events.group_by("day").agg(
        count_distinct("user_id").alias("users"),
    )

    users_per_week = events.group_by("week").agg(
        count_distinct("user_id").alias("users"),
    )

    joined = users_per_day.join(
        users_per_week,
        users_per_day.day == users_per_week.week,
        how="left",
        lsuffix="_day",
        rsuffix="_week",
    ).drop("week")

    df = get_pandas_df(joined, lowercase_columns=True)
    df = df.melt("day").dropna()

    events_plot = altair_time_series(df, "day", "value", "Date", "Number of users")

    tile(df, "Active users per day", events_plot, sql=format_sql_from_df(users_per_day))

with col2:
    st.write("## New Users")

    start, end = st.session_state["date_range"]

    customer_filter = (
        "where customer in ('" + "', '".join(st.session_state["customers"]) + "')"
        if st.session_state["customers"]
        else ""
    )

    query = f"""
    with first_day_users as (
        select
            user_id,
            date_trunc('day', event_time) as day
        from {TABLE_NAME}
        {customer_filter}
        qualify row_number() over (partition by user_id order by day asc) = 1
    ),
    new_users as (
        select
            day,
            count(*) as new_users
        from first_day_users
        group by day
        order by day asc
    )

    select * from new_users
    where day between '{start}' and '{end}'
    """

    session = SnowparkConnection().connect()

    sp_df = session.sql(query)

    df = get_pandas_df(sp_df, lowercase_columns=True)

    df = df.melt("day")
    customers_plot = altair_time_series(df, "day", "value", "Date", "Number of events")

    tile(
        df,
        "Customers per day",
        customers_plot,
        format_sql_from_df(sp_df),
        # sql=format_sql(query),
    )

with col1:
    st.write("## Power Users Distribution")

    power_users = (
        events.group_by("user_id")
        .agg(
            count_distinct("day").alias("days"),
        )
        .group_by("days")
        .agg(
            count_distinct("user_id").alias("users"),
        )
    )

    df = get_pandas_df(power_users, lowercase_columns=True)
    df["Percentage"] = df["users"] / df["users"].sum()

    power_user_chart = (
        alt.Chart(df)
        .mark_bar(
            color="#004280",
            opacity=0.8,
            cornerRadiusTopLeft=2,
            cornerRadiusTopRight=2,
        )
        .encode(
            x=alt.X(
                "days:O",
                title="Days of Use in Period",
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y("Percentage:Q", title="% of Users", axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("days:O", title="Days of Use in Period"),
                alt.Tooltip("Percentage:Q", title="Percentage of Users", format=".1%"),
                alt.Tooltip("users:Q", title="Number of Users", format=",.0f"),
            ],
        )
        .properties(width=600, height=400)
    )

    tile(
        df=df,
        description="Power User Distribution",
        chart=power_user_chart,
        sql=format_sql_from_df(power_users),
    )
