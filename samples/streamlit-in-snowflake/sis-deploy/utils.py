from __future__ import annotations

import hashlib
import re
import time
from contextlib import contextmanager
from datetime import date
from functools import reduce
from typing import Mapping, cast

import altair as alt
import pandas as pd
import snowflake.snowpark as sp
import streamlit as st
from plotly.graph_objs._figure import Figure
from snowflake.snowpark import Column, DataFrame, Session, Table
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSessionException, SnowparkSQLException
from streamlit.runtime.scriptrunner import get_script_run_ctx


def deep_get(dictionary, keys, default={}):
    """
    deep_get can can iterate through nested dictionaries, used to get credentials in the
    case of a multi-level secrets.toml hierarchy
    """
    return reduce(
        lambda d, key: d.get(key, default) if isinstance(d, Mapping) else default,
        keys.split("."),
        dictionary,
    )


class SnowparkConnection:
    # Initialize the connection; optionally provide the connection_name
    # (used to look up credentials in secrets.toml when run locally)
    #
    # For now this just provides a convenience connect() method to get an underlying
    # Snowpark session; it could also be extended to handle caching and other
    # functionality!
    def __init__(self, connection_name="connections.snowflake"):
        self.connection_name = connection_name

    # connect() returns a snowpark session object; in SiS it gets the active session
    # on local, it checks session state for an existing object and tries to initialize
    # one if not yet created. It checks secrets.toml for credentials and provides a
    # more descriptive error if credentials are missing or path is misconfigured
    # (otherwise you get a "User is empty" error)
    def connect(self):
        try:
            sess = get_active_session()
            return sess
        except SnowparkSessionException:
            ctx = get_script_run_ctx()

            session_state = {} if ctx is None else st.session_state

            if "snowpark_session" not in session_state:
                creds = deep_get(st.secrets, self.connection_name)

                if not creds:
                    st.exception(
                        ValueError(
                            "Unable to initialize connection to Snowflake, did not "
                            "find expected credentials in secret "
                            f"{self.connection_name}. "
                            "Try updating your secrets.toml"
                        )
                    )
                    st.stop()
                session = Session.builder.configs(creds).create()
                session_state["snowpark_session"] = session
            return session_state["snowpark_session"]


def get_data_frame_from_raw_sql(
    sql,
    connection: SnowparkConnection | None = None,
    lowercase_columns: bool = False,
    show_sql: bool = False,
    show_time: bool = False,
    data_name: str = "data",
) -> pd.DataFrame:
    start_time = time.time()

    if show_sql:
        with st.expander("Show the SQL query that generated this data"):
            st.code(
                format_sql(sql),
                language="sql",
            )

    @st.cache_data(ttl=60 * 60 * 6, show_spinner=False)
    def get_data(sql: str, lowercase_columns: bool, _connection: SnowparkConnection):
        session = _connection.connect()
        dataframe = session.sql(sql).to_pandas()
        if lowercase_columns:
            dataframe.columns = [column.lower() for column in dataframe.columns]
        return dataframe

    with st.spinner(f"⌛ Loading {data_name}..."):
        if connection is None:
            connection = SnowparkConnection()
        dataframe = get_data(
            sql, lowercase_columns=lowercase_columns, _connection=connection
        )

    if show_time:
        st.info(f"⏱️ Data loaded in {time.time() - start_time:.2f} seconds")
    return dataframe


@st.cache_data(ttl=60 * 60 * 6)
def _get_df(
    _df: DataFrame,
    queries_str: str,
) -> pd.DataFrame:
    """Converts a Snowpark DataFrame to Pandas DataFrame

    Args:
        _df (DataFrame): Snowpark DataFrame
        queries_str (str): SQL query to get this dataframe.
                           The argument is not used in the function, but it is used
                           to effectively invalidate the cache when the query changes.

    Returns:
        pd.DataFrame: Pandas DataFrame
    """
    _ = queries_str  # This line is here to avoid a warning about an unused argument

    return _df.to_pandas()


def get_pandas_df(
    df: DataFrame,
    lowercase_columns: bool = False,
    show_sql: bool = False,
    show_time: bool = False,
) -> pd.DataFrame:
    """Converts a Snowpark DataFrame to Pandas DataFrame

    Args:
        df (DataFrame): Snowpark DataFrame

    Returns:
        pd.DataFrame: Pandas DataFrame
    """
    queries = str(df._plan.queries[0].sql)
    # Filter out temp tables so that they don't mess up the cache
    filtered = re.sub("SNOWPARK_TEMP_TABLE_[A-Z0-9]+", "TEMP_TABLE", queries)
    filtered = re.sub("query_id_place_holder_[a-zA-Z0-9]+", "query_id", filtered)
    filtered = re.sub('"[l|r]_[a-zA-Z0-9]{4}_', "join_id_", filtered)

    start = time.time()

    try:
        pd_df = _get_df(_df=df, queries_str=filtered)
    except SnowparkSQLException:
        st.expander("Show the SQL query that caused the error").code(
            format_sql_from_df(df),
            language="sql",
        )
        raise

    if lowercase_columns:
        pd_df.columns = [column.lower() for column in pd_df.columns]

    if show_sql:
        with st.expander("Show the SQL query that generated this data"):
            st.code(
                format_sql_from_df(df),
                language="sql",
            )

    if show_time:
        st.info(f"⏱️ Data loaded in {time.time() - start:.2f} seconds")

    return pd_df


@st.cache_resource
def get_table(table_name: str, _session: Session | None = None) -> Table:
    if _session is None:
        _session = cast(Session, SnowparkConnection().connect())
    return _session.table(table_name)


def join_cached(
    df1: DataFrame,
    df2: DataFrame,
    on: str | Column | None = None,
    how: str | None = None,
    *,
    lsuffix: str = "",
    rsuffix: str = "",
    **kwargs,
) -> DataFrame:
    """
    This function serves as a cached alternative to Snowpark's `join` method.

    For example, instead of df1.join(df1, ...) you can do `join_cached(df1, df2, ...)`

    This works by constructing a unique key based on the SQL queries that generated by
    the two dataframes + the `on` condition.

    The dataframes are converted into the underling sql query, and the `on` condition is
    either a Column (like `col('a')` or `(col('a') == col('b'))`), or a string (like
    `'a'`), so combining all those strings should uniquely identify the join.
    """

    @st.cache_resource
    def _join_cached(_df1, _df2, _on, how, lsuffix, rsuffix, meta_query, **kwargs):
        """
        Ignore df1, df2 and on, because those are not cachable, and instead just depend
        on how, lsuffix, rsuffix, and meta_query
        """
        return _df1.join(
            _df2, on=_on, how=how, lsuffix=lsuffix, rsuffix=rsuffix, **kwargs
        )

    meta_query = df1._plan.queries[0].sql + df2._plan.queries[0].sql

    if isinstance(on, Column):
        meta_query += str(on._expression)
    elif isinstance(on, str):
        meta_query += on

    return _join_cached(df1, df2, on, how, lsuffix, rsuffix, meta_query, **kwargs)


def format_sql(sql: str) -> str:
    try:
        import sqlparse
    except ImportError:
        return sql

    return sqlparse.format(sql, reindent=True, keyword_case="lower")


def format_sql_from_df(df: DataFrame, use_header: bool = True) -> str:
    header = "-- This query was generated by Snowpark\n" if use_header else ""
    return header + format_sql(str(df._plan.queries[0].sql))


@st.cache_data
def get_download_link(_session: Session, df: pd.DataFrame, filename: str) -> str:
    """
    Get download link for a dataframe.

    Before this works, you need to create a stage using this command:
    ```
    CREATE OR REPLACE STAGE {temp_stage}
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT = (TYPE = CSV, COMPRESSION = NONE)
    ```
    """
    temp_stage = "TEMP_STAGE_DOWNLOAD"

    db = _session.get_current_database()
    schema = _session.get_current_schema()
    full_temp_stage = f"@{db}.{schema}.{temp_stage}"
    snowpark_df = _session.create_dataframe(df)

    snowpark_df.write.copy_into_location(
        f"{full_temp_stage}/{filename}",
        header=True,
        overwrite=True,
        single=True,
    )

    res = _session.sql(
        f"select get_presigned_url({full_temp_stage}, '{filename}', 3600) as url"
    ).collect()
    url = res[0]["URL"]

    return f"[Download data 📥]({url})"


def sis_download_button(
    data: pd.DataFrame, filename: str | None = None, key: str | None = None
) -> None:
    """
    Adds a button to download a dataframe as a CSV file.
    This is a workaround for st.download_button while it is not supported in SiS.
    It uses Snowpark's get_presigned_url function to generate a link!

    Args:
        data (pd.DataFrame): Data to be downloaded
        filename (str, optional): Filename. Defaults to None.
        key (str, optional): Key of the streamlit button. Defaults to None.
    """

    data_hash = hashlib.sha256(pd.util.hash_pandas_object(data).values).hexdigest()

    if filename is None:
        filename = f"{data_hash}_{date.today()}.csv"

    if key is None:
        key = f"{data_hash}_{filename}"

    if st.button("Get link to download CSV", key=key):
        if data.empty:
            st.error("No data")
        else:
            st.info(
                """
            Right click on the link below and select 'open in new tab' to download the
            data.
            """
            )
            session = SnowparkConnection().connect()
            st.write(get_download_link(session, data, filename))


@contextmanager
def tile_ctx(
    df: sp.DataFrame | pd.DataFrame,
    description: str,
    sql: str | None = None,
    chart: alt.Chart | alt.LayerChart | Figure | None = None,
    skip_chart: bool = False,
):
    """Create a tile with a chart, a dataframe preview, the SQL query and a description.
    This is the context manager version that can be used to add more notes to the
    chart tab in the tile.

    Examples:
    >>> with tile(df, "Description", "SELECT * FROM ...", chart=chart):
            st.write("I want to add more notes here under the chart.")

    >>> with tile(df, "Description", "SELECT * FROM ...", chart=None):
            st.write("I want to add more notes here above the chart.")
            st.plotly_chart(chart, use_container_width=True)

    Args:
        df (spark.DataFrame | pd.DataFrame): Data.
        description (str): Description of the what the tile is about.
        sql (str | None, optional): Underlying SQL query. Defaults to None.
        chart (alt.Chart | Figure | None, optional): Optional chart. Defaults to None.

    Raises:
        ValueError: Whenever chart is not an altair or plotly chart.
    """

    if isinstance(df, sp.DataFrame):
        data = get_pandas_df(df)
    else:
        data = df

    if not skip_chart:
        t1, t2, t3, t4, t5 = st.tabs(
            ["Chart", "Data Preview", "SQL", "Description", "Download Data"]
        )
    else:
        t2, t3, t4, t5 = st.tabs(
            ["Data Preview", "SQL", "Description", "Download Data"]
        )
        if chart is not None:
            raise ValueError("Chart cannot be passed when skip_chart is True. ")

    t4.markdown(description)

    if chart is not None:
        if isinstance(chart, alt.Chart) or isinstance(chart, alt.LayerChart):
            t1.altair_chart(chart, use_container_width=True)
        elif isinstance(chart, Figure):
            t1.plotly_chart(
                chart, use_container_width=True, config={"displayModeBar": False}
            )
        else:
            raise ValueError("Chart must be an altair or plotly chart.")

    if data.empty:
        t2.error("No data")
    else:
        t2.dataframe(data, use_container_width=True)

        with t5:
            data_hash = hashlib.sha256(
                pd.util.hash_pandas_object(data).values
            ).hexdigest()

            sis_download_button(
                data=data,
                key=f"{description}_{data_hash}",
            )

    # When dataframe is a Snowpark dataframe, the SQL query is not explicitly passed
    # Instead, the query is found in the dataframe's metadata.
    if sql is None and isinstance(df, sp.DataFrame):
        sql = format_sql_from_df(df)
        t3.code(format_sql(sql))
    elif sql is None:
        t3.caption("No SQL query was provided.")
    else:
        t3.code(format_sql(sql))

    if skip_chart:
        yield
    else:
        with t1:
            if data.empty:
                st.error("No data")
            yield


def tile(
    df: sp.DataFrame | pd.DataFrame,
    description: str,
    chart: alt.Chart | Figure | alt.LayerChart | None = None,
    sql: str | None = None,
    skip_chart: bool = False,
) -> None:
    """Create a tile with a chart, a dataframe preview, the SQL query and a description.

    Args:
        df (spark.DataFrame | pd.DataFrame): Data.
        description (str): Description of the what the tile is about.
        chart (alt.Chart | Figure | None, optional): Chart.
        sql (str | None, optional): Underlying SQL query. Defaults to None.
        skip_chart (bool, optional): Whether to skip the chart tab. Defaults to False.

    Examples:
    >>> chart = alt.Chart(df).mark_bar().encode(...)
        tile(df, "Description", "SELECT * FROM ...", chart=chart)
    """

    with tile_ctx(df, description, sql, chart, skip_chart):
        pass


def altair_time_series(
    data: pd.DataFrame,
    x: str,
    y: str,
    x_title: str,
    y_title: str,
    y_axis_format: str = ".0f",
    line_color="blue",
    color_var="variable",
    **kwargs,
) -> alt.Chart | None:
    """Plot a time series using Altair
    Args:
        data (pd.DataFrame): Original dataframe
        x (str): Column to use for x
        y (str): Column to use for y
    """
    # If the data is empty, return an empty chart
    if data.empty:
        return None

    data = data.copy()
    data[x] = data[x] + pd.Timedelta(days=1)

    # Label data that has an average in the column name
    data["is_avg"] = data[color_var].str.contains("_avg_")

    # Some data may require a differently formatted tooltip,
    # like average values or scientific notation. In order to
    # allow this, we need to create two charts (one for each tooltip)
    # and combine them
    if any(data["is_avg"]):
        data_noavg = data[~data["is_avg"]]
        data_avg = data[data["is_avg"]]
        data_list = [data_noavg, data_avg]
        format_list = [y_axis_format, ".2f"]
    else:
        data_list = [data]
        format_list = [y_axis_format]

    # Iterate through the dataframes and create the charts
    chart_list = []
    for df, format in zip(data_list, format_list):
        # Move the min tick step
        if df[y].max() < 1:
            tick_min_step = 0
        else:
            tick_min_step = 1

        base = alt.Chart(df).mark_line(color=line_color, point=True)
        domain = [0, float(df[y].max()) * 1.2]
        chart = base.encode(
            x=alt.X(f"yearmonthdate({x}):O", axis=alt.Axis(title=x_title)),
            y=alt.Y(
                y,
                axis=alt.Axis(
                    title=y_title,
                    format=format,
                    tickMinStep=tick_min_step,
                ),
                scale=alt.Scale(domain=domain),
            ),
            color=alt.Color(color_var, title="Variable"),
            tooltip=[
                f"yearmonthdate({x}):O",
                alt.Tooltip(y, format=format),
                color_var,
            ],
        )
        chart_list.append(chart)

    # Combine the charts if necessary
    if len(chart_list) > 1:
        chart = alt.layer(*chart_list)
    else:
        chart = chart_list[0]

    chart = chart.configure_legend(orient="bottom")
    return chart
