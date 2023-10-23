from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSessionException


def running_in_sis() -> bool:
    import snowflake.connector.connection
    import inspect
    # snowflake.connector.connection.SnowflakeConnection does not exist inside a Stored Proc or Streamlit.
    # It is only part of the external package. So this returns true only in SiS.
    return 0 == len([x for x in inspect.getmembers(snowflake.connector.connection) if x[0] == 'SnowflakeConnection'])


if running_in_sis():
    import streamlit as st
    session = get_active_session()
else:
    import streamlit_in_snowflake as st
    from streamlit_in_snowflake.local_session import get_local_session
    session = get_local_session()


st.title("Hello, world!")

# shows a warning
st.write("What if I go wild and use `unsafe_allow_html`?!", unsafe_allow_html=True)

# also shows a warning
st.image("https://placekitten.com/200/300")

# Use an interactive slider to get user input
hifives_val = st.slider(
	"Number of high-fives in Q3",
	min_value=0,
	max_value=90,
	value=60,
	help="Use this to enter the number of high-fives you gave in Q3",
)

# Write a query that uses the slider
# (Note: this is just dummy data, but you can also query your Snowflake data here)
query = f"""
		select 50 as high_fives, 25 as fist_bumps, 'Q1' as quarter
		union
		select 20 as high_fives, 35 as fist_bumps, 'Q2' as quarter
		union
		select {hifives_val} as high_fives, 30 as fist_bumps, 'Q3' as quarter
"""

# Execute the query and convert it into a Pandas dataframe
sql_data = session.sql(query=query).to_pandas()

# Create a simple bar chart
# See docs.streamlit.io for more types of charts
st.subheader('Number of high-fives')
st.bar_chart(data=sql_data, x="QUARTER", y="HIGH_FIVES")

st.subheader('Underlying data')
st.dataframe(sql_data, use_container_width=True)
