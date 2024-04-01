import streamlit as st

# Import the snowflake telemetry library
# Need to install snowflake-telemetry-python
from snowflake import telemetry

event_table = "my_db.my_schema.event_table"
db = "my_db"

st.title(f"Streamlit in Snowflake telemetry events :balloon:")

hifives_val = st.slider("High fives", 0, 20, 10)

if st.button("Record event"):
    # Call telemetry.add_event() with an event name and any object attributes
    telemetry.add_event("Recording Streamlit event", {"hifives_val": hifives_val})
    st.success("Recorded the event!")

with st.expander("View previously logged events"):
    with st.echo(code_location="below"):
        "Note that new events will take several minutes to propagate, starting when the viewer closes the app."
        # Example query to analyze events
        query = f"""
                select TIMESTAMP,
                RESOURCE_ATTRIBUTES['snow.database.name'] as DATABASE,
                RESOURCE_ATTRIBUTES['snow.schema.name'] as SCHEMA,
                RESOURCE_ATTRIBUTES['db.user'] as USER,
                RECORD['name'] as RECORD,
                TO_JSON(RECORD_ATTRIBUTES) as ATTRIBUTES
                from {event_table}
                where RECORD_TYPE = 'SPAN_EVENT'
                -- Add your own filters as appropriate
                and RESOURCE_ATTRIBUTES['snow.database.name'] = '{db}'
                limit 10;
                """
        df = st.experimental_connection("snowpark").query(query, ttl=120)
        st.dataframe(df)
