# Cortex for Cost Governance

## 1. Streamlit in Snowflake - Cortex Query Explorer

Use Cortex + Streamlit to better understand query activity in your environments!

Installation steps:

1. Create a new Streamlit in Snowflake app
2. In the packages dropdown, search for and add the latest version of `snowflake-ml-python`
3. Copy/paste the contents of the SiS app `cortex_query_explorer_sis.py` into the code window

Make sure to tweak the prompt in the code to provide targeted context to Cortex!

## 2. Cortex Tagging

Use Cortex in a Snowflake Task to add a suggested query tag into a dedicated table!

1. Create a table that stores `query_id` and a tag field
2. Define your tagging strategy prompt for the Cortex model you choose
3. Limit the conditions of the queries you look to tag - DO NOT USE THIS TO TAG EVERY QUERY IN YOUR SNOWFLAKE ACCOUNT

4. Schedule the task to run on a cadence that makes sense for your use case