#  Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

# Import python packages
import pandas as pd
import streamlit as st

from snowflake.snowpark.context import get_active_session


# Get the current credentials
session = get_active_session()

temp_stage = "TEMP_STAGE_DOWNLOAD"

st.title("📥 Download Dataframe as CSV")


def get_download_link(df: pd.DataFrame, filename: str) -> str:
    # Create temporary stage, if it doesn't already exist
    db = session.get_current_database()
    schema = session.get_current_schema()
    full_temp_stage = f"@{db}.{schema}.{temp_stage}"
    snowpark_df = session.create_dataframe(df)

    snowpark_df.write.copy_into_location(
        f"{full_temp_stage}/{filename}",
        header=True,
        overwrite=True,
        single=True,
    )

    res = session.sql(
        f"select get_presigned_url({full_temp_stage}, '{filename}', 3600) as url"
    ).collect()
    url = res[0]["URL"]

    return f"[Download {filename} 📥]({url})"


with st.expander("Show stage creation code"):
    st.code(
        f"""
CREATE OR REPLACE STAGE {temp_stage}
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
FILE_FORMAT = (TYPE = CSV, COMPRESSION = NONE)
    """,
        language="sql",
    )


df = pd.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9],
    }
)

st.write(
    "Try editing the dataframe below, and then click the button to download it as a csv"
)

result = st.experimental_data_editor(df, num_rows="dynamic")

if st.button("Get download link"):
    st.write("Generating url...")
    st.write(get_download_link(result, "test.csv"))
    st.info("Right click and choose 'Open in new tab'")
