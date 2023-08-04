# Download link in Streamlit

In open-source streamlit, you can download data with st.download_button. Today this
is not enabled in SiS. One workaround is to

1. Upload the data into a stage
2. Generate a presigned url for that file in the stage
3. Create a markdown link to download the data

You can see an example of this in download_csv.py

NOTE: In order for this to work properly, a suitable stage must be greated first, and
must be set up so that it is possible to generate a presigned url for file in the stage.

For example:

```sql
CREATE OR REPLACE STAGE TEMP_STAGE_DOWNLOAD
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
FILE_FORMAT = (TYPE = CSV, COMPRESSION = NONE)
```

Note also that, as of today, you need to right-click on the link and open it in a new tab for the download to work properly.
