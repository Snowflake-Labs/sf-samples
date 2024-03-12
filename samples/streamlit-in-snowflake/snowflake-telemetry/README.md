# Snowflake telemetry for Streamlit in Snowflake

Streamlit in Snowflake apps support [Snowflake logging and tracing](https://docs.snowflake.com/en/developer-guide/logging-tracing/logging-tracing-overview).
Developers can collect telemetry events from their Streamlit in Snowflake apps with the
`snowflake-telemetry-python` package and a few extra lines of code.

```python
import streamlit as st
from snowflake import telemetry

if st.button("Submit"):
    telemetry.add_event("new_submission", {"hifives_val":42})
```

[View a full app example.](./streamlit_app.py)

**Note:** Event collection will only work for apps running in Streamlit in Snowflake.

## Setup

To enable telemetry in Streamlit in Snowflake apps, you will need to complete the following steps:

1. [Set up an event table](https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up) for your account
1. [Set an appropriate TRACE_LEVEL](https://docs.snowflake.com/en/developer-guide/logging-tracing/tracing-trace-level) at the account,
   database, or schema level.
1. Install the `snowflake-telemetry-python` package in your Streamlit app. This can be done via the
   [package selection UI](https://docs.snowflake.com/en/developer-guide/streamlit/create-streamlit-ui#add-a-supported-python-package-to-a-streamlit-app)
   or [environment.yml file](https://docs.snowflake.com/en/developer-guide/streamlit/create-streamlit-sql#installing-packages-by-using-the-environment-yml-file).

The docs about [emitting Trace Events in Python](https://docs.snowflake.com/en/developer-guide/logging-tracing/tracing-python) may also be useful.

## Querying event data

See [Accessing Trace Data](https://docs.snowflake.com/en/developer-guide/logging-tracing/tracing-accessing-events) for documentation on the schema, available metadata and example queries.
The sample app in this repo also has a basic example for querying and rendering event data in a Streamlit app.

## Current limitations

Telemetry for Streamlit in Snowflake has the following limitations currently. We are working to improve these.

- App name is not captured automatically in `RESOURCE_ATTRIBUTES`.
- `snow.executable.type` shows `"PROCEDURE"` instead of `"STREAMLIT"`.
- Display title of the app is not captured automatically.
- Events take a few minutes to propagate to event table starting when the app session is closed.
- You can record only 128 events per app session.
- `TRACE_LEVEL` and `LOG_LEVEL` parameters cannot be set at Streamlit object level - only account, database or schema.
