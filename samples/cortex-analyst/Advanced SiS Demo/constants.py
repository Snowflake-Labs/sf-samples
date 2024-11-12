import os

HOST = "snowpilot-test.preprod6.us-west-2.aws.snowflakecomputing.com"

# Api-related:
API_ENDPOINT = "/api/v2/cortex/analyst/message"
API_TIMEOUT = 30_000  # in miliseconds

# Very important to set those right:
# List of available semantic model paths in the format: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>
# Each path points to a YAML file defining a semantic model
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.RAW_DATA/revenue_timeseries.yaml"
]
# Path to app working schema
APP_SCHEMA_PATH = "CORTEX_ANALYST_DEMO.CA_SIS_DEMO_APP_SCHEMA"

# Name of the table where app saves information about saved queries
SAVED_QUERIES_TABLE_NAME = "SAVED_QUERIES"

# Local-dev specific:
DEV_SNOWPARK_CONNECTION_NAME = os.getenv(
    "SNOWPARK_CONNECTION_NAME", "ca-sis-demo-connection"
)

# Enable/disable additional LLM-powered features:

# Followup questions in chat after each analyst reponse containing SQL
ENABLE_SMART_FOLLOWUP_QUESTIONS_SUGGESTIONS = True
SMART_FOLLOWUP_QUESTIONS_SUGGESTIONS_MODEL = "llama3.2-3b"

# Data summary after executing the query in chat
ENABLE_SMART_DATA_SUMMARY = True
SMART_DATA_SUMMARY_MODEL = "llama3-8b"

# Chart suggestions for data after executing the query in chat
ENABLE_SMART_CHART_SUGGESTION = True
SMART_CHART_SUGGESTION_MODEL = "mistral-large"
