import os

HOST = "snowpilot-test.preprod6.us-west-2.aws.snowflakecomputing.com"

# Api-related:
API_ENDPOINT = "/api/v2/cortex/analyst/message"
API_TIMEOUT = 30000  # in miliseconds

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
