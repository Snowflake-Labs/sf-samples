#!/bin/bash

set -e  # Exit on any error

# Get the directory where this script is located
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"

# Parse command line arguments
CONNECTION_NAME=""
CONNECTION_ARG=""

while getopts "c:r:" opt; do
    case $opt in
        c)
            CONNECTION_NAME="$OPTARG"
            CONNECTION_ARG="-c $CONNECTION_NAME"
            ;;
        r)
            SNOWFLAKE_ROLE="$OPTARG"
            ;;
        \?)
            echo "Usage: $0 [-c connection_name] [-r snowflake_role]"
            exit 1
            ;;
    esac
done

# Set up Python environment
## Check if we're already in a virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
    python -m venv .venv
    source .venv/bin/activate
else
    echo "Already in virtual environment: $VIRTUAL_ENV"
fi

## Install dependencies for DAG
pip install -r "$SCRIPT_DIR/../requirements.txt" --quiet

## Install and configure SnowCLI
pip install snowflake-cli --quiet
if ! snow connection test --database="" --warehouse="" $CONNECTION_ARG ; then
    echo "Failed to connect to Snowflake. Please check your configuration."
    exit 1
fi

# Load environment variables
export SNOWFLAKE_ROLE=${SNOWFLAKE_ROLE:-ENGINEER}
export SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE:-SNOWBANK}
export SNOWFLAKE_SCHEMA=${SNOWFLAKE_SCHEMA:-DAG_DEMO}
export SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE:-DEMO_WH}
export SNOWFLAKE_COMPUTE_POOL=${SNOWFLAKE_COMPUTE_POOL:-DEMO_POOL}

## Non-standard variables
SNOWFLAKE_DATA_SCHEMA=${SNOWFLAKE_DATA_SCHEMA:-DATA}

# Set up Snowflake environment
## Create resources for DAG
snow sql \
    $CONNECTION_ARG \
    --role $SNOWFLAKE_ROLE \
    -f "$SCRIPT_DIR/create_resources.sql" \
    -D "database_name=$SNOWFLAKE_DATABASE" \
    -D "schema_name=$SNOWFLAKE_SCHEMA" \
    -D "data_schema_name=$SNOWFLAKE_DATA_SCHEMA"

## Set up feature store
SNOWFLAKE_SCHEMA=$SNOWFLAKE_DATA_SCHEMA \
python "$SCRIPT_DIR/setup_feature_store.py" $CONNECTION_ARG