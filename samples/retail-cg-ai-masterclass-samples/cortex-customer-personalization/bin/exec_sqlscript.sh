#!/bin/bash
#
# This is mainly used as part of streamlit app invoking SQL script. Typically
# not needed for manual run.
#

# Retreive the connection secrets
SNOWSQL_ACCOUNT="$( python ./bin/parse_connection_secrets.py account )"
SNOWSQL_USER="$( python ./bin/parse_connection_secrets.py user )"
export SNOWSQL_PWD="$( python ./bin/parse_connection_secrets.py password )"

#start snowsql session
echo "Running SQL script : $1 ..."
snowsql --config .app_store/snowsql_config.ini -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -f "$1"