#!/bin/bash


# Retreive the connection secrets
SNOWSQL_ACCOUNT="$( python ./bin/parse_connection_secrets.py account )"
SNOWSQL_USER="$( python ./bin/parse_connection_secrets.py user )"
export SNOWSQL_PWD="$( python ./bin/parse_connection_secrets.py password )"

#start snowsql session
echo "Running SQL script : $1 ..."
snowsql -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -f "$1"