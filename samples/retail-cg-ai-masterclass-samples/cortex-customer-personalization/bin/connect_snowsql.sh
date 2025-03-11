#!/bin/bash
#
# Purpose: 
#   Utility script to run the snowsql, the benefit of using this wrapper script
# is that it, pre-configures the snowsql with connection information provided
# it also creates the local config file, which contains the various variables that could
# be substituted in the sql scripts. 
#

# Retreive the connection secrets
SNOWSQL_ACCOUNT="$( python ./bin/parse_connection_secrets.py account )"
SNOWSQL_USER="$( python ./bin/parse_connection_secrets.py user )"
export SNOWSQL_PWD="$( python ./bin/parse_connection_secrets.py password )"

#start snowsql session
snowsql --config .app_store/snowsql_config.ini -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER