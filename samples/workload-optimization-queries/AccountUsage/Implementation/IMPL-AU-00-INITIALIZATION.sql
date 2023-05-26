-------------------------------------------------
-- NAME:	 IMPL-AU-00-INIT-VANTAGE.txt
-------------------------------------------------
-- DESCRIPTION:
--		Initialization script to setup environment variables
--
-- OPTIONS:
--		Set context using USE commands
--		Setup ACCOUNT_NAME for filtering
--		Setup other environment variables for filtering
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
--------------------------------------------
-- INITIALIZATION SCRIPT
--------------------------------------------

------------------------
-- SET CONTEXT
------------------------
use warehouse WNA_WH;
use role WNA_ROLE;
use database SNOWFLAKE;
use schema ACCOUNT_USAGE;

------------------------
-- SET ENVIRONMENT VARIABLES
------------------------

SET TS_START = '2021-08-01 00:00:00'::TIMESTAMP_LTZ;
SET TS_END = '2021-08-31 00:00:00'::TIMESTAMP_LTZ;

SET 
(
    DATABASES,
    SCHEMATA,
    STAGES,
    TABLES,
    COLUMNS,
    VIEWS,
    TABLE_STORAGE_METRICS,
    FUNCTIONS,
    FILE_FORMATS,
    MASKING_POLICIES,
    POLICY_REFERENCES,
    TASK_HISTORY,
    ROLES,
    USERS,
    PIPES,
    COPY_HISTORY,
    GRANTS_TO_ROLES,
    GRANTS_TO_USERS
)
=
(
    'SNOWFLAKE.ACCOUNT_USAGE.DATABASES',
    'SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA',
    'SNOWFLAKE.ACCOUNT_USAGE.STAGES',
    'SNOWFLAKE.ACCOUNT_USAGE.TABLES',
    'SNOWFLAKE.ACCOUNT_USAGE.COLUMNS',
    'SNOWFLAKE.ACCOUNT_USAGE.VIEWS',
    'SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS',
    'SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS',
    'SNOWFLAKE.ACCOUNT_USAGE.FILE_FORMATS',
    'SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES',
    'SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES',
    'SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.ROLES',
    'SNOWFLAKE.ACCOUNT_USAGE.USERS',
    'SNOWFLAKE.ACCOUNT_USAGE.PIPES',
    'SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES',
    'SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS'
);