-------------------------------------------------
-- NAME:	 COST-AU-00-INIT-VANTAGE.txt
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
use role SYSADMIN;
use database SNOWFLAKE;
use schema ACCOUNT_USAGE;
------------------------
-- SET ENVIRONMENT VARIABLES
------------------------
SET TS_START = '2021-08-01 00:00:00'::TIMESTAMP_LTZ;
SET TS_END = '2021-08-31 00:00:00'::TIMESTAMP_LTZ;

SET 
(
    WAREHOUSE_METERING_HISTORY,
    PIPE_USAGE_HISTORY,
    AUTOMATIC_CLUSTERING_HISTORY,
    MATERIALIZED_VIEW_REFRESH_HISTORY,
    SEARCH_OPTIMIZATION_HISTORY,
    REPLICATION_USAGE_HISTORY,
    DATABASE_STORAGE_USAGE_HISTORY,
    STAGE_STORAGE_USAGE_HISTORY
)
=
(
    'SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY',
    'SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY'
);

