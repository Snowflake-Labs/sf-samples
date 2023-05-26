-------------------------------------------------
-- NAME:	 COST-IS-00-INIT-VANTAGE.txt
-------------------------------------------------
-- DESCRIPTION:
--		Initialization script to setup environment variables
--
-- OPTIONS:
--		Set context using USE commands
--		Set DATABASE in order to setup table references
--		Setup ACCOUNT_NAME for filtering
--		Setup other environment variables for filtering
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
--------------------------------------------
-- INITIALIZATION SCRIPT
--------------------------------------------

------------------------
-- SET CONTEXT
------------------------
use warehouse SFsupport_WH;
use role TECHNICAL_ACCOUNT_MANAGER;
use database SNOWFLAKE;
use schema ACCOUNT_USAGE;

------------------------
-- SET ENVIRONMENT VARIABLES
------------------------
SET DEPLOYMENT = '';
SET ACCOUNT_NAME = '';
SET DATABASE = 'SNOWFLAKE';

SET WAREHOUSE_METERING_HISTORY = $DATABASE || '.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY';
SET PIPE_USAGE_HISTORY = $DATABASE || '.INFORMATION_SCHEMA.PIPE_USAGE_HISTORY';
SET AUTOMATIC_CLUSTERING_HISTORY = $DATABASE || '.INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY';
SET MATERIALIZED_VIEW_REFRESH_HISTORY = $DATABASE || '.INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY';
SET SEARCH_OPTIMIZATION_HISTORY = $DATABASE || '.INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY';
SET REPLICATION_USAGE_HISTORY = $DATABASE || '.INFORMATION_SCHEMA.REPLICATION_USAGE_HISTORY';
SET STORAGE_USAGE = $DATABASE || '.INFORMATION_SCHEMA.STORAGE_USAGE';

SET TS_START = '2021-08-01 00:00:00'::TIMESTAMP_LTZ;
SET TS_END = '2021-08-31 00:00:00'::TIMESTAMP_LTZ;