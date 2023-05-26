-------------------------------------------------
-- NAME:	 PERF-AU-78-repl-query-hist.txt
-------------------------------------------------
-- DESCRIPTION:
--	storage metrics for tables
--
-- OUTPUT:
-- 	Query History columns
--
-- NEXT STEPS:
--	use information to determine start, end, and status of refresh
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------

-- Retrieves row(s) related to ALTER REFRESH statements.  Has basic metrics like start 
-- time, end tiime, and status.  Use query_id to access metadata in other views and table 
-- functions

SELECT
	*
FROM
	SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
	AND QUERY_TYPE='REFRESH_GLOBAL_DATABASE'
LIMIT 100;

