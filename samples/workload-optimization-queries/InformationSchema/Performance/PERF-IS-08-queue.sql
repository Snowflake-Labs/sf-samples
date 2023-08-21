-------------------------------------------------
-- NAME:	 PERF-IS-08-queue.txt
-------------------------------------------------
-- DESCRIPTION:
--	Top N requests with longest queue times.  Least resource intensive process to 
--	identify queuing time periods
--
-- OUTPUT:
--	Identifies periods of concurrency issues
--
-- NEXT STEPS:
--	Further analyze concurrency to understand full impact
--	Address throughput (SQL & Cluster Keys)
--	Address throughput (scale up… vWH sizing)
--	Address concurrency (scale out…increase clusters)
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT
	-- HEADER
	QUERY_ID,
	QUERY_TYPE,
	QUERY_TEXT,
	DATABASE_NAME,
	SCHEMA_NAME,
	SESSION_ID,
	USER_NAME,
	ROLE_NAME,
	WAREHOUSE_NAME,
	WAREHOUSE_SIZE,
	WAREHOUSE_TYPE,
	CLUSTER_NUMBER,
	QUERY_TAG,
	EXECUTION_STATUS,
	ERROR_CODE,
	ERROR_MESSAGE,
	START_TIME,
	END_TIME,
	TOTAL_ELAPSED_TIME,
	-- DETAIL
	QUEUED_PROVISIONING_TIME,
	QUEUED_REPAIR_TIME,
	QUEUED_OVERLOAD_TIME

FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp()))
ORDER BY
    QUEUED_PROVISIONING_TIME DESC
LIMIT 100
;