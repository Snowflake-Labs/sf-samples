-------------------------------------------------
-- NAME:	 PERF-AU-08-queue.txt
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
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
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
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    QUEUED_PROVISIONING_TIME DESC
LIMIT 100
;