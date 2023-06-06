-------------------------------------------------
-- NAME:	 PERF-AU-09-duration.txt
-------------------------------------------------
-- DESCRIPTION:
--	Top N Duration requests, pulls Top N offenders for timeframe specified
--
-- OUTPUT:
--	Duration of requests, poor throughput, longest runners
--
-- NEXT STEPS:
--	(1) Review SQL optimizations
--	(2) Review Cluster Key options
--	(3) Analyze warehouse sizing
--	(4) Check for Queuing, then address Concurrency
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
	COMPILATION_TIME,
	EXECUTION_TIME,
	QUEUED_PROVISIONING_TIME,
	QUEUED_REPAIR_TIME,
	QUEUED_OVERLOAD_TIME,
	TRANSACTION_BLOCKED_TIME,
	LIST_EXTERNAL_FILES_TIME

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    TOTAL_ELAPSED_TIME DESC
LIMIT 100
;