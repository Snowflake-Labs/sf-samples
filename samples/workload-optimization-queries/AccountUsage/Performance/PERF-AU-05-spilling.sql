-------------------------------------------------
-- NAME:	 PERF-AU-05-spilling.txt
-------------------------------------------------
-- DESCRIPTION:
--	Top N Spilling requests, pulls Top N offenders for timeframe specified
--
-- OUTPUT:
--	Local or remote spilling, which slows request
--
-- NEXT STEPS:
--	(1) Review SQL optimizations
--	(2) Review Cluster Key options
--	(3) Analyze warehouse sizing
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
	BYTES_SPILLED_TO_LOCAL_STORAGE,
	BYTES_SPILLED_TO_REMOTE_STORAGE,
	BYTES_SENT_OVER_THE_NETWORK

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    BYTES_SPILLED_TO_REMOTE_STORAGE desc
--    BYTES_SPILLED_TO_LOCAL_STORAGE desc
LIMIT 100
;