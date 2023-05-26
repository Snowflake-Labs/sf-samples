-------------------------------------------------
-- NAME:	 PERF-AU-07-cache.txt
-------------------------------------------------
-- DESCRIPTION:
--	Top N Cache requests, pulls Top N offenders for timeframe specified
--
-- OUTPUT:
--	Lack of Result and Data Cache reuse, which slows request
--
-- NEXT STEPS:
--	(1) Review SQL optimizations
--	(2) Review Cluster Key options
--	(3) Workload segregation
--	(4) Analyze warehouse sizing
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
	BYTES_SCANNED,
	PERCENTAGE_SCANNED_FROM_CACHE,
	BYTES_WRITTEN,
	BYTES_WRITTEN_TO_RESULT,
	BYTES_READ_FROM_RESULT

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    PERCENTAGE_SCANNED_FROM_CACHE ASC
LIMIT 100
;