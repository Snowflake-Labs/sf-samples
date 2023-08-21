-------------------------------------------------
-- NAME:	 PERF-AU-39-spilling-recurring.txt
-------------------------------------------------
-- DESCRIPTION:
--	Takes the Top N Spilling requests to pull all identical requests for timeframe specified
--	Be careful in targeting analysis because results can become very large based on request types
--
-- OUTPUT:
--	Local or remote spilling, which slows request
--	Lineage of memory usage and spilling
--	Frequency of spilling
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
	
WITH XSEED (QUERY_TEXT,REQUEST_COUNT,REQUEST_LABEL) AS
(
SELECT
	QUERY_TEXT AS QUERY_TEXT,
	COUNT(*) AS REQUEST_COUNT,
	RANK() OVER (ORDER BY REQUEST_COUNT DESC,QUERY_TEXT) AS REQUEST_LABEL
FROM
    TABLE($query_history)
WHERE
    QUERY_TYPE = 'SELECT' AND
    START_TIME BETWEEN $TS_START AND $TS_END AND
    BYTES_SPILLED_TO_REMOTE_STORAGE > 0  -- SPILLING
GROUP BY 1	
HAVING COUNT(*)>1
ORDER BY REQUEST_LABEL
LIMIT 20
)
SELECT
	-- HEADER
	QUERY_ID,
	QUERY_TYPE,
    X.REQUEST_LABEL,
	Q.QUERY_TEXT,
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
    TABLE($query_history) Q
    JOIN XSEED X ON X.QUERY_TEXT = Q.QUERY_TEXT
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    X.REQUEST_LABEL,START_TIME
;
