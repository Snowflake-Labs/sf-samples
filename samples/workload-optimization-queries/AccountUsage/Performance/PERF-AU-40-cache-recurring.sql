-------------------------------------------------
-- NAME:	 PERF-AU-40-cache-recurring.txt
-------------------------------------------------
-- DESCRIPTION:
--	Takes the Top N Cache requests to pull all identical requests for timeframe specified
--	Be careful in targeting analysis because results can become very large based on request types
--
-- OUTPUT:
--	Lack of Result and Data Cache reuse, which slows request
--	Lineage of cache usage 
--	Frequency of cache reuse issues
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
    BYTES_READ_FROM_RESULT = 0 -- RESULT CACHE
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
	BYTES_SCANNED,
	PERCENTAGE_SCANNED_FROM_CACHE,
	BYTES_WRITTEN,
	BYTES_WRITTEN_TO_RESULT,
	BYTES_READ_FROM_RESULT

FROM
    TABLE($query_history) Q
    JOIN XSEED X ON X.QUERY_TEXT = Q.QUERY_TEXT
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    X.REQUEST_LABEL,START_TIME
;