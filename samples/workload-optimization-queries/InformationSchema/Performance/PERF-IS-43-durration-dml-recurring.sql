-------------------------------------------------
-- NAME:	 PERF-IS-43-durration-dml-recurring.txt
-------------------------------------------------
-- DESCRIPTION:
--	Takes the Top N Duration requests to pull all identical requests for timeframe specified
--	Be careful in targeting analysis because results can become very large based on request types
--	Separate scripts to help target analysis
--					
-- OUTPUT:					
--	Duration of requests, poor throughput
--	Lineage of poor throughput
--	Frequency of poor throughput
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
WITH XSEED (QUERY_TEXT,REQUEST_COUNT,REQUEST_LABEL) AS
(
SELECT
	QUERY_TEXT AS QUERY_TEXT,
	COUNT(*) AS REQUEST_COUNT,
	RANK() OVER (ORDER BY REQUEST_COUNT DESC,QUERY_TEXT) AS REQUEST_LABEL
FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp()))
WHERE
    QUERY_TYPE IN ('SELECT','INSERT','UPDATE','DELETE') AND
    TOTAL_ELAPSED_TIME >300000
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
	COMPILATION_TIME,
	EXECUTION_TIME,
	QUEUED_PROVISIONING_TIME,
	QUEUED_REPAIR_TIME,
	QUEUED_OVERLOAD_TIME,
	TRANSACTION_BLOCKED_TIME

FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp())) Q
    JOIN XSEED X ON X.QUERY_TEXT = Q.QUERY_TEXT
ORDER BY
    X.REQUEST_LABEL,START_TIME
;