-------------------------------------------------
-- NAME:	 PERF-AU-04-query-type-bucket.txt
-------------------------------------------------
-- DESCRIPTION:
--	Several SQL providing various ways to bucket requests by elapsed time
--
-- OUTPUT:
--	Looking for outliers of request types, typically high counts of GS requests on low 
--	end of spectrum or XP requests on high end of spectrum, or other outliers mid-spectrum
--
-- NEXT STEPS:
--	(1) Look at type and frequency, if GS request like SHOW, determine if a tool is 
--	submitting large volumes unnecessarily
--	(2) If XP requests with high frequency, but low to moderate execution times, then 
--	determine need for this type of processing.  Look for ways to optimize this type of processing
--	(3) If XP requests with high execution times, look at optimizing SQL and Cluster Key
--	(4) After processing type, SQL, and Cluster Key options are exhausted, then evaluate 
--	throughput and concurrency resolutions
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
---------------------------------------
-- TOTAL PERIOD BUCKETS OF ELAPSED TIME BY QUERY TYPE
---------------------------------------
SELECT
	-- HEADER
--	QUERY_ID,
	QUERY_TYPE,
--	QUERY_TEXT,
--	DATABASE_NAME,
--	SCHEMA_NAME,
--	SESSION_ID,
--	USER_NAME,
--	ROLE_NAME,
--	WAREHOUSE_NAME,
--	EXECUTION_STATUS,
--	START_TIME,
--	END_TIME,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 1000 THEN 1 ELSE 0 END) AS LT_1,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 5000 AND TOTAL_ELAPSED_TIME >= 1000 THEN 1 ELSE 0 END) AS LT_5,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 10000 AND TOTAL_ELAPSED_TIME >= 5000 THEN 1 ELSE 0 END) AS LT_10,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 30000 AND TOTAL_ELAPSED_TIME >= 10000 THEN 1 ELSE 0 END) AS LT_30,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 60000 AND TOTAL_ELAPSED_TIME >= 30000 THEN 1 ELSE 0 END) AS LT_60,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 90000 AND TOTAL_ELAPSED_TIME >= 60000 THEN 1 ELSE 0 END) AS LT_90,
SUM(CASE WHEN TOTAL_ELAPSED_TIME >= 90000 THEN 1 ELSE 0 END) AS GTE_90

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
GROUP BY
    1
ORDER BY
    GTE_90 DESC
;

---------------------------------------
-- DAY BUCKETS OF ELAPSED TIME BY QUERY TYPE
---------------------------------------
SELECT
	-- HEADER
--	QUERY_ID,
	QUERY_TYPE,
--	QUERY_TEXT,
--	DATABASE_NAME,
--	SCHEMA_NAME,
--	SESSION_ID,
--	USER_NAME,
--	ROLE_NAME,
--	WAREHOUSE_NAME,
--	EXECUTION_STATUS,
	TO_DATE(START_TIME) AS DAY,
--	END_TIME,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 1000 THEN TOTAL_ELAPSED_TIME ELSE 0 END) AS LT_1,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 5000 AND TOTAL_ELAPSED_TIME >= 1000 THEN 1 ELSE 0 END) AS LT_5,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 10000 AND TOTAL_ELAPSED_TIME >= 5000 THEN 1 ELSE 0 END) AS LT_10,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 30000 AND TOTAL_ELAPSED_TIME >= 10000 THEN 1 ELSE 0 END) AS LT_30,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 60000 AND TOTAL_ELAPSED_TIME >= 30000 THEN 1 ELSE 0 END) AS LT_60,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 90000 AND TOTAL_ELAPSED_TIME >= 60000 THEN 1 ELSE 0 END) AS LT_90,
SUM(CASE WHEN TOTAL_ELAPSED_TIME >= 90000 THEN 1 ELSE 0 END) AS GTE_90

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
GROUP BY
    1,2
ORDER BY
    GTE_90 DESC
;

---------------------------------------
-- DAY/USER BUCKETS OF ELAPSED TIME BY QUERY TYPE
---------------------------------------
SELECT
	-- HEADER
--	QUERY_ID,
	QUERY_TYPE,
--	QUERY_TEXT,
--	DATABASE_NAME,
--	SCHEMA_NAME,
--	SESSION_ID,
	USER_NAME,
--	ROLE_NAME,
--	WAREHOUSE_NAME,
--	EXECUTION_STATUS,
	TO_DATE(START_TIME) AS DAY,
--	END_TIME,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 1000 THEN TOTAL_ELAPSED_TIME ELSE 0 END) AS LT_1,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 5000 AND TOTAL_ELAPSED_TIME >= 1000 THEN 1 ELSE 0 END) AS LT_5,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 10000 AND TOTAL_ELAPSED_TIME >= 5000 THEN 1 ELSE 0 END) AS LT_10,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 30000 AND TOTAL_ELAPSED_TIME >= 10000 THEN 1 ELSE 0 END) AS LT_30,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 60000 AND TOTAL_ELAPSED_TIME >= 30000 THEN 1 ELSE 0 END) AS LT_60,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 90000 AND TOTAL_ELAPSED_TIME >= 60000 THEN 1 ELSE 0 END) AS LT_90,
SUM(CASE WHEN TOTAL_ELAPSED_TIME >= 90000 THEN 1 ELSE 0 END) AS GTE_90

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
GROUP BY
    1,2,3
ORDER BY
    GTE_90 DESC
;

---------------------------------------
-- SHOW COMMAND
---------------------------------------
SELECT
	-- HEADER
--	QUERY_ID,
	QUERY_TYPE,
--	QUERY_TEXT,
--	DATABASE_NAME,
--	SCHEMA_NAME,
--	SESSION_ID,
	USER_NAME,
--	ROLE_NAME,
--	WAREHOUSE_NAME,
--	EXECUTION_STATUS,
	TO_DATE(START_TIME) AS DAY,
--	END_TIME,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 1000 THEN TOTAL_ELAPSED_TIME ELSE 0 END) AS LT_1,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 5000 AND TOTAL_ELAPSED_TIME >= 1000 THEN 1 ELSE 0 END) AS LT_5,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 10000 AND TOTAL_ELAPSED_TIME >= 5000 THEN 1 ELSE 0 END) AS LT_10,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 30000 AND TOTAL_ELAPSED_TIME >= 10000 THEN 1 ELSE 0 END) AS LT_30,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 60000 AND TOTAL_ELAPSED_TIME >= 30000 THEN 1 ELSE 0 END) AS LT_60,
SUM(CASE WHEN TOTAL_ELAPSED_TIME < 90000 AND TOTAL_ELAPSED_TIME >= 60000 THEN 1 ELSE 0 END) AS LT_90,
SUM(CASE WHEN TOTAL_ELAPSED_TIME >= 90000 THEN 1 ELSE 0 END) AS GTE_90

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END AND 
    STATEMENT_TYPE = 'SHOW'
GROUP BY
    1,2,3
ORDER BY
    LT_1 DESC
LIMIT 2500
;
