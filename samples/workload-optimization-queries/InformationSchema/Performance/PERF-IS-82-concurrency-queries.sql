-------------------------------------------------
-- NAME:	 PERF-IS-82-concurrency-queries.txt
-------------------------------------------------
-- DESCRIPTION:
--	Set a PIT TS variable to pull requests and metrics leading up to queue event and queued requests
--					
-- OUTPUT:					
--	start/end time, query text, and performance metrics
--	
-- OPTIONS:
--	Use this information to better understand why queuing occurred
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 05MAY22	WNA		created/updated for repository
-------------------------------------------------

SET WAREHOUSE_NAME = 'DELIVERY';

SET TS_PIT = 60263;

SELECT
	START_TIME,
	END_TIME,
	CAST((EXTRACT(HOUR FROM START_TIME)) *3600 
			+ (EXTRACT(MINUTE FROM START_TIME)) *60 
			+ (EXTRACT(SECOND FROM START_TIME) ) AS DEC(8,2)) as TS_START,
	$TS_PIT AS TS_PIT,
	CAST(EXTRACT(HOUR FROM END_TIME)*3600
			+ EXTRACT(MINUTE FROM END_TIME)*60 
			+ EXTRACT(SECOND FROM END_TIME) AS DEC(8,2)) as TS_END,
	QUERY_ID,
	QUERY_TEXT,
	USER_NAME,
	WAREHOUSE_NAME,
	TOTAL_ELAPSED_TIME,
	BYTES_SCANNED,
	ROWS_PRODUCED,
	COMPILATION_TIME,
	EXECUTION_TIME,
	QUEUED_PROVISIONING_TIME,
	QUEUED_REPAIR_TIME,
	QUEUED_OVERLOAD_TIME,
	TRANSACTION_BLOCKED_TIME

FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp()))
WHERE	TRUE
AND		START_TIME BETWEEN $TS_START AND $TS_END
and 	WAREHOUSE_NAME = $WAREHOUSE_NAME
AND		CAST((EXTRACT(HOUR FROM START_TIME)) *3600 
			+ (EXTRACT(MINUTE FROM START_TIME)) *60 
			+ (EXTRACT(SECOND FROM START_TIME) ) AS DEC(8,2)) <= $TS_PIT
AND		CAST(EXTRACT(HOUR FROM END_TIME)*3600
			+ EXTRACT(MINUTE FROM END_TIME)*60 
			+ EXTRACT(SECOND FROM END_TIME) AS DEC(8,2)) >= $TS_PIT
ORDER BY
    1,2
LIMIT 100
;
