-------------------------------------------------
-- NAME:	 PERF-AU-23-count-by-timeslice.txt
-------------------------------------------------
-- DESCRIPTION:
--	Count of requests by time slice
--
-- OUTPUT:
--	Time slice and request count
--
-- NEXT STEPS:
--	Use information to narrow performance analysis
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
--------------------------------
--ACCOUNT_USAGE
--------------------------------
SELECT time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC') as "SLICE_START_TIME",
        COUNT(QUERY_ID)
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
 WHERE WAREHOUSE_NAME = $WAREHOUSE_NAME
   AND DATABASE_NAME = $DATABASE_NAME
   AND SCHEMA_NAME = $SCHEMA_NAME
   AND START_TIME >= $TS_START
   AND START_TIME < $TS_END
   AND USER_NAME= $USER_NAME
   AND LOWER(QUERY_TEXT) ILIKE 'with%'
 GROUP BY time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC')
 ORDER BY time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC');
 
