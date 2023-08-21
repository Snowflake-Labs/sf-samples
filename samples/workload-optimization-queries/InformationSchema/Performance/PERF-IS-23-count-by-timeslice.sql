-------------------------------------------------
-- NAME:	 PERF-IS-23-count-by-timeslice.txt
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
--------------------------------
--Using CURRENT_TIMESTAMP in table function
--------------------------------
SELECT time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC') as "SLICE_START_TIME",
        COUNT(QUERY_ID)
FROM 
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		END_TIME_RANGE_END=>CURRENT_TIMESTAMP()))
 WHERE WAREHOUSE_NAME = $WAREHOUSE_NAME
   AND DATABASE_NAME = $DATABASE_NAME
   AND SCHEMA_NAME = $SCHEMA_NAME
   AND USER_NAME= $USER_NAME
   AND LOWER(QUERY_TEXT) ILIKE 'with%'
 GROUP BY time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC')
 ORDER BY time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC');
 
--------------------------------
--Using timestamp variables in table function
--------------------------------
SELECT time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC') as "SLICE_START_TIME",
        COUNT(QUERY_ID)
  FROM TABLE(information_schema.query_history(
		END_TIME_RANGE_START =>$TS_START,
		END_TIME_RANGE_END =>$TS_END))

 WHERE WAREHOUSE_NAME = $WAREHOUSE_NAME
   AND DATABASE_NAME = $DATABASE_NAME
   AND SCHEMA_NAME = $SCHEMA_NAME
   AND USER_NAME= $USER_NAME
   AND LOWER(QUERY_TEXT) ILIKE 'with%'
 GROUP BY time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC')
 ORDER BY time_slice(TO_TIMESTAMP_NTZ(START_TIME), 60, 'SEC');