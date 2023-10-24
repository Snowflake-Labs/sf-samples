-------------------------------------------------
-- NAME:	 PERF-IS-52-duration-similar-requests.txt
-------------------------------------------------
-- DESCRIPTION:
--	Lists similar statements based on duration metrics
--					
-- OUTPUT:					
--	Lists statements with execution frequency and aggregated metrics
--	
-- NEXT STEPS:					
--	Review frequency of metrics
--  Refine substring for clarity of statement where necessary
--  Investigate statements for improvement
--  Quantify impact/improvement for customer
--
--  OPTIONS
--  Add filters to narrow search
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------

SELECT
	WAREHOUSE_NAME,
    DATABASE_NAME,
	SCHEMA_NAME,
 	substring(QUERY_TEXT,1,regexp_instr(QUERY_TEXT,' ',1,6)) AS QUERY_TEXT,
 	
 	count(*) as request_count,
	avg(TOTAL_ELAPSED_TIME/1000) as avg_time,
   min(TOTAL_ELAPSED_TIME/1000) as min_time,
   max(TOTAL_ELAPSED_TIME/1000) as max_time,
   
   sum(case when TRANSACTION_BLOCKED_TIME>0 then 1 else 0 end) as lock_cnt,
   avg(TRANSACTION_BLOCKED_TIME/1000) as avg_lock,
   min(TRANSACTION_BLOCKED_TIME/1000) as min_lock,
   max(TRANSACTION_BLOCKED_TIME/1000) as max_lock,
   
   sum(case when TO_NUMBER((QUEUED_PROVISIONING_TIME + QUEUED_OVERLOAD_TIME) / 1000, 10, 3)>0 then 1 else 0 end) as queue_cnt,
   avg(TO_NUMBER((QUEUED_PROVISIONING_TIME + QUEUED_OVERLOAD_TIME) / 1000, 10, 3)) AS avg_QUEUE,
   min(TO_NUMBER((QUEUED_PROVISIONING_TIME + QUEUED_OVERLOAD_TIME) / 1000, 10, 3)) AS min_QUEUE,
   max(TO_NUMBER((QUEUED_PROVISIONING_TIME + QUEUED_OVERLOAD_TIME) / 1000, 10, 3)) AS max_QUEUE


FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp()))
--WHERE
--    QUERY_ID = $QUERY_ID
GROUP BY 1,2,3,4
ORDER BY queue_cnt*avg_QUEUE DESC
LIMIT 100
;