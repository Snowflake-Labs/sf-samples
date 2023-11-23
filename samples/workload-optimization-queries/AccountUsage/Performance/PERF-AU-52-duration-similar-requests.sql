-------------------------------------------------
-- NAME:	 PERF-AU-52-duration-similar-requests.txt
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
-- 18N22	WNA		created/updated for repository
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
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END --AND
--    QUERY_ID = '01a04f0b-0403-02fb-0000-4329151bee92' --AND
--    USER_NAME = $USER_NAME AND
--    WAREHOUSE_NAME = $WAREHOUSE_NAME
GROUP BY 1,2,3,4
ORDER BY queue_cnt*avg_QUEUE DESC
LIMIT 100
;



