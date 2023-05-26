-------------------------------------------------
-- NAME:	 PERF-AU-46-summary-metrics.txt
-------------------------------------------------
-- DESCRIPTION:
--	Summary of key metrics by account, warehouse, and date
--					
-- OUTPUT:					
--	Using metrics, narrow down timeframe and types of metric analysis for specific accounts and 
--	warehouses
--		-Three request count buckets, plus total request count
--		Counts of occurrences: local & remote spilling, cache usage, partition elimination,
--		-queuing
--	
-- NEXT STEPS:					
--	Based on metric outliers, run associated metric scripts
--
-- OPTIONS:
--	(1) First, run script to include all accounts and warehouses, typically for one day 
--	if you have many accounts and warehouses
--	(2) Second, add filters to narrow analysis to specific accounts and warehouses, but 
--	with more days of data to see trends
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------

SELECT
	WAREHOUSE_NAME,
	WAREHOUSE_SIZE,
	TO_DATE(START_TIME) AS EXEC_DAY,
	SUM(case when TOTAL_ELAPSED_TIME < 10000 then 1 else 0 end) as SHORT_REQUEST,
	SUM(case when TOTAL_ELAPSED_TIME < 600000 AND TOTAL_ELAPSED_TIME > 10000 then 1 else 0 end) as MEDIUM_REQUEST,
	SUM(case when TOTAL_ELAPSED_TIME > 600000 then 1 else 0 end) as LONG_REQUEST,
    	COUNT(*) AS TOTAL_REQUEST,
    
    	SUM(CASE WHEN BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 1 ELSE 0 END) as REMOTE_SPILL_CNT,
	SUM(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE > 0 THEN 1 ELSE 0 END) as LOCAL_SPILL_CNT,

    	SUM(CASE WHEN PARTITIONS_TOTAL - PARTITIONS_SCANNED > 0 THEN 1 ELSE 0 END) as PE_CNT,

 	SUM(CASE WHEN BYTES_READ_FROM_RESULT > 0 THEN 1 ELSE 0 END) AS RESULT_CACHE_CNT,
	SUM(CASE WHEN BYTES_SCANNED  > 0 THEN 1 ELSE 0 END) AS DATA_CACHE_CNT,

    	SUM(CASE WHEN TO_NUMBER(QUEUED_PROVISIONING_TIME + QUEUED_OVERLOAD_TIME) > 0 THEN 1 ELSE 0 END) AS QUEUE_CNT
	
FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END --AND
--    WAREHOUSE_NAME = ''
GROUP BY
    1,2,3
ORDER BY
    1,2,3
;
