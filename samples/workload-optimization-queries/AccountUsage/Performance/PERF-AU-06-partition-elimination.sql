-------------------------------------------------
-- NAME:	 PERF-AU-06-partition-elimination.txt
-------------------------------------------------
-- DESCRIPTION:
--	Top N Partition Elimination requests, pulls Top N offenders for timeframe specified
--
-- OUTPUT:
--	Lack of Partition Elimination, causing unnecessary IO
--
-- NEXT STEPS:
--	(1) Review SQL optimizations
--	(2) Review Cluster Key options
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
SELECT
	-- HEADER
	QUERY_ID,
	QUERY_TYPE,
	QUERY_TEXT,
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
	PARTITIONS_SCANNED,
	PARTITIONS_TOTAL,
    CASE WHEN PARTITIONS_TOTAL = 0 THEN 0
        ELSE (PARTITIONS_TOTAL - PARTITIONS_SCANNED) / PARTITIONS_TOTAL * 100 
        END AS PE_PCT

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END AND
    QUERY_TYPE IN ('SELECT','INSERT','UPDATE','DELETE') AND
    PARTITIONS_TOTAL > 1000
ORDER BY
    PE_PCT ASC
LIMIT 1000
;