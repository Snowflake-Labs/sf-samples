-------------------------------------------------
-- NAME:	 PERF-AU-51-pe-similar-requests.txt
-------------------------------------------------
-- DESCRIPTION:
--	Lists similar statements based on partition elimination metrics
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
    MIN(CASE WHEN PARTITIONS_TOTAL = 0 THEN 0
        ELSE (PARTITIONS_TOTAL - PARTITIONS_SCANNED) / PARTITIONS_TOTAL * 100 
        END) AS MIN_PE_PCT,
    AVG(CASE WHEN PARTITIONS_TOTAL = 0 THEN 0
        ELSE (PARTITIONS_TOTAL - PARTITIONS_SCANNED) / PARTITIONS_TOTAL * 100 
        END) AS AVG_PE_PCT,
    MAX(CASE WHEN PARTITIONS_TOTAL = 0 THEN 0
        ELSE (PARTITIONS_TOTAL - PARTITIONS_SCANNED) / PARTITIONS_TOTAL * 100 
        END) AS MAX_PE_PCT
FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END --AND
--    QUERY_ID = '01a04f0b-0403-02fb-0000-4329151bee92' --AND
--    USER_NAME = $USER_NAME AND
--    WAREHOUSE_NAME = $WAREHOUSE_NAME
GROUP BY 1,2,3,4
ORDER BY AVG_PE_PCT DESC
LIMIT 100
;



