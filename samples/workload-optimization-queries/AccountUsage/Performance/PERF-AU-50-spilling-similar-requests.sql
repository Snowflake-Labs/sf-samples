-------------------------------------------------
-- NAME:	 PERF-AU-50-spilling-similar-requests.txt
-------------------------------------------------
-- DESCRIPTION:
--	Lists similar statements based on spilling metrics
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
	WAREHOUSE_SIZE,
	substring(QUERY_TEXT,1,regexp_instr(QUERY_TEXT,' ',1,6)) AS QUERY_TEXT,
	
    AVG(TOTAL_ELAPSED_TIME/1000)	AS AVG_WallClockSec,
	COUNT(*)            AS REQ_CNT,
	SUM(CASE WHEN nvl(BYTES_SPILLED_TO_LOCAL_STORAGE,0) > 0
        THEN 1 ELSE 0 END) AS LOCAL_SPILL_CNT,
	AVG(nvl(BYTES_SPILLED_TO_LOCAL_STORAGE,0)) as MAX_BYTES_SPILLED_LOCAL,
	MAX(nvl(BYTES_SPILLED_TO_LOCAL_STORAGE,0)) as MAX_BYTES_SPILLED_LOCAL,
	SUM(CASE WHEN nvl(BYTES_SPILLED_TO_REMOTE_STORAGE,0) > 0
        THEN 1 ELSE 0 END) AS REMOTE_SPILL_CNT,
	AVG(nvl(BYTES_SPILLED_TO_REMOTE_STORAGE,0)) as AVG_BYTES_SPILLED_REMOTE,
	MAX(nvl(BYTES_SPILLED_TO_REMOTE_STORAGE,0)) as MAX_BYTES_SPILLED_REMOTE
	
FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END --AND
--    QUERY_ID = '01a04f0b-0403-02fb-0000-4329151bee92' --AND
--    USER_NAME = $USER_NAME AND
--    WAREHOUSE_NAME = $WAREHOUSE_NAME
GROUP BY 1,2,3
ORDER BY
    1,2
LIMIT 100
;



