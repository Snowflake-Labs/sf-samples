-------------------------------------------------
-- NAME:	 PERF-AU-14-session-statement.txt
-------------------------------------------------
-- DESCRIPTION:
--	Counts by statement type for a session
--
-- OUTPUT:
--	Understanding of session characteristics
--	Ex. Why a large volume of SHOW commands are being executed with a single SELECT
--
-- NEXT STEPS:
--  (0) Once session determined, add filter for session to pull all requests for session specified
--	(1) Drill further to understand statement type usage
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
SELECT
	-- HEADER
    J.QUERY_TYPE,
	J.SESSION_ID,
	TO_DATE(J.START_TIME) AS CREATED_ON,
	COUNT(*)
FROM
    TABLE($QUERY_HISTORY) J
WHERE
    J.START_TIME BETWEEN $TS_START AND $TS_END --AND
--    J.SESSION_ID IN ('157096785686','157095981318')
GROUP BY 1,2,3
ORDER BY
    2,3
LIMIT 1000
;