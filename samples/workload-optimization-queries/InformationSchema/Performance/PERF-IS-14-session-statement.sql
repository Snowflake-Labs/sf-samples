-------------------------------------------------
-- NAME:	 PERF-IS-14-session-statement.txt
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT
	-- HEADER
    J.QUERY_TYPE,
	J.SESSION_ID,
	TO_DATE(J.START_TIME) AS CREATED_ON,
	COUNT(*)
FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		END_TIME_RANGE_END=>CURRENT_TIMESTAMP())) J
--WHERE
--    J.SESSION_ID IN ('157096785686','157095981318')
GROUP BY 1,2,3
ORDER BY
    2,3
LIMIT 1000
;