-------------------------------------------------
-- NAME:	 PERF-AU-75-repl-usage-hist.txt
-------------------------------------------------
-- DESCRIPTION:
--	storage metrics for tables
--
-- OUTPUT:
--	start, end, database,
--	CREDITS_USED, BYTES_TRANSFERRED
--
-- NEXT STEPS:
--	use information to determine table sizing
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------

SELECT
    START_TIME,
    END_TIME,
    DATABASE_NAME,
    DATABASE_ID,
    CREDITS_USED,
    BYTES_TRANSFERRED
FROM
    SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
    AND DATABASE_NAME = $DATABASE_NAME
ORDER BY
    DATABASE_NAME,START_TIME;
    
    


