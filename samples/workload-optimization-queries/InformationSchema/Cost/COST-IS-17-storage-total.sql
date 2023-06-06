-------------------------------------------------
-- NAME:	 COST-IS-17-storage-total.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily storage terabytes
--
-- OUTPUT:
--	database, stage, failsafe, and total storage terabytes by account/day
--
-- NEXT STEPS:
--	Use for reporting purposes
--	Look for outliers of runaway cost
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT
	DATE_TRUNC('DAY', USAGE_DATE) AS USAGE_DAY,
	SUM(CONSUMED_BYTES)/power(1024, 4) AS TOTAL_CONSUMED_TB
FROM
(
SELECT
	USAGE_DATE,
	AVERAGE_STAGE_BYTES AS CONSUMED_BYTES
FROM
 	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.STAGE_STORAGE_USAGE_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
WHERE
	USAGE_DATE >= CURRENT_DATE-30
UNION
SELECT
	USAGE_DATE,
	AVERAGE_DATABASE_BYTES + AVERAGE_FAILSAFE_BYTES AS CONSUMED_BYTES
FROM
 	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATABASE_STORAGE_USAGE_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
WHERE
	USAGE_DATE >= CURRENT_DATE-30
)
GROUP BY 1;