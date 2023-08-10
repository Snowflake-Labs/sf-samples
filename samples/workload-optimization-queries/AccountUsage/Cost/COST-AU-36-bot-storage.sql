-------------------------------------------------
-- NAME:	 COST-AU-36-bot-storage.txt
-------------------------------------------------
-- DESCRIPTION:
--	Beginning of Time, monthly reporting of cost based upon sources data retention
--
-- OUTPUT:
--	account, year, month, database Tb, stage Tb, failsafe Tb, and total
--
-- NEXT STEPS:
--	Use for historical reporting
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
SELECT
    EXTRACT(YEAR FROM USAGE_DATE) AS XYEAR,
    EXTRACT(MONTH FROM USAGE_DATE) AS XMONTH,
	SUM(AVERAGE_DATABASE_BYTES)/power(1024, 4) AS database_Tbytes,
	SUM(AVERAGE_STAGE_BYTES)/power(1024, 4) AS stage_Tbytes,
	SUM(AVERAGE_FAILSAFE_BYTES)/power(1024, 4) AS failsafe_Tbytes,
	SUM(AVERAGE_STAGE_BYTES + AVERAGE_DATABASE_BYTES + AVERAGE_FAILSAFE_BYTES)/power(1024, 4) AS total_Tbytes
FROM
(
SELECT
	USAGE_DATE,
	AVERAGE_STAGE_BYTES,
	0 AS AVERAGE_DATABASE_BYTES,
	0 AS AVERAGE_FAILSAFE_BYTES
FROM
	TABLE($STAGE_STORAGE_USAGE_HISTORY)
UNION
SELECT
	USAGE_DATE,
	0 AS AVERAGE_STAGE_BYTES,
	AVERAGE_DATABASE_BYTES,
	AVERAGE_FAILSAFE_BYTES
  FROM
	TABLE($DATABASE_STORAGE_USAGE_HISTORY)
)
GROUP BY 1,2
ORDER BY 1,2;