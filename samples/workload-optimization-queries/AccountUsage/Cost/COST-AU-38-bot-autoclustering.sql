-------------------------------------------------
-- NAME:	 COST-AU-38-bot-autoclustering.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily gs credits
--
-- OUTPUT:
--	auto clustering credits by account/yr/month, database/stage/table
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
    database_name,
    schema_name,
    table_name,
	EXTRACT(YEAR FROM START_TIME) AS XYEAR,
    EXTRACT(MONTH FROM START_TIME) AS XMONTH,
    sum(credits_used) as credits
FROM
	TABLE($AUTOMATIC_CLUSTERING_HISTORY)
group by 1,2,3,4,5
ORDER BY 1,2;