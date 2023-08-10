-------------------------------------------------
-- NAME:	 COST-AU-33-bot-replication.txt
-------------------------------------------------
-- DESCRIPTION:
--	Beginning of Time, monthly reporting of cost based upon sources data retention
--
-- OUTPUT:
--	account, replication, year, month, credits
--
-- NEXT STEPS:
--	Use for historical reporting
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
	select
      database_name
	,EXTRACT(YEAR FROM START_TIME) AS XYEAR
    ,EXTRACT(MONTH FROM START_TIME) AS XMONTH
      ,sum(credits_used) as CREDITS
	FROM
      TABLE($REPLICATION_USAGE_HISTORY)
	group by 1,2,3
	order by 1,2;