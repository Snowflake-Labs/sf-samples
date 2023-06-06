-------------------------------------------------
-- NAME:	 COST-AU-34-bot-search-opt.txt
-------------------------------------------------
-- DESCRIPTION:
--	Beginning of Time, monthly reporting of cost based upon sources data retention
--
-- OUTPUT:
--	account, search optimization, year, month, credits
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
      ,schema_name
      ,table_name
	,EXTRACT(YEAR FROM START_TIME) AS XYEAR
    ,EXTRACT(MONTH FROM START_TIME) AS XMONTH
      ,sum(credits_used) as units_consumed 
	FROM
      TABLE($SEARCH_OPTIMIZATION_HISTORY)
	group by 1,2,3,4,5
	order by 1,2,3,4,5;