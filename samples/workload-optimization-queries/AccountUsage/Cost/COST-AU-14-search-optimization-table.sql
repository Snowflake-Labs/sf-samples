-------------------------------------------------
-- NAME:	 COST-AU-14-search-optimization-table.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily search optimization credits
--
-- OUTPUT:
--	search optimization credits by account/table/day
--
-- NEXT STEPS:
--	Use for reporting purposes
--	Look for outliers of runaway cost
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
      ,to_date(trunc(start_time, 'DAY')) as usage_day
      ,sum(credits_used) as units_consumed 
	FROM
      TABLE($SEARCH_OPTIMIZATION_HISTORY)
	group by 1,2,3,4
	order by 1,2,3,4;