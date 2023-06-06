-------------------------------------------------
-- NAME:	 COST-AU-13-search-optimization.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily search optimization credits
--
-- OUTPUT:
--	search optimization credits by account/day
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
      to_date(trunc(start_time, 'DAY')) as usage_day
      ,sum(credits_used) as units_consumed 
	FROM
      TABLE($SEARCH_OPTIMIZATION_HISTORY)
	group by 1
	order by 1;