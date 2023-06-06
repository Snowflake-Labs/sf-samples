-------------------------------------------------
-- NAME:	 COST-IS-13-search-optimization.txt
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
select
      to_date(trunc(start_time, 'DAY')) as usage_day
      ,'SEARCH_OPTIMIZATION' as usage_category					--SEARCH OPTIMIZATION
      ,sum(credits_used) as units_consumed 
	FROM
 	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
	group by 1,2;