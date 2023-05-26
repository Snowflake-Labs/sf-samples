-------------------------------------------------
-- NAME:	 COST-IS-14-search-optimization-table.txt
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
select
--     database_name
--    ,schema_name
      table_name
      ,to_date(trunc(start_time, 'DAY')) as usage_day
      ,'SEARCH_OPTIMIZATION' as usage_category					--SEARCH OPTIMIZATION
      ,sum(credits_used) as units_consumed 
	FROM
 	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
	group by 1,2,3
	order by 4 desc;