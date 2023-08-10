-------------------------------------------------
-- NAME:	 COST-IS-11-materialized-view.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily materialized view credits
--
-- OUTPUT:
--	materialized view credits by day
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
      ,'MVIEW_REFRESH' as usage_category						--MV REFRESH
      ,sum(credits_used) as units_consumed
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
	group by 1,2;