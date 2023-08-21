-------------------------------------------------
-- NAME:	 COST-IS-08-snowpipe-name.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily snowpipe credits
--
-- OUTPUT:
--	snowpipe credits by account/pipe/day
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
      ,'SNOWPIPE' as usage_category								--SNOWPIPE
      ,pipe_name
      ,sum(credits_used) as units_consumed
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
	group by 1,2,3;