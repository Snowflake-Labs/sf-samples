-------------------------------------------------
-- NAME:	 COST-IS-07-snowpipe.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily snowpipe credits
--
-- OUTPUT:
--	snowpipe credits by account/day
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
      ,sum(credits_used) as units_consumed
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
	group by 1,2;