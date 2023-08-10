-------------------------------------------------
-- NAME:	 COST-IS-15-replication.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily replication credits
--
-- OUTPUT:
--	replication credits by account/day
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
      ,'REPLICATION' as usage_category							--REPLICATION
      ,sum(credits_used) as units_consumed
	FROM
 	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.REPLICATION_USAGE_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
	group by 1,2;
