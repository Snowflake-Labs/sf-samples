-------------------------------------------------
-- NAME:	 COST-AU-15-replication.txt
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
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
	select
      to_date(trunc(start_time, 'DAY')) as usage_day
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($REPLICATION_USAGE_HISTORY)
	group by 1
	order by 1;
