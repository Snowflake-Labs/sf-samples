-------------------------------------------------
-- NAME:	 COST-AU-07-snowpipe.txt
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
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
---------------------------------------
-- NAME:	COST-AU-07-snowpipe.txt
---------------------------------------
-- Pre-Requisite:
--  Adjust and run initialization script COST-AU-00-INIT.txt
---------------------------------------
-- Description:
--	Determines snowpipe credit consumption for the account
---------------------------------------

	select
      to_date(trunc(start_time, 'DAY')) as usage_day
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($PIPE_USAGE_HISTORY)
	group by 1
	order by 1;