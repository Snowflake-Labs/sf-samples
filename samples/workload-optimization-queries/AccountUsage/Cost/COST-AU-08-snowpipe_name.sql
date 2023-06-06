-------------------------------------------------
-- NAME:	 COST-AU-08-snowpipe_name.txt
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
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
---------------------------------------
-- NAME:	COST-AU-08-snowpipe-name.txt
---------------------------------------
-- Pre-Requisite:
--  Adjust and run initialization script COST-AU-00-INIT.txt
---------------------------------------
-- Description:
--	Determines snowpipe credit consumption for the account by pipe name
---------------------------------------

	select
      to_date(trunc(start_time, 'DAY')) as usage_day
      ,pipe_name
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($PIPE_USAGE_HISTORY)
	group by 1,2
	order by 1,2;