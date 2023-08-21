-------------------------------------------------
-- NAME:	 COST-AU-19-xp.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily xp credits
--
-- OUTPUT:
--	xp credits by account/day
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
--COMPUTE by day
---------------------------------------

    select 
	  to_date(trunc(start_time, 'DAY')) as usage_day
	  ,sum(credits_used_compute) as xp-credits
	FROM
	  TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1
    ORDER BY 1;
    