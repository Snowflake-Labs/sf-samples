-------------------------------------------------
-- NAME:	 COST-AU-01-total.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily compute credits
--
-- OUTPUT:
--	XP, GS, and total credits by account/day
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
-- NAME:	COST-AU-01-total.txt
---------------------------------------
-- Pre-Requisite:
--  Adjust and run initialization script COST-AU-00-INIT.txt
---------------------------------------
-- Description:
--	Sums xp, gs, and total credits by day for the account
---------------------------------------

    select 
	  to_date(trunc(start_time, 'DAY')) as usage_day
	  ,sum(credits_used_compute) as xp_credits
	  ,sum(credits_used_cloud_services) as gs_credits
	  ,sum(credits_used_compute + credits_used_cloud_services) as credits
	FROM
	  TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1
    ORDER BY 1,2;
