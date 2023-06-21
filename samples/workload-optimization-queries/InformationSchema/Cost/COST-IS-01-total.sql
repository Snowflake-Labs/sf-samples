-------------------------------------------------
-- NAME:	 COST-IS-01-total.txt
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	

---------------------------------------
--COMPUTE by day
---------------------------------------

    select 
	  to_date(trunc(start_time, 'DAY')) as usage_day
	  ,'COMPUTE' as usage_category								--COMPUTE
	  ,sum(credits_used_compute) as xp_credits
	  ,sum(credits_used_cloud_services) as gs_credits
	  ,sum(credits_used_compute + credits_used_cloud_services) as credits
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
    GROUP BY 1,2
    ORDER BY 1,2;
