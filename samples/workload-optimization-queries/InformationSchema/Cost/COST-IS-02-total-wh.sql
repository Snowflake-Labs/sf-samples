-------------------------------------------------
-- NAME:	 COST-IS-02-total-wh.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily compute credits
--	Multiple versions provided
--
-- OUTPUT:
--	XP, GS, and total credits by account/warehouse/day
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
--COMPUTE by day by wh
---------------------------------------

    select 
	  warehouse_name
	  ,to_date(trunc(start_time, 'DAY')) as usage_day
	  ,'COMPUTE' as usage_category								--COMPUTE
	  ,sum(credits_used_compute) as units_consumed
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
    GROUP BY 1,2,3
    ORDER BY 1,2,3;
    
    
---------------------------------------
--COMPUTE by day by wh
---------------------------------------

    select 
	  warehouse_name
	  ,to_date(trunc(start_time, 'DAY')) as usage_day
	  ,'COMPUTE' as usage_category								--COMPUTE
	  ,sum(credits_used_compute) as xp_credits
	  ,sum(credits_used_cloud_services) as gs_credits
	  ,sum(credits_used_compute + credits_used_cloud_services) as credits
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
    GROUP BY 1,2,3
    ORDER BY 1,2,3;