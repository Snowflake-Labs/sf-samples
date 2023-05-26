-------------------------------------------------
-- NAME:	 COST-IS-20-xp-wh.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily xp credits
--
-- OUTPUT:
--	xp credits by account/warehouse/day
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
	  warehouse_name
	  ,to_date(trunc(start_time, 'DAY')) as usage_day
	  ,'COMPUTE' as usage_category								--COMPUTE
	  ,sum(credits_used_compute) as xp-credits
	  ,sum(credits_used_cloud_services) as gs-credits
	  ,sum(credits_used_compute + credits_used_cloud_services) as credits
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
    GROUP BY 1,2,3
    ORDER BY 1,2,3;