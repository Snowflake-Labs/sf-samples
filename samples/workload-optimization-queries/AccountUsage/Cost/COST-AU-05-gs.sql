-------------------------------------------------
-- NAME:	 COST-AU-05-gs.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily gs credits
--
-- OUTPUT:
--	GS credits by account/day
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
-- NAME:	COST-AU-04-gs.txt
---------------------------------------
-- Pre-Requisite:
--  Adjust and run initialization script COST-AU-00-INIT.txt
---------------------------------------
-- Description:
--	Determines GS credit consumption for cloud services
---------------------------------------

select 
	  to_date(trunc(start_time, 'DAY')) as usage_day
	  ,sum(credits_used_cloud_services) as gs_credits
	FROM
      TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1
    order by 1;