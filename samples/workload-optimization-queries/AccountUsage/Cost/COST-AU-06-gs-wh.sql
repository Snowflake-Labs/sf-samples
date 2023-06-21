-------------------------------------------------
-- NAME:	 COST-AU-06-gs-wh.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily gs credits
--
-- OUTPUT:
--	GS credits by account/warehouse/day
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
-- NAME:	COST-AU-05-gs-wh.txt
---------------------------------------
-- Pre-Requisite:
--  Adjust and run initialization script COST-AU-00-INIT.txt
---------------------------------------
-- Description:
--	Determines GS credit consumption for cloud services by warehouse
---------------------------------------

select 
	  warehouse_name
	  ,to_date(trunc(start_time, 'DAY')) as usage_day
	  ,sum(credits_used_cloud_services) as gs-credits
	FROM
      TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1,2
    order by 1,2;