-------------------------------------------------
-- NAME:	 COST-AU-31-bot-compute.txt
-------------------------------------------------
-- DESCRIPTION:
--	Beginning of Time, monthly reporting of cost based upon sources data retention
--
-- OUTPUT:
--	account, warehouse, year, month, xp, gs
--
-- NEXT STEPS:
--	Use for historical reporting
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
    select 
	  warehouse_name
	,EXTRACT(YEAR FROM START_TIME) AS XYEAR
    ,EXTRACT(MONTH FROM START_TIME) AS XMONTH
	  ,sum(credits_used_compute) as xp_credits
	  ,sum(credits_used_cloud_services) as gs_credits
	FROM
	  TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1,2,3
    ORDER BY 1,2,3;