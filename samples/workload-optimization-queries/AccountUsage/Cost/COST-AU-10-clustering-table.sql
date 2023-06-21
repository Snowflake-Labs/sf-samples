-------------------------------------------------
-- NAME:	 COST-AU-10-clustering-table.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily auto clustering credits
--
-- OUTPUT:
--	auto clustering credits by account/table/day
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
-- NAME:	COST-AU-09-clustering.txt
---------------------------------------
-- Pre-Requisite:
--  Adjust and run initialization script COST-AU-00-INIT.txt
---------------------------------------
-- Description:
--	Determines reclustering credit consumption for the account by table
---------------------------------------

select
      database_name
      ,schema_name
      ,table_name
      ,to_date(trunc(start_time, 'DAY')) as usage_day
      ,'AUTOMATIC CLUSTERING' as usage_category					--AUTOMATIC CLUSTERING
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($AUTOMATIC_CLUSTERING_HISTORY)
	group by 1,2,3,4,5
    order by 6 desc;