-------------------------------------------------
-- NAME:	 COST-AU-99-credit-all.txt
-------------------------------------------------
-- DESCRIPTION:
--	Report of all cost types
--
-- OUTPUT:
--	Total, gs, xp, adjustment, materialized views, replication, autoclustering, pipes,
--	search optimization, and storage cost types
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
	
alter session set timezone = 'UTC';

select usage_month
,usage_category
, round(units_consumed,3) as units_consumed
, round(units_consumed * CASE WHEN usage_category = 'STORAGE' THEN 23 else 2.25 END,2) as total_usage
from
  (
    select 
	  to_date(trunc(start_time, 'MONTH')) as usage_month
	  ,'COMPUTE' as usage_category								--COMPUTE
	  ,sum(credits_used_compute) as units_consumed
	FROM
      TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1,2
	UNION ALL
	select 
	  to_date(trunc(start_time, 'MONTH')) as usage_month
	  ,'CLOUD SERVICES' as usage_category				--CLOUD SERVICES
	  ,sum(credits_used_cloud_services) as units_consumed
	FROM
      TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1,2
	UNION ALL
	select
      to_date(trunc(start_time, 'MONTH')) as usage_month
      ,'ADJ FOR INCL CLOUD SERVICES' as usage_category			--ADJ FOR INCL CLOUD SERVICES
      ,sum(credits_used_compute * -.1) as units_consumed
	FROM
      TABLE($WAREHOUSE_METERING_HISTORY)
    GROUP BY 1,2
	UNION ALL
	select
      to_date(trunc(start_time, 'MONTH')) as usage_month
      ,'SNOWPIPE' as usage_category								--SNOWPIPE
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($PIPE_USAGE_HISTORY)
	group by 1,2
	UNION ALL
	select
      to_date(trunc(start_time, 'MONTH')) as usage_month
      ,'AUTOMATIC CLUSTERING' as usage_category				--AUTOMATIC CLUSTERING
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($AUTOMATIC_CLUSTERING_HISTORY)
	group by 1,2
	UNION ALL
	select
      to_date(trunc(start_time, 'MONTH')) as usage_month
      ,'MVIEW_REFRESH' as usage_category					--MV REFRESH
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($MATERIALIZED_VIEW_REFRESH_HISTORY)
	group by 1,2
	UNION ALL
	select
      to_date(trunc(start_time, 'MONTH')) as usage_month
      ,'SEARCH_OPTIMIZATION' as usage_category				--SEARCH OPTIMIZATION
      ,sum(credits_used) as units_consumed 
	FROM
      TABLE($SEARCH_OPTIMIZATION_HISTORY)
	group by 1,2
	UNION ALL
	select
      to_date(trunc(start_time, 'MONTH')) as usage_month
      ,'REPLICATION' as usage_category					--REPLICATION
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($REPLICATION_USAGE_HISTORY)
	group by 1,2
	UNION ALL 
	
	SELECT
	DATE_TRUNC('MONTH', USAGE_DATE) AS USAGE_MONTH,
	'STORAGE' AS usage_category,
	SUM(CONSUMED_BYTES)/power(1024, 4) AS TOTAL_CONSUMED_TB
FROM
(
SELECT
	USAGE_DATE,
	AVERAGE_STAGE_BYTES AS CONSUMED_BYTES
FROM
	TABLE($STAGE_STORAGE_USAGE_HISTORY)
WHERE
	USAGE_DATE >= '2020-01-01'
UNION
SELECT
	USAGE_DATE,
	AVERAGE_DATABASE_BYTES + AVERAGE_FAILSAFE_BYTES AS CONSUMED_BYTES
FROM
	TABLE($DATABASE_STORAGE_USAGE_HISTORY)
WHERE
	USAGE_DATE >= '2020-01-01'
)
GROUP BY 1
  ) u
order by 1 desc,2
;