-------------------------------------------------
-- NAME:	 PERF-IS-25-concurrency-hwm.txt
-------------------------------------------------
-- DESCRIPTION:
--	Calculates High Water Mark and average HWM for a warehouse
--
-- OUTPUT:
--	Understand HWM and average HWM for a warehouse
--
-- NEXT STEPS:
--	Further analyze concurrency to understand full impact
--	Address throughput (SQL & Cluster Keys)
--	Address throughput (scale up… vWH sizing)
--	Address concurrency (scale out…increase clusters)
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
----------------------------------
-- TEMP_JOBS
----------------------------------
WITH temp_jobs as
(
select 
	date_trunc('month',START_TIME)::DATE AS YEAR_MONTH ,
    QUERY_ID AS QUERY_ID,
	dateadd(seconds, (QUEUED_PROVISIONING_TIME / 1000), START_TIME)::timestamp_ltz as usage_at,
	+1 as type,
	Warehouse_name ,
	row_number() 
		over(partition by MONTH(START_TIME),Warehouse_name
		order by dateadd(seconds, (QUEUED_PROVISIONING_TIME / 1000), START_TIME)::timestamp_ltz) 
		as start_ordinal
FROM
    TABLE(information_schema.query_history(
		END_TIME_RANGE_START =>$TS_START,
		END_TIME_RANGE_END =>$TS_END)) 
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END AND
    QUERY_TYPE in ('COPY', 'INSERT', 'MERGE', 'UNLOAD', 'RECLUSTER','SELECT','DELETE', 'CREATE_TABLE_AS_SELECT', 'UPDATE')  
  
union all
    
select 
	date_trunc('month',START_TIME)::DATE AS YEAR_MONTH,
    QUERY_ID AS QUERY_ID,
	end_time as usage_at,
	-1 as type,
	Warehouse_name ,
	null as start_ordinal
FROM
    TABLE(information_schema.query_history(
		END_TIME_RANGE_START=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		END_TIME_RANGE_END=>CURRENT_TIMESTAMP()))
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END AND
    QUERY_TYPE in ('COPY', 'INSERT', 'MERGE', 'UNLOAD', 'RECLUSTER','SELECT','DELETE', 'CREATE_TABLE_AS_SELECT', 'UPDATE')
), 
----------------------------------
-- JOB_CONCURRENCY_PREP
----------------------------------
job_concurrency_prep as
(
select 
	*,
	row_number() 
		over(partition by year_month, warehouse_name 
		order by usage_at, type) as start_or_end_ordinal
from temp_jobs
)
----------------------------------
-- SELECT
----------------------------------
select distinct year_month, warehouse_name, highwater_mark_of_concurrency, avg_highwater_mark_of_concurrency_per_minute
from
(
select 
	year_month,
	warehouse_name,
	date_trunc(minute, usage_at) as usage_minute,
	max(max(2 * start_ordinal - start_or_end_ordinal)) 
		over(partition by year_month, warehouse_name) as highwater_mark_of_concurrency,
	avg(max(2 * start_ordinal - start_or_end_ordinal)) 
		over(partition by year_month, warehouse_name) as avg_highwater_mark_of_concurrency_per_minute
from 
	job_concurrency_prep
where 
	type = 1
group by 
	1,2,3
order by 
	1,2,3,4)
order by 
	1
;