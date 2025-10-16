// FOR ORG VERSION
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;

--ORG GRAIN
--ORG UNIT ECONOMIC METRICS
--runtime variables
--Use the settings below to control behavior
set time_slice = 'week';  --use 'month', 'week', 'quarter' for size of time buckets
set start_rpt  = date_trunc($time_slice,(current_date() - 90)); --Start of Time Period for Report
set end_rpt    = current_date() - 0; --last day of report. Current logic assumes end date is beyond last day of last full period

-- num_periods is the number of periods that will be pivoted..
set num_periods = (select count(1) from
(select (date(date_trunc($time_slice,start_time))) time_period from WAREHOUSE_METERING_HISTORY
where start_time > $start_rpt AND start_time < $end_rpt 
group by all));
--Calculate last ordinal position of the last full time period
set cols3_last_pos = '$' || ($num_periods+3-1)::varchar; --calculate next to last period with 3 leading cols. used for delta calculations
set cols2_last_pos = '$' || ($num_periods+2-1)::varchar; --calculate next to last period with 2 leading cols. used for delta calculations
set cols1_last_pos = '$' || ($num_periods+1-1)::varchar; --calculate next to last period with 1 leading cols. used for delta calculations

CREATE OR REPLACE TRANSIENT TABLE ORG_UNIT_ECON_ORG AS
with  wh_metering as (
select 
     date(date_trunc('week',start_time)) time_period
    ,round(sum(credits_used_compute),2) credits_used
from SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY       
--from SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY
where start_time > $start_rpt AND start_time < $end_rpt  
group by all
),
warehouse_load as (
select account_name,warehouse_name,
    iff(avg(avg_running)>=3,1,0) num_whs_high_concurrency,
    iff(avg(avg_running)<3,1,0) num_whs_low_concurrency,
    date(date_trunc('week',start_time)) time_period
from  SNOWFLAKE.ORGANIZATION_USAGE.warehouse_load_history 
where start_time > $start_rpt AND start_time < $end_rpt  
group by all
),
wh_economic_metrics as (
select 
    date(date_trunc('week',start_time)) time_period,
    sum(1) as query_count,
    count(distinct(user_name)) active_users,
    round(sum(bytes_scanned)/1024/1024/1024,2) gb_scanned, 
    round(sum(iff(query_load_percent>=75,execution_time,0))/1000/60/60,2) as execution_time_high_util_hrs,
    round(sum(execution_time)/1000/60/60,2) as execution_time_hrs
FROM SNOWFLAKE.ORGANIZATION_USAGE.QUERY_HISTORY
where start_time > $start_rpt AND start_time < $end_rpt  
    and warehouse_name not like 'COMPUTE_SERVICE_WH%' and execution_time > 0
    and cluster_number is not null
group by all
),
warehouse_load_groupby as (
select time_period,sum(num_whs_high_concurrency) num_whs_high_concurrency,sum(num_whs_low_concurrency) num_whs_low_concurrency 
from warehouse_load
group by all
),
combined as (
select  wmet.time_period, active_users,sum(wmet.credits_used) credits_used_calc ,
    sum(wem.query_count) as query_count_calc, 
    round(sum(gb_scanned),1) as gb_scanned_calc,
    round(div0null(credits_used_calc,query_count_calc/1000),2) credits_per_thousand_queries, 
    round(div0null(execution_time_high_util_hrs,execution_time_hrs)*100,2) pct_wh_highly_utilized, 
    round(div0null(credits_used_calc,gb_scanned_calc),2) credits_per_gb_scanned
from wh_metering wmet
left outer join wh_economic_metrics wem
    ON ( wmet.time_period = wem.time_period)
group by all
),
all_results as (
select * from (select 'COMPUTE CREDITS' as metric,time_period,credits_used from wh_metering)
         pivot(sum(credits_used) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'NUM WHS WITH LOW CONCURRENCY' as metric,time_period,num_whs_low_concurrency from warehouse_load_groupby) 
pivot(sum(num_whs_low_concurrency) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'NUM WHS WITH HIGH CONCURRENCY' as metric,time_period,num_whs_high_concurrency from warehouse_load_groupby) 
pivot(sum(num_whs_high_concurrency) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'ACTIVE USERS' as metric,time_period,active_users from wh_economic_metrics) 
pivot(sum(active_users) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'EFFICIENCY SCORE (CREDITS/1000 QRYS)' as metric,time_period,credits_per_thousand_queries from combined) 
pivot(sum(credits_per_thousand_queries) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'NUM QUERIES EXECUTED' as metric,time_period,query_count from wh_economic_metrics) 
pivot(sum(query_count) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'GB SCANNED' as metric,time_period,gb_scanned from wh_economic_metrics) 
pivot(sum(gb_scanned) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'PCT QUERY HRS RUNNING HIGHLY DISTRIB' as metric,time_period,pct_wh_highly_utilized from combined) 
pivot(sum(pct_wh_highly_utilized) for time_period in (ANY order by time_period))
UNION ALL
select * from (select 'QUERY RUNTIME DURATION (HRS)' as metric,time_period,execution_time_hrs from wh_economic_metrics) 
pivot(sum(execution_time_hrs) for time_period in (ANY order by time_period))
)
select 
*,
    identifier($cols1_last_pos)-$2                               as "BEGIN/END DELTA",
    round(100 * div0null((identifier($cols1_last_pos)-$2),$2),1) as "BEGIN/END % CHANGE",
 from all_results;
;