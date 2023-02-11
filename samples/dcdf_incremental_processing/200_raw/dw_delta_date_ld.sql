--------------------------------------------------------------------
--  Purpose: Example of how to identify the logical partitions that 
--    need to be processed as part of the incremental processing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database dev_webinar_common_db;
use schema util;
use warehouse dev_webinar_wh;
insert overwrite into dw_delta_date
with l_delta_date as
(
    select distinct
        o_orderdate as event_dt
    from
        dev_webinar_orders_rl_db.tpch.line_item_stg 
)
select
     event_dt
    ,current_timestamp()            as dw_load_ts
from
    l_delta_date
order by
    1
;

select * 
from dev_webinar_common_db.util.dw_delta_date
order by event_dt;

select start_dt, end_dt 
FROM table(dev_webinar_common_db.util.dw_delta_date_range_f('week')) 
order by 1;