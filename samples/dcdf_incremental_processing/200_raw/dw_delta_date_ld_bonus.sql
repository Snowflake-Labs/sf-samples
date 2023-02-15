--------------------------------------------------------------------
--  Purpose: Example of how to identify the logical partitions that 
--    need to be processed as part of the incremental processing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database dev_webinar_common_db;
use schema   util;
use warehouse dev_webinar_wh;

insert overwrite into dw_delta_date
with l_delta_date as
(
    select distinct
        o_orderdate as event_dt
    from
        dev_webinar_orders_rl_db.tpch.line_item_stg 
    --
    union 
    --
    select distinct
        change_date
    from
        dev_webinar_orders_rl_db.tpch.customer_stg
)
select
     event_dt
    ,current_timestamp()            as dw_load_ts
from
    l_delta_date
order by
    1
;
