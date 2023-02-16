--------------------------------------------------------------------
--  Purpose: create table to manage incremental processing
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_common_db};
use SCHEMA &{l_common_schema};

create or replace transient table dw_delta_date
(
     event_dt       timestamp_ltz not null
    ,dw_load_ts     timestamp_ltz not null
)
data_retention_time_in_days = 0
;


