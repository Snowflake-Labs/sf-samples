--------------------------------------------------------------------
--  Purpose: data presentation
--     create supplier_dm table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_pl_db};
use schema   &{l_pl_schema};

--
-- permanent latest table with retention days
--
create or replace table supplier_dm
(
     dw_supplier_shk                binary( 20 )        not null
    --
    ,s_suppkey                      number              not null
    ,s_name                         varchar( 25 )       not null
    ,s_address                      varchar( 40 )       not null
    ,s_nationkey                    number              not null
    ,s_phone                        varchar( 15 )       not null
    ,s_acctbal                      number( 12, 2 )     not null
    ,s_comment                      varchar( 101 )      not null
    ,last_modified_dt               timestamp_ltz       not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
