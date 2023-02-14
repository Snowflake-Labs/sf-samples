--------------------------------------------------------------------
--  Purpose: data presentation
--      customer dimension table.
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
create or replace table customer_dm
(
     dw_customer_shk                binary( 20 )        not null
    --
    ,c_custkey                      number              not null
    ,c_name                         varchar( 25 )       not null
    ,c_address                      varchar( 40 )       not null
    ,c_nationkey                    number              not null
    ,c_phone                        varchar( 15 )       not null
    ,c_acctbal                      number              not null
    ,c_mktsegment                   varchar( 10 )       not null
    ,c_comment                      varchar( 117 )      not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
