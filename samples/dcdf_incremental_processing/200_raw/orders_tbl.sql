--------------------------------------------------------------------
--  Purpose: raw logical level tables for orders
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_raw_db};
use schema   &{l_raw_schema};

--
-- transient staging table with no retention days
--
create or replace transient table orders_stg
(
     o_orderkey                     number              not null
    ,o_custkey                      number              not null
    ,o_orderstatus                  varchar( 1 )        not null
    ,o_totalprice                   number( 12, 2 )     not null
    ,o_orderdate                    date                not null
    ,o_orderpriority                varchar( 15 )       not null
    ,o_clerk                        varchar( 15 )       not null
    ,o_shippriority                 number              not null
    ,o_comment                      varchar( 79 )       not null
    ,last_modified_dt               timestamp_ltz       not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 0
copy grants
;

--
-- permanent history table with retention days
--
create or replace table orders_hist
(
     dw_order_shk                   binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,o_orderkey                     number              not null
    ,o_custkey                      number              not null
    ,o_orderstatus                  varchar( 1 )        not null
    ,o_totalprice                   number( 12, 2 )     not null
    ,o_orderdate                    date                not null
    ,o_orderpriority                varchar( 15 )       not null
    ,o_clerk                        varchar( 15 )       not null
    ,o_shippriority                 number              not null
    ,o_comment                      varchar( 79 )       not null
    ,last_modified_dt               timestamp_ltz       not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;

--
-- permanent latest table with retention days
--
create or replace table orders
(
     dw_order_shk                   binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,o_orderkey                     number              not null
    ,o_custkey                      number              not null
    ,o_orderstatus                  varchar( 1 )        not null
    ,o_totalprice                   number( 12, 2 )     not null
    ,o_orderdate                    date                not null
    ,o_orderpriority                varchar( 15 )       not null
    ,o_clerk                        varchar( 15 )       not null
    ,o_shippriority                 number              not null
    ,o_comment                      varchar( 79 )       not null
    ,last_modified_dt               timestamp_ltz       not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
