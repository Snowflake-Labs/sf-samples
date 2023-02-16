--------------------------------------------------------------------
--  Purpose: raw logical level tables
--      for Customer data
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
create or replace transient table customer_stg
(
     c_custkey                      number              not null
    ,change_date                    timestamp_ltz       not null
    ,c_name                         varchar( 25 )       not null
    ,c_address                      varchar( 40 )       not null
    ,c_nationkey                    number              not null
    ,c_phone                        varchar( 15 )       not null
    ,c_acctbal                      number              not null
    ,c_mktsegment                   varchar( 10 )       not null
    ,c_comment                      varchar( 117 )      not null
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
create or replace table customer_hist
(
     dw_customer_shk                binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,change_date                    timestamp_ltz       not null
    ,c_custkey                      number              not null
    ,c_name                         varchar( 25 )       not null
    ,c_address                      varchar( 40 )       not null
    ,c_nationkey                    number              not null
    ,c_phone                        varchar( 15 )       not null
    ,c_acctbal                      number              not null
    ,c_mktsegment                   varchar( 10 )       not null
    ,c_comment                      varchar( 117 )      not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
