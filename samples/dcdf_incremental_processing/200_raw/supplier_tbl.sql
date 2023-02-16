--------------------------------------------------------------------
--  Purpose: raw logical level table
--      for the supplier data
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
create or replace transient table supplier_stg
(
     s_suppkey                      number              not null
    ,s_name                         varchar( 25 )       not null
    ,s_address                      varchar( 40 )       not null
    ,s_nationkey                    number              not null
    ,s_phone                        varchar( 15 )       not null
    ,s_acctbal                      number( 12, 2 )     not null
    ,s_comment                      varchar( 101 )      not null
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
-- permanent latest table with retention days
--
create or replace table supplier
(
     dw_supplier_shk               binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
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
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
