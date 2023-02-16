--------------------------------------------------------------------
--  Purpose: raw logical level tables
--      part supplier data tables
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
create or replace transient table partsupp_stg
(
     ps_partkey                     number              not null
    ,ps_suppkey                     number              not null
    ,ps_availqty                    number              not null
    ,ps_supplycost                  number( 12, 2 )     not null
    ,ps_comment                     varchar( 199 )      not null
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
create or replace table partsupp_hist
(
     dw_partsupp_shk               binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,ps_partkey                     number              not null
    ,ps_suppkey                     number              not null
    ,ps_availqty                    number              not null
    ,ps_supplycost                  number( 12, 2 )     not null
    ,ps_comment                     varchar( 199 )      not null
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
create or replace table partsupp
(
     dw_partsupp_shk               binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,ps_partkey                     number              not null
    ,ps_suppkey                     number              not null
    ,ps_availqty                    number              not null
    ,ps_supplycost                  number( 12, 2 )     not null
    ,ps_comment                     varchar( 199 )      not null
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
