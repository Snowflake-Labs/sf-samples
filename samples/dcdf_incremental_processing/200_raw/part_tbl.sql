--------------------------------------------------------------------
--  Purpose: raw logical level tables
--      for Part level data
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
create or replace transient table part_stg
(
     p_partkey                      number              not null
    ,p_name                         varchar( 55 )       not null
    ,p_mfgr                         varchar( 25 )       not null
    ,p_brand                        varchar( 10 )       not null
    ,p_type                         varchar( 25 )       not null
    ,p_size                         number              not null
    ,p_container                    varchar( 10 )       not null
    ,p_retailprice                  number( 12, 2 )     not null
    ,p_comment                      varchar( 23 )       not null
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
create or replace table part
(
     dw_part_shk               binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,p_partkey                      number              not null
    ,p_name                         varchar( 55 )       not null
    ,p_mfgr                         varchar( 25 )       not null
    ,p_brand                        varchar( 10 )       not null
    ,p_type                         varchar( 25 )       not null
    ,p_size                         number              not null
    ,p_container                    varchar( 10 )       not null
    ,p_retailprice                  number( 12, 2 )     not null
    ,p_comment                      varchar( 23 )       not null
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
