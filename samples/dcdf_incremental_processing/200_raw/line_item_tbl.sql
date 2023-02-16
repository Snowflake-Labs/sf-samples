--------------------------------------------------------------------
--  Purpose: raw logical level tables
--     Line item tables
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
create or replace transient table line_item_stg
(
     l_orderkey                     number              not null
    ,o_orderdate                    date                not null
    ,l_partkey                      number              not null
    ,l_suppkey                      number              not null
    ,l_linenumber                   number              not null
    ,l_quantity                     number( 12, 2 )     not null
    ,l_extendedprice                number( 12, 2 )     not null
    ,l_discount                     number( 12, 2 )     not null
    ,l_tax                          number( 12, 2 )     not null
    ,l_returnflag                   varchar( 1 )        not null
    ,l_linestatus                   varchar( 1 )        not null
    ,l_shipdate                     date                not null
    ,l_commitdate                   date                not null
    ,l_receiptdate                  date                not null
    ,l_shipinstruct                 varchar( 25 )       not null
    ,l_shipmode                     varchar( 10 )       not null
    ,l_comment                      varchar( 44 )       not null
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
create or replace table line_item_hist
(
     dw_line_item_shk               binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,l_orderkey                     number              not null
    ,o_orderdate                    date                not null
    ,l_partkey                      number              not null
    ,l_suppkey                      number              not null
    ,l_linenumber                   number              not null
    ,l_quantity                     number( 12, 2 )     not null
    ,l_extendedprice                number( 12, 2 )     not null
    ,l_discount                     number( 12, 2 )     not null
    ,l_tax                          number( 12, 2 )     not null
    ,l_returnflag                   varchar( 1 )        not null
    ,l_linestatus                   varchar( 1 )        not null
    ,l_shipdate                     date                not null
    ,l_commitdate                   date                not null
    ,l_receiptdate                  date                not null
    ,l_shipinstruct                 varchar( 25 )       not null
    ,l_shipmode                     varchar( 10 )       not null
    ,l_comment                      varchar( 44 )       not null
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
create or replace table line_item
(
     dw_line_item_shk               binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,dw_version_ts                  timestamp_ltz       not null
    --
    ,l_orderkey                     number              not null
    ,o_orderdate                    date                not null
    ,l_partkey                      number              not null
    ,l_suppkey                      number              not null
    ,l_linenumber                   number              not null
    ,l_quantity                     number( 12, 2 )     not null
    ,l_extendedprice                number( 12, 2 )     not null
    ,l_discount                     number( 12, 2 )     not null
    ,l_tax                          number( 12, 2 )     not null
    ,l_returnflag                   varchar( 1 )        not null
    ,l_linestatus                   varchar( 1 )        not null
    ,l_shipdate                     date                not null
    ,l_commitdate                   date                not null
    ,l_receiptdate                  date                not null
    ,l_shipinstruct                 varchar( 25 )       not null
    ,l_shipmode                     varchar( 10 )       not null
    ,l_comment                      varchar( 44 )       not null
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
