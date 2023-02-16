--------------------------------------------------------------------
--  Purpose: data presentation
--      part dimension table.
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
create or replace table part_dm
(
     dw_part_shk                    binary( 20 )        not null
    --
    ,p_partkey                      number              not null
    ,part_name                      varchar( 55 )       not null
    ,mfgr                           varchar( 25 )       not null
    ,brand                          varchar( 10 )       not null
    ,type                           varchar( 25 )       not null
    ,size                           number              not null
    ,container                      varchar( 10 )       not null
    ,retail_price                   number( 12, 2 )     not null
    ,comment                        varchar( 23 )       not null
    ,first_orderdate                date                
    ,last_modified_dt               timestamp_ltz       not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
