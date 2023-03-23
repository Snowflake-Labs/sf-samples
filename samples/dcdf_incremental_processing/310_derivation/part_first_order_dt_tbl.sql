--------------------------------------------------------------------
--  Purpose: data integration/derivation
--     persist intermediate results for deriving first order date for a part
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_il_db};
use schema   &{l_il_schema};

--
-- permanent latest table with retention days
--
create or replace table part_first_order_dt
(
     dw_part_shk                    binary( 20 )        not null
    --
    ,first_orderdate                date                not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
