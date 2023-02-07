--------------------------------------------------------------------
--  Purpose: data integration/derivation
--     persist intermediate data for calculating the line item margin
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
create or replace table line_item_margin
(
     dw_line_item_shk               binary( 20 )        not null
    --
    ,o_orderdate                    date                not null
    ,margin_amt                     number( 12, 2 )     not null
    ,last_modified_dt               timestamp_ltz       not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
