--------------------------------------------------------------------
--  Purpose: data presentation
--      atomic level order line table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_pl_db};
use schema   &{l_pl_schema};

--
-- permanent history table with retention days
--
create or replace table order_line_fact
(
     dw_line_item_shk               binary( 20 )        not null
    ,orderdate                      date                not null
    ,dw_order_shk                   binary( 20 )        not null
    ,dw_part_shk                    binary( 20 )        not null
    ,dw_supplier_shk                binary( 20 )        
    --
    ,quantity                       number( 12, 2 )     not null
    ,extendedprice                  number( 12, 2 )     not null
    ,discount                       number( 12, 2 )     not null
    ,tax                            number( 12, 2 )     not null
    ,returnflag                     varchar( 1 )        not null
    ,linestatus                     varchar( 1 )        not null
    ,shipdate                       date        
    ,commitdate                     date       
    ,receiptdate                    date      
    ,margin_amt                     number( 12, 2 )     not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;

