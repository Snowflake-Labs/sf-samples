--------------------------------------------------------------------
--  Purpose: data acquisition
--      This data acquisition script uses snowflake sample data to 
--      simulate the acquistion of new, modified and duplicate 
--      records.
--
--      Note:  This uses the login internal stage @~.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database DEV_WEBINAR_ORDERS_RL_DB;
use schema   TPCH;
use warehouse DEV_WEBINAR_WH;


/*
--------------------------------------------------------------------------------

  Use COPY INTO to export data to 1 or more files that start with "orders" to your users interal Snowflake stage.

  After the COPY INTO completes you can list the files created as follows:

  >list @~ pattern = 'orders.*';

  The files can later be deleted as follows:

  >remove @~ pattern = 'orders.*';

  select count(*), min( o_orderdate ), max( o_orderdate ) from snowflake_sample_data_sk.tpch_sf1000.orders
  -- 1992-01-01 thru 1998-08-02

--------------------------------------------------------------------------------
*/

-- run this 2 or 3 times to produce overlapping files with new and modified records.
copy into
    @~/orders
from
(
    with l_order as
    (
        select
              row_number() over(order by uniform( 1, 60, random() ) ) as seq_no
             ,o.o_orderkey
             ,o.o_custkey
             ,o.o_orderstatus
             ,o.o_totalprice
             ,o.o_orderdate
             ,o.o_orderpriority
             ,o.o_clerk
             ,o.o_shippriority
             ,o.o_comment
        from
            snowflake_sample_data.tpch_sf1000.orders o
        where
                o.o_orderdate >= dateadd( day, -16, to_date( '1998-07-02', 'yyyy-mm-dd' ) )
            and o.o_orderdate  < dateadd( day,   1, to_date( '1998-07-02', 'yyyy-mm-dd' ) )
    )
    select
         lo.o_orderkey
        ,lo.o_custkey
        -- simulate modified data by randomly changing the status
        ,case uniform( 1, 100, random() )
            when  1 then 'A'
            when  5 then 'B'
            when 20 then 'C'
            when 30 then 'D'
            when 40 then 'E'
            else lo.o_orderstatus
         end                            as o_orderstatus
        ,lo.o_totalprice
        ,lo.o_orderdate
        ,lo.o_orderpriority
        ,lo.o_clerk
        ,lo.o_shippriority
        ,lo.o_comment
        ,current_timestamp()            as last_modified_dt -- generating a last modified timestamp as part of data acquisition.
    from
        l_order lo
    order by
        lo.o_orderdate
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
max_file_size    = 16000000
;

list @~/orders;