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
/*
--------------------------------------------------------------------------------

  Use COPY INTO to export data to 1 or more files that start with "line_item" to your users interal Snowflake stage.

  After the COPY INTO completes you can list the files created as follows:

  >list @~ pattern = 'line_item.*';

  The files can later be deleted as follows:

  >remove @~ pattern = 'line_item.*';

  select count(*), min( o_orderdate ), max( o_orderdate ) from snowflake_sample_data_sk.tpch_sf1000.lineitem
  -- 1992-01-01 thru 1998-08-02

--------------------------------------------------------------------------------
*/

use database DEV_WEBINAR_ORDERS_RL_DB;
use schema   TPCH;
use warehouse DEV_WEBINAR_WH;

-- Set variables for this sample data for the time frame to acquire
set l_start_dt = dateadd( day, -16, to_date( '1998-07-02', 'yyyy-mm-dd' ) );
set l_end_dt   = dateadd( day,   1, to_date( '1998-07-02', 'yyyy-mm-dd' ) );

select $l_start_dt, $l_end_dt;


-- run this 2 or 3 times to produce overlapping files with new and modified records.
copy into
    @~/line_item
from
(
    with l_line_item as
    (
        select
              row_number() over(order by uniform( 1, 60, random() ) ) as seq_no
             ,l.l_orderkey
             ,o.o_orderdate
             ,l.l_partkey
             ,l.l_suppkey
             ,l.l_linenumber
             ,l.l_quantity
             ,l.l_extendedprice
             ,l.l_discount
             ,l.l_tax
             ,l.l_returnflag
             ,l.l_linestatus
             ,l.l_shipdate
             ,l.l_commitdate
             ,l.l_receiptdate
             ,l.l_shipinstruct
             ,l.l_shipmode
             ,l.l_comment
        from
            snowflake_sample_data.tpch_sf1000.orders o
            join snowflake_sample_data.tpch_sf1000.lineitem l
              on l.l_orderkey = o.o_orderkey
        where
                o.o_orderdate >= $l_start_dt
            and o.o_orderdate  < $l_end_dt
    )
    select
         l.l_orderkey
        ,l.o_orderdate
        ,l.l_partkey
        ,l.l_suppkey
        ,l.l_linenumber
        ,l.l_quantity
        ,l.l_extendedprice
        ,l.l_discount
        ,l.l_tax
        ,l.l_returnflag
        -- simulate modified data by randomly changing the status
        ,case uniform( 1, 100, random() )
            when  1 then 'A'
            when  5 then 'B'
            when 20 then 'C'
            when 30 then 'D'
            when 40 then 'E'
            else l.l_linestatus
         end                            as l_linestatus
        ,l.l_shipdate
        ,l.l_commitdate
        ,l.l_receiptdate
        ,l.l_shipinstruct
        ,l.l_shipmode
        ,l.l_comment
        ,current_timestamp()            as last_modified_dt -- generating a last modified timestamp as part of data acquisition.
    from
        l_line_item l
    order by
        l.l_orderkey
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
max_file_size    = 16000000
;

list @~/line_item;