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

  Use COPY INTO to export data to 1 or more files that start with "customer" to your users interal Snowflake stage.

  After the COPY INTO completes you can list the files created as follows:

  >list @~ pattern = 'customer.*';

  The files can later be deleted as follows:

  >remove @~ pattern = 'customer.*';

  select count(*), min( o_orderdate ), max( o_orderdate ) from snowflake_sample_data_sk.tpch_sf1000.lineitem
  -- 1992-01-01 thru 1998-08-02

--------------------------------------------------------------------------------
*/

-- run this 2 or 3 times to produce overlapping files with new and modified records.
copy into
    @~/customer
from
(
    with l_customer as
    (
        select
              row_number() over(order by uniform( 1, 60, random() ) ) as seq_no
             ,c_custkey
             ,change_date
             ,c_name
             ,c_address
             ,c_nationkey
             ,c_phone
             ,c_acctbal
             ,c_mktsegment
             ,c_comment
        from
            (
              select
                    c.c_custkey
                   ,to_date('1/1/1992','mm/dd/yyyy') as change_date
                   ,c.c_name
                   ,c.c_address
                   ,c.c_nationkey
                   ,c.c_phone
                   ,c.c_acctbal
                   ,c.c_mktsegment
                   ,c.c_comment
              from
                  snowflake_sample_data.tpch_sf1000.customer c
              --
              union all
              --
              -- Create some changed data for specific customers
              --
              select
                   c.c_custkey
                   ,to_date('6/20/1998','mm/dd/yyyy') as change_date
                   ,c.c_name
                   ,c.c_address
                   ,c.c_nationkey
                   ,c.c_phone
                   ,c_acctbal + 1000 as c_acctbal
                   ,c.c_mktsegment
                   ,c.c_comment
              from
                  snowflake_sample_data.tpch_sf1000.customer c
              where
                  c_custkey in (50459048)
        )
    )
    select
         c.c_custkey
        ,c.change_date
        ,c.c_name
        ,c.c_address
        ,c.c_nationkey
        ,c.c_phone
        ,c.c_acctbal
        ,c.c_mktsegment
        ,c.c_comment
    from
        l_customer c
    order by
        c.c_custkey
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
max_file_size    = 16000000
;
