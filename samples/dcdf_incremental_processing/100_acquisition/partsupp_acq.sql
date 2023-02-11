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

  Use COPY INTO to export data to 1 or more files that start with "partsupp" to your users interal Snowflake stage.

  After the COPY INTO completes you can list the files created as follows:

  >list @~ pattern = 'partsupp.*';

  The files can later be deleted as follows:

  >remove @~ pattern = 'partsupp.*';

  select count(*), min( o_orderdate ), max( o_orderdate ) from snowflake_sample_data_sk.tpch_sf1000.lineitem
  -- 1992-01-01 thru 1998-08-02

--------------------------------------------------------------------------------
*/

-- run this 2 or 3 times to produce overlapping files with new and modified records.
copy into
    @~/partsupp
from
(
    with l_partsupp as
    (
        select
              row_number() over(order by uniform( 1, 60, random() ) ) as seq_no
             ,p.ps_partkey
             ,p.ps_suppkey
             ,p.ps_availqty
             ,p.ps_supplycost
             ,p.ps_comment
        from
            snowflake_sample_data.tpch_sf1000.partsupp p
    )
    select
         p.ps_partkey
        ,p.ps_suppkey
        ,p.ps_availqty
        ,p.ps_supplycost
        ,p.ps_comment
        ,current_timestamp()            as last_modified_dt -- generating a last modified timestamp as partsupp of data acquisition.
    from
        l_partsupp p
    order by
        p.ps_partkey
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
max_file_size    = 16000000
;
