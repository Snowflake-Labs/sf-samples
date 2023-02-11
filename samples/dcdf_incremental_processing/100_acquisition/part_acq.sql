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

  Use COPY INTO to export data to 1 or more files that start with "part" to your users interal Snowflake stage.

  After the COPY INTO completes you can list the files created as follows:

  >list @~ pattern = 'part.*';

  The files can later be deleted as follows:

  >remove @~ pattern = 'part.*';

  select count(*), min( o_orderdate ), max( o_orderdate ) from snowflake_sample_data_sk.tpch_sf1000.lineitem
  -- 1992-01-01 thru 1998-08-02

--------------------------------------------------------------------------------
*/

-- run this 2 or 3 times to produce overlapping files with new and modified records.
copy into
    @~/part
from
(
    with l_part as
    (
        select
              row_number() over(order by uniform( 1, 60, random() ) ) as seq_no
             ,p.p_partkey
             ,p.p_name
             ,p.p_mfgr
             ,p.p_brand
             ,p.p_type
             ,p.p_size
             ,p.p_container
             ,p.p_retailprice
             ,p.p_comment
        from
            snowflake_sample_data.tpch_sf1000.part p
    )
    select
         p.p_partkey
        ,p.p_name
        ,p.p_mfgr
        ,p.p_brand
        ,p.p_type
        ,p.p_size
        ,p.p_container
        ,p.p_retailprice
        ,p.p_comment
        ,current_timestamp()            as last_modified_dt -- generating a last modified timestamp as part of data acquisition.
    from
        l_part p
    order by
        p.p_partkey
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
max_file_size    = 16000000
;

list @~/part;