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

  Use COPY INTO to export data to 1 or more files that start with "supplier" to your users interal Snowflake stage.

  After the COPY INTO completes you can list the files created as follows:

  >list @~ pattern = 'supplier.*';

  The files can later be deleted as follows:

  >remove @~ pattern = 'supplier.*';

--------------------------------------------------------------------------------
*/

-- run this 2 or 3 times to produce overlapping files with new and modified records.
copy into
    @~/supplier
from
(
    with l_supplier as
    (
        select
              row_number() over(order by uniform( 1, 60, random() ) ) as seq_no
             ,s.s_suppkey
             ,s.s_name
             ,s.s_address
             ,s.s_nationkey
             ,s.s_phone
             ,s.s_acctbal
             ,s.s_comment
        from
            snowflake_sample_data.tpch_sf1000.supplier s
    )
    select
         s.s_suppkey
        ,s.s_name
        ,s.s_address
        ,s.s_nationkey
        ,s.s_phone
        ,s.s_acctbal
        ,s.s_comment
        ,current_timestamp()            as last_modified_dt -- generating a last modified timestamp as supplier of data acquisition.
    from
        l_supplier s
    order by
        s.s_suppkey
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
max_file_size    = 16000000
;
