--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database dev_webinar_order_rl_db;
use schema   tpch;
use warehouse dev_webinar_wh;

/* Validation Queries
queries for reviewing loaded data

select * from supplier_stg limit 1000;
select count(*), count( distinct o_orderkey ) from supplier_stg;

select dw_file_name, count(*), sum( count(*) ) over() from supplier_stg group by 1 order by 1;


list @~ pattern = 'supplier.*';
list @~ pattern = 'supplier/data.*\.csv\.gz';

put file:///users/<user_name>/test/data_bad.csv @~/supplier 
*/

-- Truncate/Load Pattern
-- truncate stage prior to bulk load
truncate table supplier_stg;

-- perform bulk load
copy into
    supplier_stg
from
    (
    select
         s.$1                                            -- s_suppkey
        ,s.$2                                            -- s_name
        ,s.$3                                            -- s_address
        ,s.$4                                            -- s_nationkey
        ,s.$5                                            -- s_phone
        ,s.$6                                            -- s_acctbal
        ,s.$7                                            -- s_commenter
        ,s.$8                                            -- last_modified_dt
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @~ s
    )
purge         = true
pattern       = '.*supplier/data.*\.csv\.gz'
file_format   = ( type=csv field_optionally_enclosed_by = '"' )
on_error      = skip_file
--validation_mode = return_all_errors
;

--
-- review history of load errors
--
select 
    *
from 
    table(information_schema.copy_history(table_name=>'SUPPLIER_STG', start_time=> dateadd(hours, -1, current_timestamp())))
where
    status != 'Loaded'
order by
    last_load_time desc
;
