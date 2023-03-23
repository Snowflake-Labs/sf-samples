--------------------------------------------------------------------
--  Purpose: load into stage table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database dev_webinar_orders_rl_db;
use schema   tpch;
use warehouse dev_webinar_wh;

/* Validation Queries
queries for reviewing loaded data

select * from part_stg limit 1000;
select count(*), count( distinct o_orderkey ) from part_stg;

select dw_file_name, count(*), sum( count(*) ) over() from part_stg group by 1 order by 1;


list @~ pattern = 'part.*';
list @~ pattern = 'part/data.*\.csv\.gz';

put file:///users/<user_name>/test/data_bad.csv @~/part 
*/

-- Truncate/Load Pattern
-- truncate stage prior to bulk load
truncate table part_stg;

-- perform bulk load
copy into
    part_stg
from
    (
    select
         s.$1                                            -- p_partkeyy
        ,s.$2                                            -- p_name
        ,s.$3                                            -- p_mfgr
        ,s.$4                                            -- p_brand
        ,s.$5                                            -- p_type
        ,s.$6                                            -- p_size
        ,s.$7                                            -- p_container
        ,s.$8                                            -- p_retailprice
        ,s.$9                                            -- p_comment
        ,s.$10                                           -- last_modified_dt
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @~ s
    )
purge         = true
pattern       = '.*part/data.*\.csv\.gz'
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
    table(information_schema.copy_history(table_name=>'PART_STG', start_time=> dateadd(hours, -1, current_timestamp())))
where
    status = 'Loaded'
order by
    last_load_time desc
;
