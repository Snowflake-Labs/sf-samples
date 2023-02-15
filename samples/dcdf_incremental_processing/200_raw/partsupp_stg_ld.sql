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

select * from partsupp_stg limit 1000;
select count(*), count( distinct o_orderkey ) from partsupp_stg;

select dw_file_name, count(*), sum( count(*) ) over() from partsupp_stg group by 1 order by 1;


list @~ pattern = 'partsupp.*';
list @~ pattern = 'partsupp/data.*\.csv\.gz';

put file:///users/<user_name>/test/data_bad.csv @~/partsupp 
*/

-- Truncate/Load Pattern
-- truncate stage prior to bulk load
truncate table partsupp_stg;

-- perform bulk load
copy into
    partsupp_stg
from
    (
    select
         s.$1                                            -- ps_partkey
        ,s.$2                                            -- ps_suppkey
        ,s.$3                                            -- ps_availqty
        ,s.$4                                            -- ps_supplycost
        ,s.$5                                            -- ps_comment
        ,s.$6                                            -- last_modified_dt
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @~ s
    )
purge         = true
pattern       = '.*partsupp/data.*\.csv\.gz'
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
    table(information_schema.copy_history(table_name=>'PARTSUPP', start_time=> dateadd(hours, -1, current_timestamp())))
where
    status = 'Loaded'
order by
    last_load_time desc
;
