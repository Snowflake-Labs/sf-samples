--------------------------------------------------------------------
--  Purpose: insert new and modified values into history table
--      This shows an insert-only pattern where every version of a 
--      received row is kept.
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
select * from customer_hist limit 1000;

total rows and distinct hash diff should always be identical
distinct hash key count will always be less

select count(*), count( distinct c_custkey ) from customer_stg;

select count(*), count( distinct dw_customer_shk ), count( distinct dw_hash_diff ) from customer_hist;
*/

execute immediate $$

declare
  l_start_dt date;
  l_end_dt   date;
  -- Grab the dates for the logical partitions to process
  c1 cursor for select start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f('week')) order by 1;

begin

  --
  -- Loop through the dates to incrementally process based on the logical partition definition.
  -- In this example, the logical partitions are by week.
  --
  for record in c1 do
    l_start_dt := record.start_dt;
    l_end_dt   := record.end_dt;

    --
    -- insert new and modified records into history table with version date
    -- dedupe source records as part of insert
    --
    -- run a second time to see that no rows will be inserted
    --
    insert into customer_hist
    with l_stg as
    (
        --
        -- Driving CTE to identify all records in the logical partition to be processed.
        select
            -- generate hash key and hash diff to streamline processing
             sha1_binary( s.c_custkey || to_char( s.change_date, 'yyyymmdd' ) )  as dw_customer_shk
            --
            -- note that last_modified_dt is not included in the hash diff since it only represents recency of the record versus an 
            -- actual meaningful change in the data
            ,sha1_binary( concat( s.c_custkey || to_char( s.change_date, 'yyyymmdd' )
                                         ,'|', coalesce( to_char( s.change_date, 'yyyymmdd'), '~' )
                                         ,'|', coalesce( s.c_name, '~' )
                                         ,'|', coalesce( s.c_address, '~' )
                                         ,'|', coalesce( to_char( s.c_nationkey ), '~' )
                                         ,'|', coalesce( s.c_phone, '~' )
                                         ,'|', coalesce( to_char( s.c_acctbal ), '~' )
                                         ,'|', coalesce( s.c_mktsegment, '~' )
                                         ,'|', coalesce( s.c_comment, '~' )
                                )
        
                        )               as dw_hash_diff
            ,s.*
        from
            customer_stg s
        where
                s.change_date >= :l_start_dt
            and s.change_date  < :l_end_dt
    )
    ,l_deduped as
    (
        --
        -- Dedupe the records from the staging table.
        -- This assumes that there may be late arriving or duplicate data that were loaded
        -- Need to identify the most recent record and use that to update the Current state table.
        -- as there is no reason to process each individual change in the record, the last one would have the most recent updates
        select
            *
        from
            l_stg
        qualify
            row_number() over( partition by dw_hash_diff order by change_date desc, dw_file_row_no )  = 1
    )
    select
         s.dw_customer_shk
        ,s.dw_hash_diff
        ,s.dw_load_ts             as dw_version_ts
        ,s.change_date
        ,s.c_custkey
        ,s.c_name
        ,s.c_address
        ,s.c_nationkey
        ,s.c_phone
        ,s.c_acctbal
        ,s.c_mktsegment
        ,s.c_comment
        ,s.dw_file_name
        ,s.dw_file_row_no
        ,current_timestamp()    as dw_load_ts
    
    from
        l_deduped s
    where
        s.dw_hash_diff not in
        (
            select dw_hash_diff from customer_hist 
        )
    order by
        c_custkey  -- physically sort rows by a logical partitioning date
    ;

  end for;

  return 'SUCCESS';

end;
$$
;

select *
from dev_webinar_orders_rl_db.tpch.customer_hist
where c_custkey in (50459048)
order by 5;
