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

/*
select * from orders_hist limit 1000;

total rows and distinct hash diff should always be identical
distinct hash key count will always be less

select count(*), count( distinct o_orderkey ) from orders_stg;

select count(*), count( distinct dw_order_shk ), count( distinct dw_hash_diff ) from orders_hist;
*/

execute immediate $$

declare
   l_start_dt date;
   l_end_dt   date;
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
    insert into orders_hist
    with l_stg as
    (
        --
        -- Driving CTE to identify all records in the logical partition to be processed.
        select
            -- generate hash key and hash diff to streamline processing
             sha1_binary( s.o_orderkey )  as dw_order_shk
            -- note that last_modified_dt is not included in the hash diff since it only represents recency of the record versus an 
            -- actual meaningful change in the data
            ,sha1_binary( concat( s.o_orderkey
                                 ,'|', coalesce( to_char( s.o_custkey ), '~' )
                                 ,'|', coalesce( trim( s.o_orderstatus ), '~' )
                                 ,'|', coalesce( to_char( s.o_totalprice ), '~' )
                                 ,'|', coalesce( to_char( s.o_orderdate, 'yyyymmdd' ), '~' )
                                 ,'|', coalesce( s.o_orderpriority, '~' )
                                 ,'|', coalesce( s.o_clerk, '~' )
                                 ,'|', coalesce( s.o_shippriority, '~' )
                                 ,'|', coalesce( s.o_comment, '~' )
                                )
        
                        )               as dw_hash_diff
            ,s.*
        from
            orders_stg s
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
            (
            select
                 -- identify dupes and only keep copy 1
                 -- note this is deduping to unique versions of a record versus on the primary key
                 row_number() over( partition by dw_hash_diff order by s.last_modified_dt desc, s.dw_file_row_no ) as seq_no
                ,s.*
            from
                l_stg s
            )
        where
            seq_no = 1 -- keep only unique rows
    )
    select
         s.dw_order_shk
        ,s.dw_hash_diff
        ,s.dw_load_ts             as dw_version_ts
        ,s.o_orderkey
        ,s.o_custkey
        ,s.o_orderstatus
        ,s.o_totalprice
        ,s.o_orderdate
        ,s.o_orderpriority
        ,s.o_clerk
        ,s.o_shippriority
        ,s.o_comment
        ,s.last_modified_dt
        ,s.dw_file_name
        ,s.dw_file_row_no
        ,current_timestamp()    as dw_load_ts
    from
        l_deduped s
    where
        s.dw_hash_diff not in
        (
            select dw_hash_diff from orders_hist 
            where
                    o_orderdate >= :l_start_dt
                and o_orderdate  < :l_end_dt
        )
    order by
        o_orderdate  -- physically sort rows by a logical partitioning date
    ;
  end for;

  return 'SUCCESS';

end;
$$
;
