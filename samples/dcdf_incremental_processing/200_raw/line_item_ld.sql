--------------------------------------------------------------------
--  Purpose: merge new and modified values into target table
--      This shows a merge pattern where only the most recent version
--      of a given primary keys record is kept.
--
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
truncate table line_item;

select * from line_item limit 1000;

all 3 counts should be identical

select count(*), count( distinct l_orderkey ), min( o_orderdate ), max( o_orderdate ) from line_item_stg;

select count(*), count( distinct dw_line_item_shk ), count( distinct dw_hash_diff ) from line_item;
*/

-- Use Anonymous block SQL Scripting
execute immediate $$

declare
  l_start_dt date;
  l_end_dt   date;
  -- Grab the dates for the logical partitions to process
  c1 cursor for select start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f('week')) order by 1;
  
begin
    
  --
  -- Loop through the dates to incrementally process based on the logical partitions.  
  -- In this example, the logical partiitons are by week.
  --
  for record in c1 do
    l_start_dt := record.start_dt;
    l_end_dt   := record.end_dt;

    --
    -- Merge Pattern 
    --
    merge into line_item tgt using
    (
        with l_stg as
        (
            --
            -- Driving CTE to identify all records in the logical partition to be processed
            --
            select
                -- generate hash key and hash diff to streamline processing
                 sha1_binary( concat( s.l_orderkey, '|', s.l_linenumber ) )  as dw_line_item_shk
                --
                -- note that last_modified_dt is not included in the hash diff since it only represents recency of the record versus an 
                -- actual meaningful change in the data
                --
                ,sha1_binary( concat( s.l_orderkey
                                     ,'|', coalesce( to_char( s.o_orderdate, 'yyyymmdd' ), '~' )
                                     ,'|', s.l_linenumber
                                     ,'|', coalesce( to_char( s.l_partkey ), '~' )
                                     ,'|', coalesce( to_char( s.l_suppkey ), '~' )
                                     ,'|', coalesce( to_char( s.l_quantity ), '~' )
                                     ,'|', coalesce( to_char( s.l_extendedprice ), '~' )
                                     ,'|', coalesce( to_char( s.l_discount ), '~' )
                                     ,'|', coalesce( to_char( s.l_tax ), '~' )
                                     ,'|', coalesce( to_char( s.l_returnflag ), '~' )
                                     ,'|', coalesce( to_char( s.l_linestatus ), '~' )
                                     ,'|', coalesce( to_char( s.l_shipdate, 'yyyymmdd' ), '~' )
                                     ,'|', coalesce( to_char( s.l_commitdate, 'yyyymmdd' ), '~' )
                                     ,'|', coalesce( to_char( s.l_receiptdate, 'yyyymmdd' ), '~' )
                                     ,'|', coalesce( s.l_shipinstruct, '~' )
                                     ,'|', coalesce( s.l_shipmode, '~' )
                                     ,'|', coalesce( s.l_comment, '~' )
                                    )
            
                            )               as dw_hash_diff
                ,s.*
            from
                line_item_stg s
            where
                    s.o_orderdate >= :l_start_dt
                and s.o_orderdate  < :l_end_dt
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
                l_stg s
            qualify
                row_number() over ( partition by dw_line_item_shk order by s.last_modified_dt desc ) = 1
        )
        ,l_tgt as
        (
            --
            -- Select the records in the logical partition from the current table. 
            -- Its own CTE, for partition pruning efficiencies
            select *
            from line_item
            where
                    o_orderdate >= :l_start_dt
                and o_orderdate  < :l_end_dt
        )
        select
             current_timestamp()        as dw_version_ts
            ,s.*
        from
            l_deduped s
            left join l_tgt t on
                t.dw_line_item_shk = s.dw_line_item_shk
        where
            -- source row does not exist in target table
            t.dw_line_item_shk is null
            -- or source row is more recent and differs from target table
            or (
                    t.last_modified_dt  < s.last_modified_dt
                and t.dw_hash_diff     != s.dw_hash_diff
               )
        order by
            s.o_orderdate  -- physically sort rows by logical partitioning date
    ) src
    on
    (
            tgt.dw_line_item_shk = src.dw_line_item_shk
        and tgt.o_orderdate     >= :l_start_dt
        and tgt.o_orderdate      < :l_end_dt
    )
    when matched then update set
         tgt.dw_hash_diff      = src.dw_hash_diff
        ,tgt.dw_version_ts     = src.dw_version_ts
        ,tgt.l_orderkey        = src.l_orderkey
        ,tgt.l_partkey         = src.l_partkey
        ,tgt.l_suppkey         = src.l_suppkey
        ,tgt.l_linenumber      = src.l_linenumber
        ,tgt.l_quantity        = src.l_quantity
        ,tgt.l_extendedprice   = src.l_extendedprice
        ,tgt.l_discount        = src.l_discount
        ,tgt.l_tax             = src.l_tax
        ,tgt.l_returnflag      = src.l_returnflag
        ,tgt.l_linestatus      = src.l_linestatus
        ,tgt.l_shipdate        = src.l_shipdate
        ,tgt.l_commitdate      = src.l_commitdate
        ,tgt.l_receiptdate     = src.l_receiptdate
        ,tgt.l_shipinstruct    = src.l_shipinstruct
        ,tgt.l_shipmode        = src.l_shipmode
        ,tgt.l_comment         = src.l_comment
        ,tgt.last_modified_dt  = src.last_modified_dt
        ,tgt.dw_file_name      = src.dw_file_name
        ,tgt.dw_file_row_no    = src.dw_file_row_no
        ,tgt.dw_update_ts      = src.dw_version_ts
    when not matched then insert
    (
         dw_line_item_shk
        ,dw_hash_diff
        ,dw_version_ts
        ,l_orderkey
        ,o_orderdate
        ,l_partkey
        ,l_suppkey
        ,l_linenumber
        ,l_quantity
        ,l_extendedprice
        ,l_discount
        ,l_tax
        ,l_returnflag
        ,l_linestatus
        ,l_shipdate
        ,l_commitdate
        ,l_receiptdate
        ,l_shipinstruct
        ,l_shipmode
        ,l_comment
        ,last_modified_dt
        ,dw_file_name
        ,dw_file_row_no
        ,dw_load_ts
        ,dw_update_ts
    )
    values
    (
         src.dw_line_item_shk
        ,src.dw_hash_diff
        ,src.dw_version_ts
        ,src.l_orderkey
        ,src.o_orderdate
        ,src.l_partkey
        ,src.l_suppkey
        ,src.l_linenumber
        ,src.l_quantity
        ,src.l_extendedprice
        ,src.l_discount
        ,src.l_tax
        ,src.l_returnflag
        ,src.l_linestatus
        ,src.l_shipdate
        ,src.l_commitdate
        ,src.l_receiptdate
        ,src.l_shipinstruct
        ,src.l_shipmode
        ,src.l_comment
        ,src.last_modified_dt
        ,src.dw_file_name
        ,src.dw_file_row_no
        ,src.dw_load_ts
        ,src.dw_version_ts
    )
    ;

  end for;
  
  return 'SUCCESS';

end;
$$
;

