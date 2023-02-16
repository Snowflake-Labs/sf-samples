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
use database dev_webinar_il_db;
use schema   main;
use warehouse dev_webinar_wh;

/* Validation Queries
truncate table line_item;

select * from line_item limit 1000;

all 3 counts should be identical

select count(*), count( distinct l_orderkey ), min( o_orderdate ), max( o_orderdate ) from line_item_stg;

select count(*), count( distinct dw_line_item_shk ), count( distinct dw_hash_diff ) from line_item;
*/

-- What is the timeframe to be processed
select min( o_orderdate ), max( o_orderdate ), datediff( day, min(o_orderdate), max(o_orderdate)) from dev_webinar_orders_rl_db.tpch.line_item_stg;

-- Anonymous block
execute immediate $$

declare
  l_start_dt date;
  l_end_dt   date;
  -- Grab the dates for the logical partitions to process
  c1 cursor for select start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f('week')) order by 1;
  
begin
    
  for record in c1 do
    l_start_dt := record.start_dt;
    l_end_dt   := record.end_dt;

    --
    -- Merge Pattern
    --
    merge into part_first_order_dt t using
    (
        with l_stg as
        (
            -- 
            -- Driving CTE to identify all the records in the logical partition to be process
            --
            select
                 p.dw_part_shk
                ,min( s.o_orderdate ) as first_orderdate
                ,current_timestamp() as last_modified_dt
            from
                dev_webinar_orders_rl_db.tpch.line_item s
                join dev_webinar_orders_rl_db.tpch.part p
                  on p.p_partkey = s.l_partkey
            where
                    s.o_orderdate >= :l_start_dt
                and s.o_orderdate  < :l_end_dt
            group by
                1
        )
        select
             current_timestamp()        as dw_update_ts
            ,s.*
        from
            l_stg s
            left join part_first_order_dt t on
                t.dw_part_shk = s.dw_part_shk
        where
            t.first_orderdate       != s.first_orderdate
    ) s
    on
    (
        t.dw_part_shk = s.dw_part_shk
    )
    when matched then update set
         t.first_orderdate    = s.first_orderdate
        ,t.dw_update_ts  = s.dw_update_ts
    when not matched then insert
    (
         dw_part_shk
        ,first_orderdate
        ,dw_load_ts
        ,dw_update_ts
    )
    values
    (
         s.dw_part_shk
        ,s.first_orderdate
        ,s.dw_update_ts
        ,s.dw_update_ts
    )
    ;

  end for;
  
  return 'SUCCESS';

end;
$$
;

