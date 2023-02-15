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

-- Anonymous block
execute immediate $$

declare
  l_start_dt date;
  l_end_dt   date;
  -- Grab the dates for the logical partitions to process
  c1 cursor for select start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f('week')) order by 1;
  
begin
    
  --
  -- Loop through the dates to incrementally process based on the logical partitions.
  -- In this example, the logical partitions are by week
  --
  for record in c1 do
    l_start_dt := record.start_dt;
    l_end_dt   := record.end_dt;

    --
    -- Merge Pattern
    --
    merge into line_item_margin t using
    (
        with l_src as
        (
            -- 
            -- Driving CTE to identify all the records in the logical partition to be process
            --
            select
                 s.dw_line_item_shk
                ,s.o_orderdate
                ,s.l_extendedprice - (s.l_quantity * p.ps_supplycost ) as margin_amt
                ,s.last_modified_dt
            from
                dev_webinar_orders_rl_db.tpch.line_item s
                join dev_webinar_orders_rl_db.tpch.partsupp p
                  on ( p.ps_partkey = s.l_partkey
                       and p.ps_suppkey = s.l_suppkey )
            where
                    s.o_orderdate >= :l_start_dt
                and s.o_orderdate  < :l_end_dt
        )
        ,l_tgt as
        (
            -- 
            -- Select the records in the logical partition from the current table.
            -- Its own CTE, for partition pruning efficiencies
            select *
            from line_item_margin
            where
                    o_orderdate >= :l_start_dt
                and o_orderdate  < :l_end_dt
        )
        select
             current_timestamp()        as dw_update_ts
            ,s.*
        from
            l_src s
            left join l_tgt t on
                t.dw_line_item_shk = s.dw_line_item_shk
        where
            -- source row does not exist in target table
            t.dw_line_item_shk is null
            -- or source row is more recent and differs from target table
            or (
                    t.last_modified_dt  < s.last_modified_dt
                and t.margin_amt       != s.margin_amt
               )
        order by
            s.o_orderdate
    ) s
    on
    (
        t.dw_line_item_shk = s.dw_line_item_shk
        and t.o_orderdate >= :l_start_dt 
        and t.o_orderdate  < :l_end_dt
    )
    when matched then update set
         t.margin_amt    = s.margin_amt
        ,t.dw_update_ts  = s.dw_update_ts
    when not matched then insert
    (
         dw_line_item_shk
        ,o_orderdate
        ,margin_amt
        ,last_modified_dt
        ,dw_load_ts
        ,dw_update_ts
    )
    values
    (
         s.dw_line_item_shk
        ,s.o_orderdate
        ,s.margin_amt
        ,s.last_modified_dt
        ,s.dw_update_ts
        ,s.dw_update_ts
    )
    ;

  end for;
  
  return 'SUCCESS';

end;
$$
;

select m.*
from dev_webinar_il_db.main.line_item_margin m
    join dev_webinar_orders_rl_db.tpch.line_item l
where l.l_orderkey = 5722076550 
and l.l_partkey in ( 105237594, 128236374)
and m.dw_line_item_shk = l.dw_line_item_shk;