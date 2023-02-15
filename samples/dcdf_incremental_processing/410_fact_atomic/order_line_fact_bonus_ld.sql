--------------------------------------------------------------------
--  Purpose: delete/insert pattern
--      This shows an insert-only pattern 
--
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role      sysadmin;
use database  dev_webinar_pl_db;
use schema    main;
use warehouse dev_webinar_wh;

execute immediate $$

declare
  l_start_dt       date;
  l_end_dt         date;
  -- Grab the dates for the logical partitions to process
  c1 cursor for select start_dt, end_dt FROM table(dev_webinar_common_db.util.dw_delta_date_range_f('week')) order by 1;
  
begin
    
  --
  -- Loop through the dates to incrementally process based on the logical partition definition.
  -- In this example, the logical partitions are by week.
  --
  for record in c1 do
    l_start_dt       := record.start_dt;
    l_end_dt         := record.end_dt;

    --
    -- delete from the current table the logical partition to be processed.
    -- then insert the updated records.
    -- very similar to the Oracle method of inserting records into a table, and swapping that table into the Oracle partitioned table once proceessed
    --
    -- Start a transaction such that delete and insert are all committed at once.
    -- Decouple the for loop and begin transaction, fork processes to run concurrently - PUT PRESENTATION
       
     -- Delete the records using the logical partition 
     -- Very efficient when all the rows are in the same micropartitions.  Mirrors a truncate table in other database platforms.
     delete from order_line_fact_bonus
     where orderdate >= :l_start_dt
       and orderdate <  :l_end_dt;
 
     -- Insert the logical partitioned records into the table
     -- Inserts data from same order date into the same micropartitions
     -- Enables efficient querying of the data for consumption
     insert into order_line_fact_bonus
     with l_cust as
     (
        select
            dw_customer_shk
           ,c_custkey
           ,change_date as active_date
           ,nvl( lead( change_date) over (partition by c_custkey order by change_date), to_date('1/1/2888','mm/dd/yyyy')) as inactive_date
        from
             dev_webinar_orders_rl_db.tpch.customer_hist
     )
     select
         li.dw_line_item_shk
        ,o.o_orderdate
        ,o.dw_order_shk
        ,p.dw_part_shk
        ,s.dw_supplier_shk
        ,c.dw_customer_shk
        ,li.l_quantity      as quantity
        ,li.l_extendedprice as extendedprice
        ,li.l_discount      as discount
        ,li.l_tax           as tax
        ,li.l_returnflag    as returnflag
        ,li.l_linestatus    as linestatus
        ,li.l_shipdate
        ,li.l_commitdate
        ,li.l_receiptdate
        ,lim.margin_amt
        ,current_timestamp() as dw_load_ts
     from
         dev_webinar_orders_rl_db.tpch.line_item li
         --
         join dev_webinar_orders_rl_db.tpch.orders o
           on o.o_orderkey = li.l_orderkey
         --
         join dev_webinar_il_db.main.line_item_margin lim
           on lim.dw_line_item_shk = li.dw_line_item_shk
         --
         -- Left outer join in case the part record is late arriving
         --
         left outer join dev_webinar_orders_rl_db.tpch.part p
           on p.p_partkey = li.l_partkey
         --
         -- left outer join in case the supplier record is late arriving
         --
         left outer join dev_webinar_orders_rl_db.tpch.supplier s
           on s.s_suppkey = li.l_suppkey
         -- 
         left outer join l_cust c
           on   o.o_custkey    = c.c_custkey 
            and o.o_orderdate >= c.active_date
            and o.o_orderdate  < c.inactive_date
     where 
             li.o_orderdate >= :l_start_dt
         and li.o_orderdate <  :l_end_dt
     order by o.o_orderdate;

  end for;
  
  return 'SUCCESS';

end;
$$
;


select 
     c.c_custkey
    ,c.c_name
    ,c.c_acctbal
    ,olf.*
from dev_webinar_pl_db.main.customer_dm c
   join dev_webinar_pl_db.main.order_line_fact_bonus olf
     on olf.dw_customer_shk = c.dw_customer_shk
where c.c_custkey in (50459048);
order by olf.orderdate;