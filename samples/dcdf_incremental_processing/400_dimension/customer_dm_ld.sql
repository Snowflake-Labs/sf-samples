--------------------------------------------------------------------
--  Purpose: data presentation
--      insert/overwrite pattern for part_dm
--
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database dev_webinar_pl_db;
use schema   main;
use warehouse dev_webinar_wh;

execute immediate $$

begin
    
   insert overwrite into customer_dm
   select
       p.dw_customer_shk
      ,p.c_custkey
      ,p.c_name
      ,p.c_address
      ,p.c_nationkey
      ,p.c_phone
      ,p.c_acctbal
      ,p.c_mktsegment
      ,p.c_comment as comment
      ,p.dw_load_ts
      ,p.dw_load_ts as dw_update_ts
   from
       dev_webinar_orders_rl_db.tpch.customer_hist p
   ;
  
  return 'SUCCESS';

end;
$$
;


select *
from dev_webinar_pl_db.main.customer_dm p
where c_custkey in (50459048);