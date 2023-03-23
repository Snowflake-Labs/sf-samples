--------------------------------------------------------------------
--  Purpose: data presentation
--      insert/overwrite pattern for date_dm
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
    
   insert overwrite into supplier_dm
   select
       p.dw_supplier_shk
      ,p.s_suppkey
      ,p.s_name
      ,p.s_address
      ,p.s_nationkey
      ,p.s_phone
      ,p.s_acctbal
      ,p.s_comment
      ,p.last_modified_dt
      ,p.dw_load_ts
      ,p.dw_update_ts
   from
       dev_webinar_orders_rl_db.tpch.supplier p;
  
  return 'SUCCESS';

end;
$$
;

