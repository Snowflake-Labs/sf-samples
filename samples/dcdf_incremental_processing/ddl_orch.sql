
---------------------------------------------
-- snowsql session
--
!set variable_substitution=true
!set exit_on_error=true
!set echo=true

---------------------------------------------
-- 000_admin
!print 000_admin

!print set context
!source 000_admin/&{l_env}_context_ddl.sql

!print execute ddl

---------------------------------------------
-- Raw tables
!source 200_raw/line_item_tbl.sql
!source 200_raw/orders_tbl.sql
!source 200_raw/part_tbl.sql
!source 200_raw/supplier_tbl.sql
!source 200_raw/partsupp_tbl.sql
!source 200_raw/customer_tbl.sql

---------------------------------------------
-- Integration tables
!source 300_integration/date_cal_lkp_tbl.sql
!source 300_integration/date_iso_lkp_tbl.sql
!source 310_derivation/line_item_margin_tbl.sql
!source 310_derivation/part_first_order_dt_tbl.sql

---------------------------------------------
-- Integration functions
!source 000_admin/dw_delta_date_tbl.sql
!source 000_admin/dw_delta_date_range_f.sql

---------------------------------------------
-- Presentation tables
!source 400_dimension/date_dm_tbl.sql
!source 400_dimension/part_dm_tbl.sql
!source 400_dimension/customer_dm_tbl.sql
!source 400_dimension/supplier_dm_tbl.sql
!source 410_fact_atomic/order_line_fact_tbl.sql
!source 410_fact_atomic/order_line_fact_bonus_tbl.sql

!print SUCCESS!
!quit
