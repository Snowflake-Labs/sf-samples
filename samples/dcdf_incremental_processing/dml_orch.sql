------------------------------------------------------------------------------
-- snowsql session
--
!set variable_substitution=true
!set exit_on_error=true
!set echo=true

------------------------------------------------------------------------------
-- 000_admin
--
!print 000_admin

!print set context

!source 000_admin/&{l_env}_context_dml.sql

---------------------------------------------
-- Acquisition
!print acquisition
!source 100_acquisition/line_item_acq.sql
!source 100_acquisition/orders_acq.sql
!source 100_acquisition/part_acq.sql
!source 100_acquisition/supplier_acq.sql
!source 100_acquisition/partsupp_acq.sql
--
-- Raw tables load
!print raw stg
!source 200_raw/line_item_stg_ld.sql
!source 200_raw/orders_stg_ld.sql
!source 200_raw/part_stg_ld.sql
!source 200_raw/partsupp_stg_ld.sql
!source 200_raw/supplier_stg_ld.sql
--
-- Load delta date table
!print Delta date
!source 200_raw/dw_delta_date_ld.sql
--
-- Raw History tables
!print raw history
!source 200_raw/orders_hist_ld.sql
!source 200_raw/line_item_hist_ld.sql
--
-- Raw Current state tables
!print raw merge
!source 200_raw/line_item_ld.sql
!source 200_raw/orders_ld.sql
!source 200_raw/part_ld.sql
!source 200_raw/partsupp_ld.sql
!source 200_raw/supplier_ld.sql
--
-- Integration tables
!print integration
!source 300_integration/date_cal_lkp_ld.sql
!source 300_integration/date_iso_lkp_ld.sql
!source 310_derivation/line_item_margin_ld.sql
!source 310_derivation/part_first_order_dt_ld.sql

-- Presentation tables
!print presentation
!source 400_dimension/date_dm_ld.sql
!source 400_dimension/part_dm_ld.sql
!source 400_dimension/supplier_dm_ld.sql
!source 410_fact_atomic/order_line_fact_ld.sql

!print SUCCESS!
!quit
