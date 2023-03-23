--------------------------------------------------------------------
--  Purpose: data presentation
--      insert/overwrite pattern for date_dm
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

insert overwrite into date_dm
select
    -- cal
     lcd.cal_dt
    ,lcd.cal_ly_dt
    ,lcd.cal_date_label
    ,lcd.cal_weekend_fl
    ,lcd.cal_weekday_fl
    -- iso
    ,lid.iso_year_no
    ,lid.iso_year_dt
    ,lid.iso_year_quarter_no
    ,lid.iso_year_quarter_dt
    ,lid.iso_year_month_no
    ,lid.iso_year_month_dt
    ,lid.iso_year_week_no
    ,lid.iso_year_week_dt
    ,lid.iso_year_day_no
    --
    ,lcd.cal_year_no
    ,lcd.cal_year_dt
    ,lcd.cal_year_quarter_no
    ,lcd.cal_year_quarter_dt
    ,lcd.cal_year_month_no
    ,lcd.cal_year_month_dt
    ,lcd.cal_year_day_no
    --
    ,lid.iso_quarter_dt
    ,lid.iso_quarter_label
    ,lid.iso_quarter_month_no
    ,lid.iso_quarter_week_no
    ,lid.iso_quarter_day_no
    --
    ,lcd.cal_quarter_dt
    ,lcd.cal_quarter_label
    ,lcd.cal_quarter_month_no
    ,lcd.cal_quarter_week_no
    ,lcd.cal_quarter_day_no
    --
    ,lid.iso_month_dt
    ,lid.iso_month_label
    ,lid.iso_month_week_no
    ,lid.iso_month_day_no
    --
    ,lcd.cal_month_dt
    ,lcd.cal_month_label
    ,lcd.cal_month_week_no
    ,lcd.cal_month_day_no
    --
    ,lid.iso_week_dt
    ,lid.iso_week_label
    ,lid.iso_week_day_no
    ,lcd.cal_week_day_no
    -- iso itd
    ,lid.iso_itd_quarter_no
    ,lid.iso_itd_month_no
    ,lid.iso_itd_week_no
    ,lid.iso_itd_day_no
    -- cal itd
    ,lcd.cal_itd_quarter_no
    ,lcd.cal_itd_month_no
    ,lcd.cal_itd_day_no
     --
    ,lid.iso_ptd_bt
    ,lid.iso_ptd_label
    ,lcd.cal_ptd_bt
    ,lcd.cal_ptd_label
    -- future
    ,lcd.cal_future_date_bt
    ,lcd.holiday_name
    ,to_timestamp_ltz( current_timestamp() )                      as dw_load_ts
    ,to_timestamp_ltz( current_timestamp() )                      as dw_update_ts
from
    dev_webinar_il_db.main.date_cal_lkp        lcd
    join dev_webinar_il_db.main.date_iso_lkp   lid on
        lid.cal_dt = lcd.cal_dt
where
    lcd.cal_dt is not null
order by
    lcd.cal_dt
;
