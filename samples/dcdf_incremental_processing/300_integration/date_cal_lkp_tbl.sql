--------------------------------------------------------------------
--  Purpose: data integration/derivation
--       to persist the calendar date derivations for a cal_dt
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_il_db};
use schema   &{l_il_schema};

create table if not exists date_cal_lkp
(
     cal_dt                  date                 not null  primary key
    ,cal_ly_dt               date                 not null
    ,cal_date_label          varchar( 250 )       not null
    ,cal_weekend_fl          varchar( 250 )       not null
    ,cal_weekday_fl          varchar( 250 )       not null
    --
    -- year
    --
    ,cal_year_no             number               not null
    ,cal_year_dt             date                 not null
    ,cal_year_quarter_no     number               not null
    ,cal_year_quarter_dt     date                 not null     -- current year's quarter date
    ,cal_year_month_no       number               not null
    ,cal_year_month_dt       date                 not null     -- current year's month date
    ,cal_year_day_no         number               not null
    --
    -- quarter
    --
    ,cal_quarter_dt          date                 not null
    ,cal_quarter_label       varchar( 250 )       not null
    ,cal_quarter_month_no    number               not null
    ,cal_quarter_week_no     number               not null
    ,cal_quarter_day_no      number               not null
    --
    -- month
    --
    ,cal_month_dt            date                 not null
    ,cal_month_label         varchar( 250 )       not null
    ,cal_month_week_no       number               not null
    ,cal_month_day_no        number               not null
    --
    -- week
    --
    ,cal_week_day_no         number               not null
    --
    -- itd
    --
    ,cal_itd_quarter_no      number               not null
    ,cal_itd_month_no        number               not null
    ,cal_itd_day_no          number               not null
    --
    -- ptd
    --
    ,cal_ptd_bt              number               not null
    ,cal_ptd_label           varchar( 250 )       not null
    --
    -- Future
    --
    ,cal_future_date_bt      number               not null
    --
    -- season
    --
    ,holiday_name            varchar( 250 )       not null     -- based on specific date on calendar labor day, halloween, thanksgiving, christmas, etc.
    ,dw_load_ts              timestamp_ltz        not null
    ,dw_update_ts            timestamp_ltz        not null
)
data_retention_time_in_days = 1
;
