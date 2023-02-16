--------------------------------------------------------------------
--  Purpose: data presentation
--      date dimension table
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_pl_db};
use schema   &{l_pl_schema};

create table if not exists date_dm
(
     cal_dt                  date                 not null  primary key
    ,cal_ly_dt               date                 not null
    ,cal_date_label          varchar( 250 )       not null
    ,cal_weekend_fl          varchar( 250 )       not null
    ,cal_weekday_fl          varchar( 250 )       not null
    --
    -- year
    --
    ,iso_year_no             number               not null
    ,iso_year_dt             date                 not null
    ,iso_year_quarter_no     number               not null
    ,iso_year_quarter_dt     date                 not null     -- current year's quarter date
    ,iso_year_month_no       number               not null
    ,iso_year_month_dt       date                 not null     -- current year's month date
    ,iso_year_week_no        number               not null
    ,iso_year_week_dt        date                null     -- current year's week date
    ,iso_year_day_no         number               not null
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
    ,iso_quarter_dt          date                 not null
    ,iso_quarter_label       varchar( 250 )       not null
    ,iso_quarter_month_no    number               not null
    ,iso_quarter_week_no     number               not null
    ,iso_quarter_day_no      number               not null
    --
    ,cal_quarter_dt          date                 not null
    ,cal_quarter_label       varchar( 250 )       not null
    ,cal_quarter_month_no    number               not null
    ,cal_quarter_week_no     number               not null
    ,cal_quarter_day_no      number               not null
    --
    -- month
    --
    ,iso_month_dt            date                 not null       -- date to represent actual fiscal month (dd/01/yy) since it can cross calendar months.
    ,iso_month_label         varchar( 250 )       not null
    ,iso_month_week_no       number               not null
    ,iso_month_day_no        number               not null
    --
    ,cal_month_dt            date                 not null
    ,cal_month_label         varchar( 250 )       not null
    ,cal_month_week_no       number               not null
    ,cal_month_day_no        number               not null
    --
    -- week
    --
    ,iso_week_dt             date                 not null
    ,iso_week_label          varchar( 250 )       not null
    ,iso_week_day_no         number               not null
    --
    ,cal_week_day_no         number               not null
    --
    -- itd
    --
    ,iso_itd_quarter_no      number               not null
    ,iso_itd_month_no        number               not null
    ,iso_itd_week_no         number               not null
    ,iso_itd_day_no          number               not null
    --
    ,cal_itd_quarter_no      number               not null
    ,cal_itd_month_no        number               not null
    ,cal_itd_day_no          number               not null
    --
    -- ptd
    --
    ,iso_ptd_bt              number               not null
    ,iso_ptd_label           varchar( 250 )       not null
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
