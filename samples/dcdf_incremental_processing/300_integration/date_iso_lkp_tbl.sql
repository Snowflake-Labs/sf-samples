--------------------------------------------------------------------
--  Purpose: data integration/derivation
--     persist ISO calendar date derivations for cal_dt
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use database &{l_il_db};
use schema   &{l_il_schema};

create table if not exists date_iso_lkp
(
     cal_dt                  date                 not null  primary key
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
    -- quarter
    --
    ,iso_quarter_dt          date                 not null
    ,iso_quarter_label       varchar( 250 )       not null
    ,iso_quarter_month_no    number               not null
    ,iso_quarter_week_no     number               not null
    ,iso_quarter_day_no      number               not null
    --
    -- month
    --
    ,iso_month_dt            date                 not null       -- date to represent actual fiscal month (dd/01/yy) since it can cross calendar months.
    ,iso_month_label         varchar( 250 )       not null
    ,iso_month_week_no       number               not null
    ,iso_month_day_no        number               not null
    --
    -- week
    --
    ,iso_week_dt             date                 not null
    ,iso_week_label          varchar( 250 )       not null
    ,iso_week_day_no         number               not null
    --
    -- itd
    --
    ,iso_itd_quarter_no      number               not null
    ,iso_itd_month_no        number               not null
    ,iso_itd_week_no         number               not null
    ,iso_itd_day_no          number               not null
    --
    -- ptd
    --
    ,iso_ptd_bt              number               not null
    ,iso_ptd_label           varchar( 250 )       not null
    --
    ,dw_load_ts              timestamp_ltz        not null
    ,dw_update_ts            timestamp_ltz        not null
)
data_retention_time_in_days = 1
;
