--------------------------------------------------------------------
--  Purpose: data acquisition
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database webinar_il_db;
use schema   main;
use warehouse dev_webinar_wh;
--------------------------------------------------------------------
-- date range to maintain
--
set (l_start_dt, l_end_dt, l_day_cnt, l_null_dt, l_undefined_dt ) = 
(
    select
         to_date( '01/01/1998', 'mm/dd/yyyy' )                     as start_dt
        ,date_trunc( year, dateadd( year,  4, current_date() ) )   as end_dt
        ,datediff( day, start_dt, end_dt )                         as day_cnt
        -- defaults for nulls and undefined values
        ,to_date( '01/01/1950', 'mm/dd/yyyy' )                     as null_dt
        ,to_date( '01/01/1900', 'mm/dd/yyyy' )                     as undefined_dt
);

insert overwrite into date_cal_lkp
with l_date as
(
    -- generate broad date range to translate
    select
         cal_dt
        ,date_part( year, cal_dt )              as cal_year_no
        ,date_part( quarter, cal_dt )           as cal_year_quarter_no
        ,date_part( month, cal_dt )             as cal_year_month_no
        ,date_part( dayofyear, cal_dt )         as cal_year_day_no
        -- dates
        ,min( cal_dt ) over( partition by date_part( year, cal_dt ) )                                 as cal_year_dt
        ,min( cal_dt ) over( partition by date_part( year, cal_dt ), date_part( quarter, cal_dt ) )   as cal_quarter_dt
        ,min( cal_dt ) over( partition by date_part( year, cal_dt ), date_part( month, cal_dt ) )     as cal_month_dt
    from
        (
        select
            dateadd( day, seq4(), $l_start_dt )      as cal_dt
        from
            table( generator( rowcount => $l_day_cnt ) )
        -- union in null and undefined placeholders
        union all select $l_null_dt
        union all select $l_undefined_dt
        )
)
,l_cal_dt as
(
    --
    -- generate dates - full ISO years are generated.
    --
    select
         cal_dt
        ,dateadd( 'day',  -364, cal_dt )                as cal_ly_dt
        ,to_char( cal_dt, 'yyyy/mm/dd Dy' )             as cal_date_label
        ,case
            when date_part( 'dow', cal_dt ) in ( 0,6 ) then 'Y'
            else 'N'
         end                                            as cal_weekend_fl
        ,case
            when date_part( 'dow', cal_dt ) in ( 0,6 ) then 'N'
            else 'Y'
         end                                            as cal_weekday_fl
        ,cal_year_no
        ,cal_year_dt
        ,cal_year_quarter_no
        ,min( case when cal_year_no = date_part( year, current_date ) then cal_quarter_dt else to_date( null ) end ) over( partition by cal_year_quarter_no )    as cal_year_quarter_dt
        ,cal_year_month_no
        ,min( case when cal_year_no = date_part( year, current_date ) then cal_month_dt else to_date( null ) end ) over( partition by cal_year_month_no )       as cal_year_month_dt
        ,to_char( cal_year_no )
          || '/'
          || lpad( date_part( 'mm', cal_dt ), 2, '0' )
          || '/01 '
          || monthname( cal_dt )                        as cal_month_label
        ,cal_year_day_no
        ,cal_quarter_dt
        ,to_char( cal_quarter_dt, 'yyyy/mm/dd' )
                     || ' Q'
                     || cal_year_quarter_no             as cal_quarter_label
        ,dense_rank()
                over ( partition by
                        cal_year_no
                       ,cal_year_quarter_no
                    order by
                        cal_year_month_no    )          as cal_quarter_month_no
        ,dense_rank()
                over ( partition by
                           cal_year_no
                          ,cal_year_quarter_no
                       order by
                           cal_year_day_no      )       as cal_quarter_day_no
        ,cal_month_dt
        ,dense_rank()
             over ( partition by
                        cal_year_no
                       ,cal_year_month_no
                    order by
                        cal_year_day_no   )             as cal_month_day_no
        ,date_part( 'dow', cal_dt ) + 1                 as cal_week_day_no
        ,dense_rank()
              over ( order by
                          cal_year_no
                         ,cal_year_quarter_no )         as cal_itd_quarter_no
        ,dense_rank()
              over ( order by
                          cal_year_no
                         ,cal_year_month_no )           as cal_itd_month_no
        ,dense_rank()
              over ( order by
                          cal_dt )                      as cal_itd_day_no
    from
        l_date
)
select
    -- cal
     lcd.cal_dt
    ,lcd.cal_ly_dt
    ,lcd.cal_date_label
    ,lcd.cal_weekend_fl
    ,lcd.cal_weekday_fl
    --
    ,lcd.cal_year_no
    ,lcd.cal_year_dt
    ,lcd.cal_year_quarter_no
    ,lcd.cal_year_quarter_dt
    ,lcd.cal_year_month_no
    ,lcd.cal_year_month_dt
    ,lcd.cal_year_day_no
    --
    ,lcd.cal_quarter_dt
    ,lcd.cal_quarter_label
    ,lcd.cal_quarter_month_no
    ,dense_rank()
           over ( partition by
                      lcd.cal_year_no
                     ,lcd.cal_year_quarter_no
                  order by
                      lcd.cal_itd_day_no    )        as cal_quarter_week_no
    ,lcd.cal_quarter_day_no
    --
    ,lcd.cal_month_dt
    ,lcd.cal_month_label
    ,dense_rank()
           over ( partition by
                      lcd.cal_year_no
                     ,lcd.cal_year_month_no
                  order by
                      lcd.cal_itd_day_no    )       as cal_month_week_no
    ,lcd.cal_month_day_no
    --
    ,lcd.cal_week_day_no
    -- cal itd
    ,lcd.cal_itd_quarter_no
    ,lcd.cal_itd_month_no
    ,lcd.cal_itd_day_no
    --
    ,0                                                            as cal_ptd_bt
    ,'N'                                                          as cal_ptd_label
    -- future
    ,0                                                            as cal_future_date_bt
--    ,nvl( dhs.holiday_label, '~' )                               as holiday_name
    ,'~'                                                          as holiday_name
    ,to_timestamp_ltz( current_timestamp() )                      as dw_load_ts
    ,to_timestamp_ltz( current_timestamp() )                      as dw_update_ts
from
    l_cal_dt        lcd
    --
    -- create a holiday satellite within the derivation layer and then join into this load script
    -- to populate holiday names.
    --
    --    left join date_holiday_s dhs on
    --        dhs.holiday_dt = lcd.cal_dt
where
    lcd.cal_dt is not null
order by
    lcd.cal_dt
;
