select warehouse_name
	, date_trunc('month', to_date(start_time)) as month_year
	, to_char(start_time, 'mm/yy') as pretty_date
	, sum(credits_used) as credits_used_last_month
from snowflake.account_usage.warehouse_metering_history
where start_time >= date_trunc('month', dateadd(month, -6, current_date))
	and start_time < date_trunc('month', current_date)
	and warehouse_name in (
          'customercommunication_analytics'
        , 'customercare_analytics'
        , 'predictivemodeling_analytics'
        , 'merchandising_analytics'
        , 'etl_realtime_metrics_xsmall'
    )
group by month_year, pretty_date, warehouse_name
order by month_year;