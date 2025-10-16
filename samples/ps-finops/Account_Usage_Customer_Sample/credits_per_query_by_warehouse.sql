select to_date(date_trunc('month', start_time)) as "month"
    , warehouse_name
    , sum(credits_attributed_compute) as credits 
    , count(*) as query_count
    , credits / query_count as credits_per_query
from snowflake.account_usage.query_attribution_history
where start_time >= date_trunc('month', dateadd('month', -6, start_time))
    and start_time> date_trunc('month', start_time)
    and warehouse_name in (
          'customercommunication_analytics'
        , 'customercare_analytics'
        , 'predictivemodeling_analytics'
        , 'merchandising_analytics'
        , 'etl_realtime_metrics_xsmall'
    )
group by all;
