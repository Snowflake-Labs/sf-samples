with user_credits as (
    select to_date(date_trunc('month', start_time)) as "month"
        , count(distinct(user_name)) as unique_users
        , sum(credits_attributed_compute) as credits
    from snowflake.account_usage.query_attribution_history h
        join snowflake.account_usage.users u on h.user_name = u.name
    where start_time >= date_trunc('month', dateadd(month, -6, current_date))
        and start_time < date_trunc('month', current_date)
        and type is null
    group by "month"
)
select "month", credits / unique_users as credits_per_user
from user_credits  
order by "month";