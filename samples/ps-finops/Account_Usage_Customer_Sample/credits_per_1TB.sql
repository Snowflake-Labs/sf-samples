with daily_bytes_scanned as (
	select to_date(date_trunc('month', end_time)) as "month"
		, sum(bytes_scanned)/1024/1024/1024/1024 scanned_tb
	from snowflake.account_usage.query_history
	where end_time >= date_trunc('month', dateadd('month', -12, current_date))
		and end_time < date_trunc('month', current_date)
	group by all
), daily_credits as (
	select to_date(date_trunc('month', end_time)) as "month"
		, sum(credits_attributed_compute) as credits
	from snowflake.account_usage.query_attribution_history
	where end_time >= date_trunc('month', dateadd('month', -12, current_date))
		and end_time < date_trunc('month', current_date)
	group by all
)
select t1.month
	, credits/scanned_tb as credits_per_scanned_tb
from daily_bytes_scanned t1
join daily_credits t2 on t1.month = t2.month
order by t1.month;
