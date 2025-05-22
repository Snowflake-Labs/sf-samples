select to_date(date_trunc('week', end_time)) as "week"
	, sum(1000*credits_attributed_compute)/count(query_id) as credits_per_1000_queries
	, count(query_id)
from snowflake.account_usage.query_attribution_history
where end_time >= date_trunc('week', dateadd('month', -12, current_date))
	and end_time < date_trunc('week', current_date)
group by all;