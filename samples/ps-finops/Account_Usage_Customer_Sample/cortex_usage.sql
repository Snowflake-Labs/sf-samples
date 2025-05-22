select to_date(date_trunc('week', start_time)) as "week"
	, function_name
	, sum(token_credits) as credits_used
from snowflake.account_usage.cortex_functions_usage_history
where start_time > date_trunc('week', dateadd('month', -6, current_date))
	and start_time < date_trunc('week', current_date)
group by "week", function_name
order by "week" asc;