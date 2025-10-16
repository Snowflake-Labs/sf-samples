select to_date(date_trunc('month', event_timestamp)) as "month"
	, ifnull(u.type, 'user') as user_type
	, count(distinct(lh.user_name)) as cnt
from snowflake.account_usage.login_history lh
join snowflake.account_usage.users u on lh.user_name = u.name
where event_timestamp >= date_trunc('month', dateadd('month', -6, current_date))
	and event_timestamp < date_trunc('month', current_date)
group by "month", user_type;