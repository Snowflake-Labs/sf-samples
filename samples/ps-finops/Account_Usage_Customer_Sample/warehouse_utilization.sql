select to_date(date_trunc('week', end_time)) as "week"
	, warehouse_name
	, avg(utilization) as avg_utilization
from snowflake.account_usage.warehouse_utilization
where end_time >= date_trunc('month', dateadd('month', -6, current_date))
	and end_time < date_trunc('month', current_date)
	and warehouse_name in (
		  'customercommunication_analytics_medium'
		, 'merchandising_analytics_medium'
		, 'predictivemodeling_analytics_medium'
		, 'customercare_analytics_medium'
		, 'retailscience_analytics_medium'
	)
group by "week", warehouse_name
order by warehouse_name, "week";