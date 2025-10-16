    select database_name
, date_trunc('month', usage_date) as month_year
	, avg(average_database_bytes) / (1024 * 1024 * 1024 * 1024) as storage_tb
from snowflake.account_usage.database_storage_usage_history
where usage_date >= date_trunc('month', dateadd(month, -6, current_date))
	and usage_date < date_trunc('month', current_date)
	and deleted is null
	and database_name in (
		  'customercommunication'
		, 'kafkaevents'
		, 'merchandising_sandbox'
		, 'predictivemodeling_raw'
		, 'predictivemodeling_sandbox'
	)
group by database_name, month_year
order by database_name, month_year;