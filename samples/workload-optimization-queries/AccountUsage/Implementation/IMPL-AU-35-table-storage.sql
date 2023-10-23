-------------------------------------------------
-- NAME:	 IMPL-AU-35-table-storage.txt
-------------------------------------------------
-- DESCRIPTION:
--	storage metrics for tables
--
-- OUTPUT:
--	database, schema, table name,
--	active, failsafe, and time travel bytes
--
-- NEXT STEPS:
--	use information to determine table sizing
--
-- OPTIONS:
--	(1) roll up metrics to database level for capacity reporting
--	(2) roll up metrics to schema level for capacity reporting
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------

select
	table_catalog as database_name,
	table_schema as schema_name,
	table_name,
	sum(active_bytes) as active_bytes,
	sum(failsafe_bytes) as failsafe_bytes,
	sum(time_travel_bytes) as time_travel_bytes
from
	TABLE($TABLE_STORAGE_METRICS)
WHERE
	TABLE_CATALOG = $DATABASE_NAME AND
	TABLE_SCHEMMA = $SCHEMA_NAME
group by 1,2,3
order by 1,2,3
limit 100;