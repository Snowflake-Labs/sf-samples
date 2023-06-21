-----------------------
-- Table access counts
-----------------------
select f1.value:objectDomain,
f1.value:objectId,
f1.value:objectName,
count(*) as AccessFrequency
from SNOWFLAKE.ACCOUNT_USAGE.access_history
     , lateral flatten(base_objects_accessed) f1
where true and
query_start_time between $TS_START AND $TS_END and
f1.value:objectDomain in ('Table', 'External table') -- Database, Stage, Schema
--query_id='01a34678-0603-e0e5-0000-43291affa452'
--f1.value:"objectId"::int=<fill_in_object_id> and
--f1.value:"objectDomain"::string='Table' and
--query_start_time >= dateadd('day', -30, current_timestamp())
group by 1,2,3
order by 1,2,3
limit 100
;


-----------------------
-- Column access counts
-----------------------
select f1.value:objectDomain,
f1.value:objectId,
f1.value:objectName,
c.value:columnId,
c.value:columnName,
count(*) as ColumnFrequency
from SNOWFLAKE.ACCOUNT_USAGE.access_history
     , lateral flatten(base_objects_accessed) f1
	 , lateral flatten(input => f1.value:"columns", outer => true) c
where true and
--query_start_time between $TS_START AND $TS_END AND
f1.value:objectDomain in ('Table', 'External table') -- Database, Stage, Schema
--query_id='01a34678-0603-e0e5-0000-43291affa452'
--f1.value:"objectId"::int=<fill_in_object_id> and
--f1.value:"objectDomain"::string='Table' and
--query_start_time >= dateadd('day', -30, current_timestamp())
group by 1,2,3,4,5
order by 1,2,3,4,5
limit 100
;