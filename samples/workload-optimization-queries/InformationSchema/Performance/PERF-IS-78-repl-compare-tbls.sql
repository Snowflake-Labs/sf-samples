-------------------------------------------------
-- NAME:	 PERF-IS-78-repl-compare-tbls.txt
-------------------------------------------------
-- DESCRIPTION:
--	Base information for validating tables in a refresh, using hash_agg function

-- OUTPUT:
--	Result in Variant representing hash of table data
--
-- NEXT STEPS:
--	use information to compare to table hash after refresh
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------

Comparing Data Sets in Primary and Secondary Databases

Optionally use the HASH_AGG function to compare the rows in a random set of tables in a 
primary and secondary database to verify data consistency. The HASH_AGG function returns 
an aggregate signed 64-bit hash value over the (unordered) set of input rows. Query this 
function on all or a random subset of tables in a secondary database and on the primary 
database (as of the timestamp for the primary database snapshot) and compare the output.

Example

Executed on the Secondary Database

1.  On the secondary database, query the DATABASE_REFRESH_PROGRESS table function (in the 
Snowflake Information Schema). Note the snapshot_transaction_timestamp in the DETAILS 
column for the PRIMARY_UPLOADING_DATA phase. This is the timestamp for the latest snapshot 
of the primary database.

select parse_json(details)['snapshot_transaction_timestamp']
from table(information_schema.database_refresh_progress(mydb))
where phase_name = 'PRIMARY_UPLOADING_DATA';

2.  Query the HASH_AGG function for a specified table. The following query returns a hash 
value for all rows in the mytable table:

select hash_agg( * ) from mytable;

Executed on the Primary Database

3.  On the primary database, query the HASH_AGG function for the same table. Using Time 
Travel, specify the timestamp when the latest snapshot was taken for the secondary 
database:

select hash_agg( * ) from mytable at(timestamp => '<snapshot_transaction_timestamp>'::timestamp);

4.  Compare the results from the two queries. The output should be identical.