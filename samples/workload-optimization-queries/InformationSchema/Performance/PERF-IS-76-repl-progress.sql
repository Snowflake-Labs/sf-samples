-------------------------------------------------
-- NAME:	 PERF-IS-76-repl-progress.txt
-------------------------------------------------
-- DESCRIPTION:
--  DATABASE_REFRESH_PROGRESS returns a JSON object indicating the current refresh status 
--  for a secondary database by name
--
--  DATABASE_REFRESH_PROGRESS only returns the database refresh activity for the most 
--  recent database refresh if it occurred within the last 14 days
--
-- Arguments
-- secondary_db_name
--
-- OUTPUT:
--PHASE_NAME	Name of the replication phases completed (or in progress) so far. For the list of phases, see the usage notes.
--RESULT		Status of the replication phase. Valid statuses are EXECUTING, SUCCEEDED, CANCELLED, FAILED.
--START_TIME	Time when the replication phase began. Format is epoch time.
--END_TIME		Time when the phase finished, if applicable. Format is epoch time.
--DETAILS		Returned by the DATABASE_REFRESH_PROGRESS function only. A JSON object that provideds detailed information for the following phases: - Primary uploading data: The timestamp of the current snapshot of the primary database. - Primary uploading data and Secondary downloading data: Total number of bytes in the database refresh as well as the number of bytes copied so far in the phase. - Secondary downloading metadata: The number of tables, table columns, and all database objects (including tables and table columns) in the latest snapshot of the primary database.
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

-- Only returns results for account administrators (users with the ACCOUNTADMIN role).
-- When calling an Information Schema table function, the session must have an INFORMATION_SCHEMA 
-- schema in use or the function name must be fully-qualified. For more details, see Snowflake Information Schema.

-- Following is the list of phases in the order processed:
--SECONDARY_UPLOADING_INVENTORY
--PRIMARY_UPLOADING_METADATA
--PRIMARY_UPLOADING_DATA
--SECONDARY_DOWNLOADING_METADATA
--SECONDARY_DOWNLOADING_DATA
--COMPLETED / FAILED / CANCELED

SET DATABASE_NAME = '';

select * from table(information_schema.database_refresh_progress($DATABASE_NAME));
