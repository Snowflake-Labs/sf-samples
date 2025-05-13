/**********************************
FinOps Account Setup for Snowflake.
Includes creating task for daily hybrid table usage snapshot.

Requires SNOW CLI with snowflake.yml file for variables.

Roles used:
Delegated Admin which can be defined as SYSADMIN 

**********************************/
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;

-- Create a task to insert the data daily:
CREATE OR REPLACE TASK HYBRID_STORAGE_DETAILED_POP
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'SMALL'
SCHEDULE = 'USING CRON 0 23 * * * America/Los_Angeles' -- End of day, adjust as you see fit.
AS

INSERT INTO hybrid_storage_detailed (HYBRID_TABLE_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_COPY_COUNT, CREATED, LAST_ALTERED, DELETED, TABLE_RENTENTION_DAYS, TABLE_LIFESPAN_DAYS, ROW_COUNT, BYTES, DATE_COLLECTED
)
SELECT 
      ht.id as hybrid_table_id
	, ht.database_name
    , ht.schema_name
    , ht.name
    , count(ht.name) over (partition by ht.database_name, ht.schema_name, ht.name) as table_copy_count
	, ht.created
    , ht.last_altered
	, ht.deleted
	, ht.retention_time as table_rentention_days
    , datediff('day',ht.created, coalesce(ht.deleted, current_timestamp())) as table_lifespan_days
    --Storage space
	, ht.row_count
    , ht.bytes
    , CURRENT_TIMESTAMP() AS DATE_COLLECTED
FROM SNOWFLAKE.ACCOUNT_USAGE.HYBRID_TABLES ht
WHERE bytes > 0 
ORDER BY 
  ht.database_name
, ht.schema_name
, ht.name
;

