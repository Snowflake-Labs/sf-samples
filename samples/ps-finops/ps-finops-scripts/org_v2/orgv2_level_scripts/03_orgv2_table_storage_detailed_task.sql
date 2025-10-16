/**********************************
FinOps Org V2 Setup for Snowflake.
Includes creating task for daily storage usage snapshot.

Requires SNOW CLI with snowflake.yml file for variables.

Roles used:
Delegated Admin which can be defined as SYSADMIN 

**********************************/
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
USE WAREHOUSE <% ctx.env.admin_wh %>;

-- Create a task to insert the data daily:
CREATE OR REPLACE TASK TABLE_STORAGE_DETAILED_ORG_POP
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'SMALL'
schedule = 'USING CRON 0 23 * * * America/Los_Angeles' -- End of day, adjust as you see fit.
AS
INSERT INTO
    TABLE_STORAGE_DETAILED_ORG (
        ACCOUNT_LOCATOR,
        ACCOUNT_NAME,
        TABLE_ID,
        CLONE_GROUP_ID,
        IS_CLONE,
        TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME,
        TABLE_COPY_COUNT,
        TABLE_TYPE,
        IS_TRANSIENT,
        TABLE_CREATED,
        LAST_ALTERED,
        LAST_DDL,
        LAST_DDL_BY,
        AUTO_CLUSTERING_ON,
        DB_DROPPED,
        DB_RETENTION_DAYS_DEFAULT,
        SCHEMA_DROPPED,
        SCHEMA_RETENTION_DAYS_DEFAULT,
        TABLE_DROPPED,
        TABLE_RENTENTION_DAYS,
        TABLE_ENTERED_FAILSAFE,
        TABLE_DROP_OFF_DATE,
        TABLE_ACTIVE_LIFESPAN_DAYS,
        TABLE_PASSIVE_LIFESPAN_DAYS,
        ROW_COUNT,
        ACTIVE_BYTES,
        TIME_TRAVEL_BYTES,
        FAILSAFE_BYTES,
        RETAINED_FOR_CLONE_BYTES,
        TOTAL_BYTES,
        DATE_COLLECTED
    )
WITH ALL_METRICS AS (
        SELECT
            *
        FROM
            SNOWFLAKE.ORGANIZATION_USAGE.TABLE_STORAGE_METRICS
        WHERE
            TRUE
            AND TABLE_CREATED < DATEADD('HOUR', -4, CURRENT_TIMESTAMP()) --DELAY IN TABLE DATA AND IN FLIGHT OPERATIONS.
            AND TABLE_NAME NOT LIKE 'SEARCH OPTIMIZATION%' --Need to address separately
            --If no space used, let's remove:
            AND ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES + RETAINED_FOR_CLONE_BYTES > 0
    ),
    DB AS (
        --Default value for schema's and tables that haven't overridden the time_travel value.
        SELECT
            ACCOUNT_LOCATOR,
            ACCOUNT_NAME,
            DATABASE_ID,
            IS_TRANSIENT,
            RETENTION_TIME AS DB_RETENTION_DAYS_DEFAULT
        FROM
            SNOWFLAKE.ORGANIZATION_USAGE.DATABASES
        WHERE
            DELETED IS NULL --Dropped DB's table metadata should be accounted for in Tables and TSM views.
    ),
    SCH AS (
        SELECT
            ACCOUNT_LOCATOR,
            ACCOUNT_NAME,
            SCHEMA_ID,
            CATALOG_ID,
            IS_TRANSIENT,
            RETENTION_TIME AS SCHEMA_RETENTION_DAYS_DEFAULT
        FROM
            SNOWFLAKE.ORGANIZATION_USAGE.SCHEMATA
        WHERE
            DELETED IS NULL --Dropped Schema's table metadata should be accounted for in Tables and TSM views.
    )
    SELECT
    TSM.ACCOUNT_LOCATOR,
    TSM.ACCOUNT_NAME,
    TSM.ID AS TABLE_ID,
    CLONE_GROUP_ID,
    IFF(ID <> CLONE_GROUP_ID, 1, 0)::BOOLEAN AS IS_CLONE,
    TSM.TABLE_CATALOG,
    TSM.TABLE_SCHEMA,
    TSM.TABLE_NAME,
    COUNT(TSM.TABLE_NAME) OVER (
        PARTITION BY TSM.TABLE_CATALOG,
        TSM.TABLE_SCHEMA,
        TSM.TABLE_NAME
    ) AS TABLE_COPY_COUNT, --Tables with same name still taking up storage.
    T.TABLE_TYPE,
    TSM.IS_TRANSIENT, -- , case when datediff('hour',table_dropped, table_entered_failsafe) < 4 then 1 else 0 end::boolean as table_is_temporary --May not be fully accurate.
    TSM.TABLE_CREATED,
    T.LAST_ALTERED,
    T.LAST_DDL,
    T.LAST_DDL_BY,
    T.AUTO_CLUSTERING_ON,
    TSM.CATALOG_DROPPED AS DB_DROPPED,
    DB.DB_RETENTION_DAYS_DEFAULT,
    TSM.SCHEMA_DROPPED,
    SCH.SCHEMA_RETENTION_DAYS_DEFAULT,
    TSM.TABLE_DROPPED, --tsm.deleted as is_table_deleted --coalese of table, schema, and db drop dates. May have failsafe or time travel bytes.
    T.RETENTION_TIME AS TABLE_RENTENTION_DAYS, --Always populated.
    TSM.TABLE_ENTERED_FAILSAFE, --Even transient/temp tables will get a date.
    --For non-cloned tables, find date when table should go to 0 bytes consumed (estimate only as cloning may currently not be taking up space):
    CASE
        WHEN TABLE_ENTERED_FAILSAFE IS NOT NULL
        AND RETAINED_FOR_CLONE_BYTES = 0 THEN DATEADD(
            'DAY',
            IFF(TSM.IS_TRANSIENT = 'NO', 7, 0),
            TABLE_ENTERED_FAILSAFE
        )
        WHEN TABLE_DROPPED IS NOT NULL
        AND TABLE_ENTERED_FAILSAFE IS NULL
        AND RETAINED_FOR_CLONE_BYTES = 0 THEN DATEADD(
            'DAY',(
                IFF(TSM.IS_TRANSIENT = 'NO', 7, 0) + T.RETENTION_TIME
            ),
            TABLE_DROPPED
        )
        ELSE NULL
    END AS TABLE_DROP_OFF_DATE,
    DATEDIFF(
        'DAY',
        TSM.TABLE_CREATED,
        COALESCE(TSM.TABLE_DROPPED, CURRENT_TIMESTAMP())
    ) AS TABLE_ACTIVE_LIFESPAN_DAYS,
    DATEDIFF('DAY', TSM.TABLE_CREATED, CURRENT_TIMESTAMP()) AS TABLE_PASSIVE_LIFESPAN_DAYS, --Storage space
    T.ROW_COUNT,
    TSM.ACTIVE_BYTES,
    TSM.TIME_TRAVEL_BYTES,
    TSM.FAILSAFE_BYTES,
    TSM.RETAINED_FOR_CLONE_BYTES,
    TSM.ACTIVE_BYTES + TSM.TIME_TRAVEL_BYTES + TSM.FAILSAFE_BYTES + TSM.RETAINED_FOR_CLONE_BYTES AS TOTAL_BYTES, --, t.bytes as table_logical_bytes --does not include Time Travel and Fail-safe usage. Just a nice comparison data point.
    --, t.comment -- Add if useful.
    CURRENT_TIMESTAMP() AS DATE_COLLECTED
FROM
    ALL_METRICS TSM
    JOIN SNOWFLAKE.ORGANIZATION_USAGE.TABLES T
        ON TSM.ID = T.TABLE_ID
            AND TSM.ACCOUNT_LOCATOR = T.ACCOUNT_LOCATOR
            AND T.TABLE_CATALOG_ID = TSM.TABLE_CATALOG_ID
            AND T.TABLE_SCHEMA_ID = TSM.TABLE_SCHEMA_ID
    LEFT JOIN SCH
        ON SCH.CATALOG_ID = TSM.TABLE_CATALOG_ID
            AND TSM.ACCOUNT_LOCATOR = SCH.ACCOUNT_LOCATOR
            AND SCH.SCHEMA_ID = TSM.TABLE_SCHEMA_ID
    LEFT JOIN DB
        ON DB.DATABASE_ID = TSM.TABLE_CATALOG_ID
            AND DB.ACCOUNT_LOCATOR = TSM.ACCOUNT_LOCATOR
ORDER BY
    TSM.ACCOUNT_LOCATOR,
    TSM.TABLE_CATALOG,
    TSM.TABLE_SCHEMA,
    TSM.TABLE_NAME
;


--ENABLE THE TASK:
ALTER TASK TABLE_STORAGE_DETAILED_ORG_POP RESUME;

-- If delegated admin was given account level execute task privilege, use the following command to execute the task or just rely on the schedule to kick it off:
-- EXECUTE TASK TABLE_STORAGE_DETAILED_ORG_POP;


