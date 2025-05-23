-- Note: Using organization_usage schema for ORG 2.0 Views
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_QUERYCOUNTS_CC_DAY COMMENT = 'Title: Query Counts by Cost Center and Type. Description: Analyze query counts by various types and cost center.'
AS
SELECT
   DATE_TRUNC('DAY', START_TIME) AS DAY
   , h.ACCOUNT_NAME
   , GET_TAG_COST_CENTER(H.QUERY_TAG, H.USER_NAME, TU.TAG_VALUE, H.ROLE_NAME, TR.TAG_VALUE, TS.TAG_VALUE, TW.TAG_VALUE) AS COST_CENTER
   , H.QUERY_TYPE
   , SUM(CASE
           WHEN CLUSTER_NUMBER IS NULL
               THEN 1
           ELSE 0
           END) AS NON_WH_QUERY_COUNT
   , SUM(CASE
           WHEN CLUSTER_NUMBER > 0
               THEN 1
           ELSE 0
           END) AS WH_QUERY_COUNT
   , SUM(CASE
           WHEN EXECUTION_STATUS = 'SUCCESS'
               THEN 0
           ELSE 1
           END) AS FAILURE_COUNT
   , SUM(CASE
           WHEN USER_NAME = 'SYSTEM'
               THEN 1
           ELSE 0
           END) AS SNOWFLAKE_WH_TASK
FROM SNOWFLAKE.ORGANIZATION_USAGE.QUERY_HISTORY H
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TR
  ON TR.OBJECT_NAME = H.ROLE_NAME
    AND TR.ACCOUNT_LOCATOR = H.ACCOUNT_LOCATOR
    AND TR.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TR.DOMAIN = 'ROLE'
    AND H.USER_NAME <> 'SYSTEM'
--Schema Snowflake Tasks
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TS
  ON TS.OBJECT_DATABASE = H.DATABASE_NAME
    AND TS.ACCOUNT_LOCATOR = H.ACCOUNT_LOCATOR
    AND TS.OBJECT_NAME = H.SCHEMA_NAME
    AND TS.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TS.DOMAIN = 'SCHEMA'
    AND H.USER_NAME = 'SYSTEM'
--User overrides
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TU
  ON TU.OBJECT_NAME = H.USER_NAME
    AND TU.ACCOUNT_LOCATOR = H.ACCOUNT_LOCATOR
    AND TU.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TU.DOMAIN = 'USER'
--Warehouse fallback if nothing else tagged.
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TW
  ON TW.OBJECT_NAME = H.WAREHOUSE_NAME
    AND TW.ACCOUNT_LOCATOR = H.ACCOUNT_LOCATOR
    AND TW.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TW.DOMAIN = 'WAREHOUSE'
    AND ROLE_NAME <> 'WORKSHEETS_APP_RL' --SNOWSIGHT UI ROLE.
GROUP BY ALL;
