USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW QUERYCOUNTS_CC_DAY COMMENT = 'Title: Query Counts by Cost Center and Type per day. Description: Analyze query counts by various types and cost center per day.'
as
SELECT
   DATE_TRUNC('DAY', START_TIME) AS DAY, 
   QUERY_TYPE,
   GET_TAG_COST_CENTER(H.QUERY_TAG, H.USER_NAME, TU.TAG_VALUE, H.ROLE_NAME, TR.TAG_VALUE, TS.TAG_VALUE, TW.TAG_VALUE) AS COST_CENTER,
   SUM(CASE
           WHEN CLUSTER_NUMBER IS NULL
               THEN 1
           ELSE 0
           END) AS NON_WH_QUERY_COUNT,
   SUM(CASE
           WHEN CLUSTER_NUMBER > 0
               THEN 1
           ELSE 0
           END) AS WH_QUERY_COUNT,
   SUM(CASE
           WHEN EXECUTION_STATUS = 'SUCCESS'
               THEN 0
           ELSE 1
           END) AS FAILURE_COUNT,
   SUM(CASE
           WHEN USER_NAME = 'SYSTEM'
               THEN 1
           ELSE 0
           END) AS SNOWFLAKE_WH_TASK
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY H
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr ON tr.object_name = h.role_name
  AND tr.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tr.DOMAIN = 'ROLE'
  AND h.user_name <> 'SYSTEM'
--Schema Snowflake Tasks
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ts ON ts.object_database = h.database_name
  AND ts.object_name = h.schema_name
  AND ts.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND ts.DOMAIN = 'SCHEMA'
  AND h.user_name = 'SYSTEM'
--User overrides
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tu ON tu.object_name = h.user_name
  AND tu.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tu.DOMAIN = 'USER'
--Warehouse fallback if nothing else tagged.
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tw ON tw.object_name = h.warehouse_name
  AND tw.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tw.DOMAIN = 'WAREHOUSE'
   AND role_name <> 'WORKSHEETS_APP_RL' --Snowsight UI role.
GROUP BY ALL;
