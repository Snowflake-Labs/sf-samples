USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW COMPUTE_AND_QAS_CC_CREDITS_WOW COMMENT = 'Title: Compute & Query Acceleration Service (QAS) Credits by Cost Center Week over Week. Description: Analyze the major costs incurred in Snowflake Compute use week over week. Does not account for extra cloud services fees above the 10% threshold.'
as
SELECT
   DATE_TRUNC('DAY', START_SLICE) AS DAY,
   GET_TAG_COST_CENTER(SF.QUERY_TAG, SF.USER_NAME, TU.TAG_VALUE, SF.ROLE_NAME, TR.TAG_VALUE, TS.TAG_VALUE, TW.TAG_VALUE) AS COST_CENTER,
   ROUND(SUM(ALLOCATED_CREDITS_USED_COMPUTE + ZEROIFNULL(ALLOCATED_CREDITS_USED_QAS)), 2) AS COMPUTE_CREDITS, -- TOTAL WAREHOUSE COMPUTE IN CREDITS
   ZEROIFNULL(COMPUTE_CREDITS - LAG(COMPUTE_CREDITS, 1) OVER (
           PARTITION BY COST_CENTER ORDER BY DAY ASC
           )) AS WOW_DIFF,  -- Week over week difference in compute credits
   DIV0(WOW_DIFF, LAG(COMPUTE_CREDITS, 1) OVER (
           PARTITION BY COST_CENTER ORDER BY DAY ASC
           )) * 100 AS WOW_PERC_DIFF  -- Week over week percent difference in compute credits
FROM SF_CREDITS_BY_QUERY SF -- Generated by cost per query procedure
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr ON tr.object_name = sf.role_name
  AND tr.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tr.DOMAIN = 'ROLE'
  AND sf.user_name <> 'SYSTEM' -- Exclude Tasks that will be attributed to schema instead of Access Roles that are not tagged.
--Schema Snowflake Tasks
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ts ON ts.object_database = sf.database_name
  AND ts.object_name = sf.schema_name
  AND ts.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND ts.DOMAIN = 'SCHEMA'
  AND sf.user_name = 'SYSTEM'
--User overrides
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tu ON tu.object_name = sf.user_name
  AND tu.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tu.DOMAIN = 'USER'
--Warehouse fallback if nothing else tagged.
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tw ON tw.object_id = sf.warehouse_id
  AND tw.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tw.DOMAIN = 'WAREHOUSE'
GROUP BY ALL
;
