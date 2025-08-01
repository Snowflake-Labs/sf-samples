USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW CORTEX_FUNCTION_CC_TOKEN_CREDITS_DAY COMMENT = 'Title: Cortex Function Token Credits by Cost Center per day. Description: Analyze cortex function use costs by cost center per day.'
AS

SELECT
   DATE_TRUNC('DAY', SF.ADJUSTED_START_TIME) AS DAY,
   GET_TAG_COST_CENTER(SF.QUERY_TAG, SF.USER_NAME, TU.TAG_VALUE, SF.ROLE_NAME, TR.TAG_VALUE, TS.TAG_VALUE, TW.TAG_VALUE) AS COST_CENTER,
   ZEROIFNULL(SUM(TOKEN_CREDITS)) AS TOKEN_CREDITS,  -- Total TOKEN CREDITS for this day.
   FUNCTION_NAME, 
   MODEL_NAME
FROM SF_CREDITS_BY_QUERY SF -- Generated by cost per query procedure
JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY SFQ ON SF.QUERY_ID = SFQ.QUERY_ID
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr ON tr.object_name = sf.role_name
  AND tr.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tr.DOMAIN = 'ROLE'
  AND sf.user_name <> 'SYSTEM' -- Exclude Tasks that will be attributed to schema instead of Access Roles that are not tagged.
--Schema
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ts ON ts.object_database = sf.database_name
  AND ts.object_name = sf.schema_name
  AND ts.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND ts.DOMAIN = 'SCHEMA'
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
