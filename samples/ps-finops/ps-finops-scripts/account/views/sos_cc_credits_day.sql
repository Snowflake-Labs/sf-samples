USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW SOS_CC_CREDITS_DAY COMMENT = 'Title: Search Optimization Service (SOS) Credit Usage by Cost Center per day. Description: Analyze search optimization credit usage by schema and cost center per day.'
AS
SELECT
   DATE_TRUNC('DAY', START_TIME) AS DAY,
   COALESCE(TS.TAG_VALUE, SO.SCHEMA_NAME) AS COST_CENTER,
   SO.DATABASE_NAME || '.' || SO.SCHEMA_NAME AS TABLE_SCHEMA,
   SUM(CREDITS_USED) CREDITS_USED -- Total Search Optimization Service credits used for this day.
FROM snowflake.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY so
LEFT JOIN snowflake.ACCOUNT_USAGE.TAG_REFERENCES ts ON ts.OBJECT_DATABASE = so.DATABASE_NAME
   AND ts.OBJECT_NAME = so.SCHEMA_NAME
   AND ts.TAG_NAME = '<% ctx.env.finops_tag_name %>'
   AND ts.DOMAIN = 'SCHEMA'
GROUP BY ALL;
