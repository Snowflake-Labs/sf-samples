USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW MATERIALIZED_VIEW_CC_CREDITS_DAY COMMENT = 'Title: Materialized View (MV) Credit Usage by Cost Center. Description: Analyze materialized view credit costs by table name and cost center.'
AS
SELECT
   DATE_TRUNC('DAY', END_TIME) AS DAY,
   COALESCE(TS.TAG_VALUE, TABLE_NAME) AS COST_CENTER,
   MV.DATABASE_NAME || '.' || MV.SCHEMA_NAME || '.' || MV.TABLE_NAME AS TABLE_NAME,
   CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MV
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TS ON TS.OBJECT_DATABASE = MV.DATABASE_NAME
   AND TS.OBJECT_NAME = MV.SCHEMA_NAME
   AND TS.TAG_NAME = '<% ctx.env.finops_tag_name %>'
   AND TS.DOMAIN = 'SCHEMA'
;
