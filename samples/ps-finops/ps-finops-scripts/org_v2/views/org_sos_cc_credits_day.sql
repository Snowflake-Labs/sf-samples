-- Note: Using organization_usage schema for ORG 2.0 Views
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW org_sos_cc_credits_day COMMENT = 'Title: Search Optimization Service (SOS) Credit Usage by Cost Center. Description: Analyze search optimization credit usage by schema and cost center.'
as
SELECT
   DATE_TRUNC('DAY', SO.USAGE_DATE) AS DAY
   , COALESCE(TS.TAG_VALUE, SO.SCHEMA_NAME) AS COST_CENTER
   , SO.DATABASE_NAME || '.' || SO.SCHEMA_NAME AS TABLE_SCHEMA
   , SO.ACCOUNT_LOCATOR
   , SO.ACCOUNT_NAME
   , SUM(CREDITS_USED) CREDITS_USED -- TOTAL SEARCH OPTIMIZATION SERVICE CREDITS USED FOR THIS DAY.
FROM SNOWFLAKE.ORGANIZATION_USAGE.SEARCH_OPTIMIZATION_HISTORY SO
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TS
   ON TS.OBJECT_DATABASE = SO.DATABASE_NAME
      AND TS.ACCOUNT_LOCATOR = SO.ACCOUNT_LOCATOR
      AND TS.OBJECT_NAME = SO.SCHEMA_NAME
      AND TS.TAG_NAME = '<% ctx.env.finops_tag_name %>'
      AND TS.DOMAIN = 'SCHEMA'
GROUP BY ALL;
