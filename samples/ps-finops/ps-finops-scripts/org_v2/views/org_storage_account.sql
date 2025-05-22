-- Note: Using organization_usage schema for ORG 2.0 Views
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_STORAGE_ACCOUNT COMMENT = 'Title: Storage Size by Type. Description: Analyze storage size by types such as storage, stage, failsafe, and hybrid.'
as
SELECT
   USAGE_DATE,
   ACCOUNT_LOCATOR,
   ACCOUNT_NAME,
   ROUND(STORAGE_BYTES / POWER(1024, 4), 2) AS STORAGE_TB, --INCLUDES CLONED TABLES.
   ROUND(STAGE_BYTES / POWER(1024, 4), 2) AS STAGE_TB,
   ROUND(FAILSAFE_BYTES / POWER(1024, 4), 2) AS FAILSAFE_TB,
   ROUND(HYBRID_TABLE_STORAGE_BYTES / POWER(1024, 4), 2) AS HYBRID_TB
FROM SNOWFLAKE.ORGANIZATION_USAGE.STORAGE_USAGE
;
