-- Note: Using organization_usage schema for ORG 2.0 Views
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_DATABASE_STORAGE_ACCOUNT COMMENT = 'Title: Database Storage Size by Type. Description: Analyze storage size by database and type.'
as
SELECT
   USAGE_DATE,
   ACCOUNT_LOCATOR,
   ACCOUNT_NAME,
   ROUND(AVERAGE_DATABASE_BYTES / POWER(1024, 4), 2) AS AVG_DATABASE_TB,
   ROUND(AVERAGE_FAILSAFE_BYTES / POWER(1024, 4), 2) AS AVG_FAILSAFE_TB,
   ROUND(AVERAGE_HYBRID_TABLE_STORAGE_BYTES / POWER(1024, 4), 2) AS AVG_HYBRID_TB,
   DELETED
FROM SNOWFLAKE.ORGANIZATION_USAGE.DATABASE_STORAGE_USAGE_HISTORY
;
