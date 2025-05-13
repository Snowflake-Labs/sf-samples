USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_CLOUD_SERVICES_CREDITS_DAY COMMENT = 'Title: Cloud Service Usage by Account per day. Description: Analyze cloud service costs by account, useful for finding large culprits in credit services spend when >10% compute usage of it from org billing dashboard.'
AS
SELECT
   USAGE_DATE,
   ACCOUNT_NAME,
   CONCAT_WS('-', REGION, ACCOUNT_NAME) AS REGION_ACCOUNT_NAME,
   CREDITS_USED_CLOUD_SERVICES,
   CREDITS_ADJUSTMENT_CLOUD_SERVICES,
   CREDITS_USED_CLOUD_SERVICES + CREDITS_ADJUSTMENT_CLOUD_SERVICES AS BILLED_CLOUD_SERVICES
FROM SNOWFLAKE.ORGANIZATION_USAGE.METERING_DAILY_HISTORY
WHERE CREDITS_USED_CLOUD_SERVICES > 0
;
