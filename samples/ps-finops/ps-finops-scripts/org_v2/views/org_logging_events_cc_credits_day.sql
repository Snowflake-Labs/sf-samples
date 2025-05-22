USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_LOGGING_EVENTS_BY_COST_CENTER COMMENT = 'Title: Logging Event Credit Usage by Cost Center. Description: Analyze events logging credit usage by cost center.'
AS
SELECT
   DATE(START_TIME)               AS DAY,
   'SNOWFLAKE ADMIN'              AS COST_CENTER,  -- UPDATE COST_CENTER AS NEEDED
   ACCOUNT_LOCATOR                AS ACCOUNT_LOCATOR,
   ACCOUNT_NAME                   AS ACCOUNT_NAME,
   'LOGGING'                      AS SERVICE_TYPE,
   SUM(CREDITS_USED)              AS CREDITS_USED
FROM   SNOWFLAKE.ORGANIZATION_USAGE.EVENT_USAGE_HISTORY
GROUP  BY ALL;
