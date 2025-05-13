USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW LOGGING_EVENTS_CC_CREDITS_DAY COMMENT = 'Title: Logging Event Credit Usage by Cost Center per day. Description: Analyze events logging credit usage by cost center per day.'
as
SELECT DATE(START_TIME)           AS DAY,
   'SNOWFLAKE ADMIN'              AS COST_CENTER,  -- UPDATE COST_CENTER AS NEEDED
   'LOGGING'                      AS SERVICE_TYPE,
   SUM(CREDITS_USED)              AS CREDITS_USED
FROM   SNOWFLAKE.ACCOUNT_USAGE.EVENT_USAGE_HISTORY
GROUP BY ALL;
