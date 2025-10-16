USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW CORTEX_ANALYST_CC_CREDITS_DAY COMMENT = 'Title: Cortex Analyst Credits by Cost Center per day. Description: Analyze cortex analyst use costs by cost center per day.'
AS

SELECT
   DATE_TRUNC('DAY', sfq.start_time) AS DAY,
   COALESCE(tu.tag_value, sfq.username) AS COST_CENTER, 
   SUM(credits) AS TOKEN_CREDITS,  -- Total TOKEN CREDITS for this day.
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY sfq 
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tu ON tu.object_name = sfq.username
  AND tu.tag_name = '<% ctx.env.finops_tag_name %>'
  AND tu.domain = 'USER'
GROUP BY ALL
;
