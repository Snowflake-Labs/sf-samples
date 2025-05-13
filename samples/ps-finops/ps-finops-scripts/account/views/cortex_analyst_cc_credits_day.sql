USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW CORTEX_ANALYST_CC_CREDITS_DAY COMMENT = 'Title: Cortex Analyst Credits by Cost Center per day. Description: Analyze cortex analyst use costs by cost center per day.'
AS

SELECT
   DATE_TRUNC('DAY', SF.ADJUSTED_START_TIME) AS DAY,
   COALESCE(tu.TAG_VALUE, SFQ.USER_NAME) AS COST_CENTER, 
   SUM(CREDITS) AS TOKEN_CREDITS,  -- Total TOKEN CREDITS for this day.
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY SFQ 
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tu ON tu.object_name = sf.user_name
  AND tu.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND tu.DOMAIN = 'USER'
GROUP BY ALL
;
