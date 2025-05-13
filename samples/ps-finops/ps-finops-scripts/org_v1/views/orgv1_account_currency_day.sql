USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORGV1_ACCOUNT_CURRENCY_DAY COMMENT = 'Title: Organizational Usage by Account in Currency. Description: Organizational usage by accounts in currency.'
AS
SELECT
   USAGE_DATE,
   ACCOUNT_NAME,
   CONCAT_WS('-', REGION, ACCOUNT_NAME) AS REGION_ACCOUNT_NAME,
   'DAILY TOTAL' USAGE_TYPE,
   SUM(USAGE_IN_CURRENCY) USAGE_IN_CURRENCY
FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
GROUP BY ALL
HAVING SUM(USAGE_IN_CURRENCY) > 0
;
