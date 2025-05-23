USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORGV1_COMPUTE_WH_ACCOUNT_CREDIT_DAY COMMENT = 'Title: Organizational Compute Usage By WH and Account per Day. Description: Organizational Total compute usage in credits by wh and account per day.'
AS
SELECT
   DATE_TRUNC('DAY', START_TIME) AS DAY,
   ACCOUNT_NAME,
   CONCAT_WS('-', REGION, ACCOUNT_NAME) AS REGION_ACCOUNT_NAME,
   CONCAT_WS('-', REGION, ACCOUNT_NAME, WAREHOUSE_NAME) AS REGION_WH_NAME,
   SUM(CREDITS_USED_COMPUTE) AS CREDITS_COMPUTE,
FROM SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME NOT LIKE 'CLOUD_SERVICES%'
GROUP BY ALL;
