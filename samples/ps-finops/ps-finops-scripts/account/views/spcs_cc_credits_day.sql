USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW SPCS_CC_CREDITS_DAY COMMENT = 'Title: Snowpark Container Service Use by Cost Center per day. Description: Analyze container services credit costs by cost center per day.'
AS
SELECT
   DATE(START_TIME)                  AS DAY,
   COALESCE(TD.TAG_VALUE, APPLICATION_NAME, COMPUTE_POOL_NAME) AS COST_CENTER,
   'CONTAINER SERVICES'              AS SERVICE_TYPE,
   SUM(CREDITS_USED)                 AS CREDITS_USED 
FROM   SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY SH
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TD
   ON TD.OBJECT_NAME = SH.COMPUTE_POOL_NAME
   AND TD.TAG_NAME = '<% ctx.env.finops_tag_name %>'
   AND TD.DOMAIN = 'COMPUTE POOL'
GROUP BY ALL;
