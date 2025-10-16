use role <% ctx.env.finops_db_admin_role %>;
use database <% ctx.env.finops_acct_db %>;
use schema <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW REPLICATION_DATA_TRANSFER_CC_CREDITS_DAY COMMENT = 'Title: Replication Data Transfer Use by Cost Center per day. Description: Analyze replication credit costs by cost center per day.'
AS
SELECT
   DATE(START_TIME)                  AS DAY,
   COALESCE(TD.TAG_VALUE, 'UNKNOWN') AS COST_CENTER,
   'DATABASE REPLICATION'                     AS SERVICE_TYPE,
   SUM(BYTES_TRANSFERRED)/POWER(1024,3) AS TB_TRANSEFERRED, -- TOTAL DATA TRANSFERRED IN TB FOR THIS DAY.
   SUM(CREDITS_USED)                 AS CREDITS_USED  -- TOTAL CREDITS USED FOR REPLICATION FOR THIS DAY.
FROM   SNOWFLAKE.ACCOUNT_USAGE.DATABASE_REPLICATION_USAGE_HISTORY UH
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TD
   ON TD.OBJECT_DATABASE = UH.DATABASE_NAME
   AND TD.TAG_NAME = '<% ctx.env.finops_tag_name %>'
   AND TD.DOMAIN = 'DATABASE'
GROUP BY ALL;
