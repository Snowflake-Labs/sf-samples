-- Note: Using organization_usage schema for ORG 2.0 Views
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_STORAGE_CREDITS_APPROX_CC_DAY COMMENT = 'Title: Storage Cost Approximate by Cost Center. Description: Analyze storage cost approximate by cost center not including hybrid tables.'
AS

WITH DE_DUP AS 
(
    -- Retrieve one month of table storage information.
   SELECT
   DATE_TRUNC('DAY', DATE_COLLECTED) DAY
   , COALESCE(TS.TAG_VALUE, 'UNKNOWN') AS COST_CENTER
    , TSD.ACCOUNT_LOCATOR
    , TSD.ACCOUNT_NAME
    , TABLE_ID
    , TABLE_CATALOG
    , TABLE_SCHEMA
    , TABLE_NAME
    , TOTAL_BYTES
   FROM TABLE_STORAGE_DETAILED_ORG TSD  -- Generated / dependent on this daily storage snapshot table
   LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TS
    ON TS.OBJECT_DATABASE = TSD.TABLE_CATALOG
       AND TS.ACCOUNT_LOCATOR = TSD.ACCOUNT_LOCATOR
       AND TS.OBJECT_NAME = TSD.TABLE_SCHEMA
       AND TS.TAG_NAME = '<% ctx.env.finops_tag_name %>'
       AND TS.DOMAIN = 'SCHEMA'
   WHERE TRUE
       AND DAY >= DATEADD('DAY', - 31, CURRENT_DATE ()) --This is just getting a months worth of data for $23/TB/Month value for costing if available.
       AND day < CURRENT_DATE () + 1
   QUALIFY ROW_NUMBER() OVER (
           PARTITION BY DAY, TABLE_ID
    ORDER BY DAY DESC
           ) = 1
   )
SELECT
    DAY
    , ACCOUNT_LOCATOR
    , ACCOUNT_NAME
    , COST_CENTER
    , ROUND(SUM(TOTAL_BYTES / POWER(1024, 4) / DAY(LAST_DAY(DAY))), 4) AS STORAGE_COST_CREDITS
FROM DE_DUP TSD
GROUP BY ALL;
