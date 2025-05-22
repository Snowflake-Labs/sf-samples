-- Note: Using organization_usage schema for ORG 2.0 Views
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_STORAGE_CURRENCY_APPROX_CC_DAY COMMENT = 'Title: Storage Cost Approximate by Cost Center. Description: Analyze storage cost approximate by cost center not including hybrid tables.'
as
WITH mr
AS (
    -- Retrieve metering rate
   SELECT AVG(EFFECTIVE_RATE) AS METERING_RATE, DATE
   FROM SNOWFLAKE.ORGANIZATION_USAGE.RATE_SHEET_DAILY RS
   WHERE RATING_TYPE = 'STORAGE'
       AND IS_ADJUSTMENT = FALSE
       AND ACCOUNT_LOCATOR = CURRENT_ACCOUNT()
       AND RS.DATE >= (
           SELECT MIN(START_DATE)
           FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS
           WHERE START_DATE <= CURRENT_DATE()
            AND CONTRACT_ITEM IN ('CAPACITY', 'ADDITIONAL CAPACITY')
            AND NVL(EXPIRATION_DATE, '2999-01-01') > END_DATE
           )
    GROUP BY DATE
   ),
DE_DUP
AS (
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
    , ROUND(SUM(TOTAL_BYTES / POWER(1024, 4) * (MR.METERING_RATE) / DAY(LAST_DAY(DAY))), 4) AS STORAGE_COST_CURRENCY    -- Total Storage cost in respective currency for this day.
FROM DE_DUP TSD
JOIN MR ON MR.DATE = DAY
GROUP BY ALL;
