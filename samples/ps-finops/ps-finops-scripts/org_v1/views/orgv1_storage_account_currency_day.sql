USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORGV1_STORAGE_ACCOUNT_CURRENCY_DAY COMMENT = 'Title: Organizational Storage Usage by Account in Currency per Day. Description: Organizational Storage usage by account in currency.'
AS
WITH MR
AS (
    SELECT MAX(EFFECTIVE_RATE) AS METERING_RATE, DATE
    FROM SNOWFLAKE.ORGANIZATION_USAGE.RATE_SHEET_DAILY RS
    WHERE RATING_TYPE = 'STORAGE'
        AND IS_ADJUSTMENT = FALSE
        AND RS.DATE >= (
            SELECT MIN(START_DATE)
            FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS
            WHERE START_DATE <= CURRENT_DATE()
            AND CONTRACT_ITEM IN ('Capacity', 'Additional Capacity')
            AND NVL(EXPIRATION_DATE, '2999-01-01') > END_DATE
            )
    GROUP BY ALL
    )
SELECT USAGE_DATE,
   ACCOUNT_NAME,
   CONCAT_WS('-', REGION, ACCOUNT_NAME) AS REGION_ACCOUNT_NAME,
AVERAGE_BYTES / POWER(1024, 4) AS AVG_TB,
CREDITS * MR.METERING_RATE AS USAGE_IN_CURRENCY
FROM SNOWFLAKE.ORGANIZATION_USAGE.STORAGE_DAILY_HISTORY
JOIN MR ON MR.DATE = USAGE_DATE
;
