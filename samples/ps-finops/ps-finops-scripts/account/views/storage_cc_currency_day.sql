USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW STORAGE_CC_CURRENCY_DAY COMMENT = 'Title: Storage Cost Approximate by Cost Center per day. Description: Analyze storage cost approximate by cost center per day. Approximate as missing stage and hybrid.'
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
            WHERE END_DATE > CURRENT_DATE ()
        AND START_DATE <= CURRENT_DATE ()
        AND CONTRACT_ITEM IN ('Capacity', 'Additional Capacity', 'Free Usage')
        AND NVL(EXPIRATION_DATE, '2999-01-01') > START_DATE
            )
    GROUP BY DATE
   ),
DE_DUP
AS (
    -- Retrieve one month of table storage information.
    SELECT DATE_TRUNC('DAY', DATE_COLLECTED) DAY,
        COALESCE(TS.TAG_VALUE, 'UNKNOWN') AS COST_CENTER,
        TABLE_ID,
        TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME,
        TOTAL_BYTES
    FROM TABLE_STORAGE_DETAILED TSD     -- Generated / dependent on this daily storage snapshot table
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ts ON ts.object_database = tsd.table_catalog
        AND ts.object_name = tsd.table_schema
        AND ts.TAG_NAME = '<% ctx.env.finops_tag_name %>'
        AND ts.DOMAIN = 'SCHEMA'
    WHERE TRUE
        AND DAY >= DATEADD('DAY', - 31, CURRENT_DATE ()) --THIS IS JUST GETTING A MONTHS WORTH OF DATA FOR $23/TB/MONTH VALUE FOR COSTING IF AVAILABLE.
        AND DAY < CURRENT_DATE () + 1
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY DAY, TABLE_ID
            ORDER BY DAY DESC
            ) = 1 -- Snapshot process can be run multiple times a day, so we need to dedupe the data.
   )
SELECT DAY,
    COST_CENTER,
    ROUND(SUM(TOTAL_BYTES / POWER(1024, 4) * (MR.METERING_RATE) / DAY(LAST_DAY(DAY))), 4) AS STORAGE_COST_CURRENCY    -- Total Storage cost in respective currency for this day.
FROM DE_DUP TSD
JOIN MR ON MR.DATE = DAY
GROUP BY ALL;
