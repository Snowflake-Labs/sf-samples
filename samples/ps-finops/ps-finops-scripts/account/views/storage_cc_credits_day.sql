use role <% ctx.env.finops_db_admin_role %>;
use database <% ctx.env.finops_acct_db %>;
use schema <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW STORAGE_CC_CREDITS_DAY COMMENT = 'Title: Storage Cost Approximate by Cost Center per day. Description: Analyze storage cost approximate by cost center per day. Approximate as missing stage and hybrid.'
AS
WITH DE_DUP
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
        AND DAY >= DATEADD('DAY', - 31, CURRENT_DATE ()) --THIS IS JUST GETTING A MONTHS WORTH OF DATA FOR $X/TB/MONTH VALUE FOR COSTING IF AVAILABLE.
        AND DAY < CURRENT_DATE () + 1
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY DAY, TABLE_ID
            ORDER BY DAY DESC
            ) = 1 -- Snapshot process can be run multiple times a day, so we need to dedupe the data.
   )
SELECT DAY,
    COST_CENTER,
    ROUND(SUM(TOTAL_BYTES / POWER(1024, 4) / DAY(LAST_DAY(DAY))), 4) AS STORAGE_COST_CREDITS    -- Total Storage cost in credits for this day.
FROM DE_DUP TSD
GROUP BY ALL;
