-- migrations/fix-dim-client-freshness.sql
-- Ensures DIM_CLIENT is refreshed at least once every 12 hours via a Snowflake Task.
-- This provides a 2x safety margin against the 24-hour freshness SLA.
--
-- Finding: FINANCE_DEMO.ANALYTICS.DIM_CLIENT last altered 28 hours ago,
--          exceeding the 24-hour freshness threshold (12 rows).

USE SCHEMA FINANCE_DEMO.ANALYTICS;

CREATE OR REPLACE TASK FINANCE_DEMO.ANALYTICS.REFRESH_DIM_CLIENT
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE  = 'USING CRON 0 */12 * * * America/Los_Angeles'
  COMMENT   = 'Refresh DIM_CLIENT to maintain 24h freshness SLA'
AS
  MERGE INTO FINANCE_DEMO.ANALYTICS.DIM_CLIENT AS tgt
  USING FINANCE_DEMO.RAW.CLIENTS AS src
    ON tgt.CLIENT_ID = src.CLIENT_ID
  WHEN MATCHED THEN UPDATE SET
    tgt.CLIENT_NAME   = src.CLIENT_NAME,
    tgt.CLIENT_EMAIL  = src.CLIENT_EMAIL,
    tgt.UPDATED_AT    = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (CLIENT_ID, CLIENT_NAME, CLIENT_EMAIL, UPDATED_AT)
    VALUES (src.CLIENT_ID, src.CLIENT_NAME, src.CLIENT_EMAIL, CURRENT_TIMESTAMP());

-- Activate the task
ALTER TASK FINANCE_DEMO.ANALYTICS.REFRESH_DIM_CLIENT RESUME;
