-- ============================================================================
-- Martech: Streams + Incremental Pipeline Task
-- Bronze streams created in ddl/*.sql alongside their tables.
-- This file creates the IDR_INCREMENTAL_TASK; suspended on deploy by default.
-- ============================================================================

CREATE OR REPLACE TASK IDENTIFIER('&{deployment_db}.APP.IDR_INCREMENTAL_TASK')
    WAREHOUSE = MARTECH_WH
    SCHEDULE = '1 MINUTE'
    WHEN
        SYSTEM$STREAM_HAS_DATA('&{deployment_db}.BRONZE.POS_TRANSACTION_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('&{deployment_db}.BRONZE.LOYALTY_MEMBER_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('&{deployment_db}.BRONZE.WEB_CLICKSTREAM_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('&{deployment_db}.BRONZE.SHOPIFY_ORDER_STREAM')
AS
    EXECUTE IMMEDIATE $$
        BEGIN
            CALL IDR.PROCEDURES.SP_RUN_IDR_PIPELINE('&{deployment_db}');
            CALL IDENTIFIER('&{deployment_db}.APP.SP_RUN_HOUSEHOLD_PIPELINE')('&{deployment_db}');
        END
    $$;

-- Task is created in SUSPENDED state by default. Resume from the UI Settings
-- page or via:  ALTER TASK <DB>.APP.IDR_INCREMENTAL_TASK RESUME;
