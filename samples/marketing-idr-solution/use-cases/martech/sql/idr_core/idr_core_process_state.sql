-- ============================================================================
-- Martech: SILVER.IDR_CORE_PROCESS_STATE
-- Tracks last-run timestamps for incremental processing. Written by engine
-- SP_RUN_MATCHING, SP_IDENTIFY_AI_CANDIDATES, SP_RESET_IDR_CORE.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_PROCESS_STATE') (
    PROCESS_KEY      VARCHAR,
    PROCESS_NAME     VARCHAR,
    PROCESS_VALUE    VARIANT,
    LAST_RUN_AT      TIMESTAMP_NTZ,
    LAST_RUN_STATUS  VARCHAR,
    LAST_RUN_DETAILS VARIANT,
    UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
