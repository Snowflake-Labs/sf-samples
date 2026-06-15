-- ============================================================================
-- Martech: SILVER.IDR_CORE_EVENT_LOG
-- Pipeline event log (run start/end, milestones). Written by engine
-- SP_UPDATE_CLUSTERS, SP_RUN_IDR_PIPELINE.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_EVENT_LOG') (
    EVENT_ID           VARCHAR DEFAULT UUID_STRING(),
    EVENT_TYPE         VARCHAR,
    EVENT_DATA         VARIANT,
    EVENT_TIMESTAMP    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENT_SOURCE       VARCHAR,
    EVENT_DETAILS      VARIANT,
    PROCESSING_TIME_MS NUMBER,
    TASK_QUERY_ID      VARCHAR,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RUN_ID             VARCHAR,
    PIPELINE_RUN_ID    VARCHAR
);
