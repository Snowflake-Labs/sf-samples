-- ============================================================================
-- Martech: SILVER.IDR_CORE_MATCH_LOG
-- Per-rule execution log (pairs found, duration). Written by engine
-- SP_RUN_MATCHING and SP_UPDATE_CLUSTERS.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_MATCH_LOG') (
    LOG_ID            VARCHAR DEFAULT UUID_STRING(),
    MATCH_ID          VARCHAR,
    RULE_ID           VARCHAR,
    PAIRS_FOUND       INTEGER,
    DURATION_MS       INTEGER,
    SOURCE_RECORD_IDS VARIANT,
    DECISION          VARCHAR,
    MATCH_DETAILS     VARIANT,
    IS_CURRENT        BOOLEAN DEFAULT TRUE,
    SUPERSEDED_BY     VARCHAR,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RUN_ID            VARCHAR,
    PIPELINE_RUN_ID   VARCHAR
);
