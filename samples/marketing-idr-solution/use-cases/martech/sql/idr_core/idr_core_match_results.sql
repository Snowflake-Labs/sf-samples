-- ============================================================================
-- Martech: SILVER.IDR_CORE_MATCH_RESULTS
-- Per-rule match edges; written by engine SP_RUN_MATCHING.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_MATCH_RESULTS') (
    MATCH_ID                 VARCHAR DEFAULT UUID_STRING(),
    SOURCE_RECORD_ID_A       VARCHAR,
    SOURCE_RECORD_ID_B       VARCHAR,
    NEW_SOURCE_RECORD_ID     VARCHAR,
    MATCHED_SOURCE_RECORD_ID VARCHAR,
    RULE_ID                  VARCHAR,
    RULE_NAME                VARCHAR,
    MATCH_SCORE              FLOAT,
    MATCHED_ON               VARCHAR,
    IS_ACTIVE                BOOLEAN DEFAULT TRUE,
    IS_CURRENT               BOOLEAN DEFAULT TRUE,
    SUPERSEDED_BY            VARCHAR,
    IDENTIFIER_1             VARCHAR,
    IDENTIFIER_2             VARCHAR,
    EDGE_POLARITY            VARCHAR DEFAULT 'MATCH',        -- 'MATCH' | 'VETO' (steward-issued anti-edges)
    EDGE_TARGET              VARCHAR DEFAULT 'INDIVIDUAL',   -- 'INDIVIDUAL' | 'HOUSEHOLD' (future track)
    CREATED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT               TIMESTAMP_NTZ,
    RUN_ID                   VARCHAR,
    PIPELINE_RUN_ID          VARCHAR
);

-- v1: idempotent column migrations are owned by engine/sql/migrations/v1_idr_adjudication.sql
-- (runs as part of every use-case deploy_all.sh).
