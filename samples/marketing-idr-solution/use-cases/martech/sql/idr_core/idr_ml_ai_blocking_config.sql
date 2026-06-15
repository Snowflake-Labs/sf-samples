-- ============================================================================
-- Martech: CONFIG.IDR_ML_AI_BLOCKING_CONFIG (DDL only)
-- Seed data from engine/sql/seeds/08_ai_blocking_config.sql.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_ML_AI_BLOCKING_CONFIG') (
    BLOCKING_TIER       INT NOT NULL,
    BLOCKING_NAME       VARCHAR(100) NOT NULL,
    BLOCKING_KEY_COLUMN VARCHAR(100) NOT NULL,
    WEIGHT              INT NOT NULL DEFAULT 50,
    IS_ENABLED          BOOLEAN NOT NULL DEFAULT TRUE,
    MIN_CLUSTER_SIZE    INT DEFAULT 1,
    DESCRIPTION         VARCHAR(500),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BLOCKING_TIER)
);
