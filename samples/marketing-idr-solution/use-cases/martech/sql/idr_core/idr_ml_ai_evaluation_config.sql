-- ============================================================================
-- Martech: CONFIG.IDR_ML_AI_EVALUATION_CONFIG (DDL only)
-- Seed data from engine/sql/seeds/10_ai_evaluation_config.sql.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_ML_AI_EVALUATION_CONFIG') (
    CONFIG_KEY   VARCHAR(100) NOT NULL PRIMARY KEY,
    CONFIG_VALUE VARCHAR(1000) NOT NULL,
    DESCRIPTION  VARCHAR(500),
    IS_ENABLED   BOOLEAN DEFAULT TRUE,
    CREATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
