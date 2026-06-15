-- ============================================================================
-- Martech: CONFIG.IDR_CORE_STANDARDIZATION_RULES (DDL only)
-- Seed data is applied via engine/sql/seeds/00_standardization_rules.sql plus
-- martech-specific extensions in use-cases/martech/sql/config/standardization_rules_seed.sql.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_CORE_STANDARDIZATION_RULES') (
    RULE_ID           VARCHAR(50) PRIMARY KEY,
    SEMANTIC_CATEGORY VARCHAR(100) NOT NULL,
    RULE_NAME         VARCHAR(100) NOT NULL,
    SQL_EXPRESSION    VARCHAR(4000) NOT NULL,
    PRIORITY          INT DEFAULT 100,
    IS_ACTIVE         BOOLEAN DEFAULT TRUE,
    DESCRIPTION       VARCHAR(500),
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
