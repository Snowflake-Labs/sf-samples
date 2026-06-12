-- ============================================================================
-- Martech: CONFIG.IDR_CORE_NICKNAME_MAP (DDL only)
-- Seed data is applied separately by deploy/02_engine.sql (sourced from
-- engine/sql/seeds/00_nickname_map_seed.sql with DB qualifier rewritten).
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_CORE_NICKNAME_MAP') (
    nickname        VARCHAR NOT NULL,
    canonical_name  VARCHAR NOT NULL,
    gender_hint     VARCHAR(1),
    confidence      FLOAT DEFAULT 1.0,
    PRIMARY KEY (nickname)
);
