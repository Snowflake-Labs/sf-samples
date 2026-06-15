-- ============================================================================
-- Martech: SILVER.IDR_ML_INDIVIDUAL_MATCH_EXPLAIN
-- ML decision audit trail — one row per ML-driven edge emitted to MATCH_RESULTS.
-- Joined on demand by frontend for explainability panels.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_ML_INDIVIDUAL_MATCH_EXPLAIN') (
    EXPLAIN_ID            VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    MATCH_ID              VARCHAR NOT NULL,         -- FK to IDR_CORE_MATCH_RESULTS.MATCH_ID
    PAIR_ID               VARCHAR NOT NULL,         -- FK to ML_PAIR_FEATURES.PAIR_ID
    ML_MODEL_VERSION      VARCHAR NOT NULL,
    ML_PROB               FLOAT NOT NULL,
    AUTO_MERGE_THRESHOLD  FLOAT,                    -- threshold-at-emit snapshot (per NFR-5)
    FEATURE_VALUES        VARIANT,                  -- JSON: full feature snapshot at scoring time
    TOP_POSITIVE_FEATURES VARIANT,                  -- ARRAY of {name, value, contribution} pushing toward MATCH
    TOP_NEGATIVE_FEATURES VARIANT,                  -- features pushing toward NO_MATCH
    BLOCKING_KEYS_HIT     ARRAY,                    -- which strategies put this pair on the table
    SCORED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
