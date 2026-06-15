-- ============================================================================
-- Martech: SILVER.ML_PAIR_FEATURES
-- Candidate pair features for the ML scorer. Individual-resolution track.
-- v1: multi-strategy blocking (CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES);
-- legacy ML_BLOCKING_STRATEGY config key superseded.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') (
    PAIR_ID                VARCHAR DEFAULT UUID_STRING(),
    SOURCE_RECORD_ID_A     VARCHAR,
    SOURCE_TYPE_A          VARCHAR,
    SOURCE_RECORD_ID_B     VARCHAR,
    SOURCE_TYPE_B          VARCHAR,
    BLOCKING_KEY           VARCHAR,
    -- Identifier-level features
    EMAIL_EQ               BOOLEAN,
    HEM_EQ                 BOOLEAN,
    PHONE_EQ               BOOLEAN,
    LOYALTY_ID_EQ          BOOLEAN,
    DEVICE_ID_EQ           BOOLEAN,
    -- String-similarity features
    JW_FIRST_NAME          FLOAT,
    JW_LAST_NAME           FLOAT,
    JW_STREET              FLOAT,
    JW_CITY                FLOAT,
    -- Categorical features
    POSTAL_EQ              BOOLEAN,
    STATE_EQ               BOOLEAN,
    NICKNAME_FIRST_EQ      BOOLEAN,
    EMAIL_HANDLE_EQ        BOOLEAN,   -- email local-part match (gmail-dot/plus normalized handle)
    PHONE_LAST7_EQ         BOOLEAN,   -- trailing-7-digit phone match (local-number signal)
    -- Length / shape features
    EMAIL_LEN_DIFF         INT,
    PHONE_LEN_DIFF         INT,
    -- Score (legacy linear scorer)
    SCORE                  FLOAT,
    LABEL                  INT,                    -- 1=positive, 0=negative, NULL=unlabeled
    STATUS                 VARCHAR DEFAULT 'PENDING', -- PENDING / SCORED / EMITTED
    EMITTED_MATCH_ID       VARCHAR,
    -- v1: multi-strategy blocking + trained-model fields
    BLOCKING_KEYS_HIT      ARRAY,                   -- which strategies fired ['B5','B6',...]
    BLOCK_SIZE_AT_GEN      NUMBER,                  -- monitoring; size of largest contributing block
    ML_PROB                FLOAT,                   -- trained model probability (parallel to SCORE during transition)
    ML_MODEL_VERSION       VARCHAR,                 -- e.g. 'idr_pair_v0_linear'
    ML_TIER                VARCHAR,                 -- 'AUTO_MERGE' | 'GREY_LLM' | 'DROP' | 'DISPUTE'
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SCORED_AT              TIMESTAMP_NTZ,
    UPDATED_AT             TIMESTAMP_NTZ
);

ALTER TABLE IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES')
    CLUSTER BY (BLOCKING_KEY, STATUS);

-- Idempotent migration for existing deployments (one ALTER per column — Snowflake limitation).
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') ADD COLUMN IF NOT EXISTS BLOCKING_KEYS_HIT  ARRAY;
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') ADD COLUMN IF NOT EXISTS BLOCK_SIZE_AT_GEN  NUMBER;
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') ADD COLUMN IF NOT EXISTS ML_PROB            FLOAT;
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') ADD COLUMN IF NOT EXISTS ML_MODEL_VERSION   VARCHAR;
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') ADD COLUMN IF NOT EXISTS ML_TIER            VARCHAR;
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') ADD COLUMN IF NOT EXISTS EMAIL_HANDLE_EQ    BOOLEAN;
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') ADD COLUMN IF NOT EXISTS PHONE_LAST7_EQ     BOOLEAN;
