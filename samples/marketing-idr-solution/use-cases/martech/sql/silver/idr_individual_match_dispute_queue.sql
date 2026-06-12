-- ============================================================================
-- Martech: SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE
-- Surfaces disagreements between trained ML and deterministic rules.
-- Filled by SP_ML_SCORE_CANDIDATES when ML_PROB < ML_DISPUTE_GATE_THRESHOLD
-- AND a deterministic edge already exists for the pair.
-- Severity (Def B from requirements.md FR-10):
--   ML_PROB < ML_DISPUTE_STRONG_THRESHOLD → STRONG
--   else                                  → MILD
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE') (
    DISPUTE_ID                VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    PAIR_ID                   VARCHAR NOT NULL,
    SOURCE_RECORD_ID_A        VARCHAR NOT NULL,
    SOURCE_RECORD_ID_B        VARCHAR NOT NULL,
    DETERMINISTIC_MATCH_ID    VARCHAR NOT NULL,    -- FK to MATCH_RESULTS row
    DETERMINISTIC_RULE_ID     VARCHAR NOT NULL,    -- e.g. 'MARTECH_R01'
    ML_PROB                   FLOAT NOT NULL,
    ML_MODEL_VERSION          VARCHAR,
    ML_TOP_NEGATIVE_FEATURES  VARIANT,
    DISPUTE_SEVERITY          VARCHAR NOT NULL,    -- 'STRONG' | 'MILD'
    DISPUTE_REASON            VARCHAR,             -- e.g. 'ML_BELOW_DISPUTE_GATE'
    -- v2 hooks for LLM tiebreak (Option D)
    LLM_VERDICT               VARCHAR,             -- 'CONFIRM_MERGE' | 'SPLIT' | 'UNCERTAIN' | NULL
    LLM_REASONING             VARCHAR,
    LLM_CALLED_AT             TIMESTAMP_NTZ,
    -- Steward decision
    STATUS                    VARCHAR NOT NULL DEFAULT 'PENDING',  -- 'PENDING' | 'RESOLVED_KEEP' | 'RESOLVED_SPLIT'
    STEWARD_DECISION          VARCHAR,             -- 'KEEP_MERGE' | 'SPLIT'
    STEWARD_REASON            VARCHAR,
    STEWARD_USER              VARCHAR,
    DECIDED_AT                TIMESTAMP_NTZ,
    CREATED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Cluster by status for steward queue read patterns
ALTER TABLE IDENTIFIER('&{deployment_db}.SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE')
    CLUSTER BY (STATUS, DISPUTE_SEVERITY);
