-- ============================================================================
-- Martech: SILVER.LLM_REVIEW_QUEUE
-- Holds LLM verdicts for borderline ML candidates (0.55 <= ML score < 0.85).
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.LLM_REVIEW_QUEUE') (
    REVIEW_ID            VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    PAIR_ID              VARCHAR,
    SOURCE_RECORD_ID_A   VARCHAR,
    SOURCE_TYPE_A        VARCHAR,
    SOURCE_RECORD_ID_B   VARCHAR,
    SOURCE_TYPE_B        VARCHAR,
    ML_SCORE             FLOAT,
    LLM_VERDICT          VARCHAR,         -- MATCH / NOT_MATCH / UNCLEAR
    LLM_RATIONALE        VARCHAR(4000),
    LLM_CONFIDENCE       FLOAT,           -- model's same-person probability (0-1); drives R17 MATCH_SCORE
    LLM_RAW_OUTPUT       VARCHAR(4000),
    LLM_PARSE_ERROR      VARCHAR,
    LLM_MODEL            VARCHAR,
    LLM_PROMPT_KEY       VARCHAR,
    LLM_TOKENS_INPUT     INT,
    LLM_TOKENS_OUTPUT    INT,
    HUMAN_VERDICT        VARCHAR,         -- ACCEPT / REJECT / NULL (pending)
    HUMAN_REVIEWER       VARCHAR,
    HUMAN_REVIEWED_AT    TIMESTAMP_NTZ,
    EMITTED_MATCH_ID     VARCHAR,
    STATUS               VARCHAR DEFAULT 'PENDING_HUMAN', -- PENDING_HUMAN / ACCEPTED / REJECTED
    CREATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT           TIMESTAMP_NTZ
);

-- Idempotent column add for existing deployments (CREATE IF NOT EXISTS above
-- will not alter an already-created table).
ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.LLM_REVIEW_QUEUE') ADD COLUMN IF NOT EXISTS LLM_CONFIDENCE FLOAT;
