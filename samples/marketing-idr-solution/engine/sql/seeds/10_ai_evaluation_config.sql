-- ============================================================================
-- AI Evaluation Configuration and SP_AI_EVALUATE_CANDIDATES
-- Uses Cortex COMPLETE to evaluate candidate pairs
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDR;
USE SCHEMA CONFIG;
USE WAREHOUSE IDR_DEMO_WH;

-- Configuration table for AI evaluation thresholds
CREATE TABLE IF NOT EXISTS CONFIG.IDR_ML_AI_EVALUATION_CONFIG (
    CONFIG_KEY          VARCHAR(50) NOT NULL,
    CONFIG_VALUE        VARCHAR(100),
    DESCRIPTION         VARCHAR(500),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CONFIG_KEY)
);

-- Seed configuration values
MERGE INTO CONFIG.IDR_ML_AI_EVALUATION_CONFIG tgt
USING (
    SELECT * FROM VALUES
        ('AUTO_MATCH_THRESHOLD',   '0.95', 'Score >= this → STATUS=MATCH (auto-merge candidate)'),
        ('REVIEW_MIN_THRESHOLD',   '0.60', 'Score >= this but < match → STATUS=REVIEW (human review)'),
        ('AUTO_REJECT_THRESHOLD',  '0.60', 'Score < this → STATUS=NO_MATCH (discard)'),
        ('MAX_PAIRS_PER_RUN',      '50',   'Maximum candidate pairs to evaluate per SP run'),
        ('AI_MODEL',               'claude-4-sonnet', 'Cortex COMPLETE model to use')
    AS src(CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
) src
ON tgt.CONFIG_KEY = src.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    CONFIG_VALUE = src.CONFIG_VALUE,
    DESCRIPTION = src.DESCRIPTION,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT 
    (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
VALUES 
    (src.CONFIG_KEY, src.CONFIG_VALUE, src.DESCRIPTION);

-- View for current configuration
CREATE OR REPLACE VIEW CONFIG.V_AI_EVALUATION_CONFIG AS
SELECT CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT
FROM CONFIG.IDR_ML_AI_EVALUATION_CONFIG
ORDER BY CONFIG_KEY;

SELECT 'AI Evaluation Config created' AS STATUS;
SELECT * FROM CONFIG.V_AI_EVALUATION_CONFIG;
