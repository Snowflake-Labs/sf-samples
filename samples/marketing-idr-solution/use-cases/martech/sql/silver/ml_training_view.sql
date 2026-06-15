-- ============================================================================
-- Martech: SILVER.V_ML_TRAINING_VIEW
-- Labeled feature view for training the LightGBM-style scorer.
-- Positives: pairs that already match deterministically via R01-R15.
-- Negatives: pairs in same blocking bucket WITHOUT any deterministic edge.
-- ============================================================================

CREATE OR REPLACE VIEW IDENTIFIER('&{deployment_db}.SILVER.V_ML_TRAINING_VIEW') AS
WITH deterministic_edges AS (
    SELECT DISTINCT
        LEAST(SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B) AS REC_A,
        GREATEST(SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B) AS REC_B
    FROM IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_MATCH_RESULTS')
    WHERE IS_CURRENT = TRUE
      AND RULE_ID IN ('MARTECH_R01','MARTECH_R02','MARTECH_R03','MARTECH_R04',
                      'MARTECH_R05','MARTECH_R06','MARTECH_R07','MARTECH_R08',
                      'MARTECH_R09','MARTECH_R10','MARTECH_R11','MARTECH_R12',
                      'MARTECH_R13','MARTECH_R14','MARTECH_R15')
)
SELECT
    f.PAIR_ID,
    f.SOURCE_RECORD_ID_A,
    f.SOURCE_TYPE_A,
    f.SOURCE_RECORD_ID_B,
    f.SOURCE_TYPE_B,
    f.BLOCKING_KEY,
    f.EMAIL_EQ, f.HEM_EQ, f.PHONE_EQ, f.LOYALTY_ID_EQ, f.DEVICE_ID_EQ,
    f.JW_FIRST_NAME, f.JW_LAST_NAME, f.JW_STREET, f.JW_CITY,
    f.POSTAL_EQ, f.STATE_EQ, f.NICKNAME_FIRST_EQ,
    f.EMAIL_LEN_DIFF, f.PHONE_LEN_DIFF,
    -- Auto-label: 1 if a deterministic edge exists, else 0
    CASE WHEN d.REC_A IS NOT NULL THEN 1 ELSE 0 END AS LABEL
FROM IDENTIFIER('&{deployment_db}.SILVER.ML_PAIR_FEATURES') f
LEFT JOIN deterministic_edges d
  ON LEAST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = d.REC_A
 AND GREATEST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = d.REC_B;
