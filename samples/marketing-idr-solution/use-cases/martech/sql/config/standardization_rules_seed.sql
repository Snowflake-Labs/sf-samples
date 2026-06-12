-- ============================================================================
-- Martech: Standardization Rules Seed (extension to engine seed)
-- Inserts martech-specific entries into <DB>.CONFIG.IDR_CORE_STANDARDIZATION_RULES
-- (the engine-supplied base rules are seeded by deploy/02_engine.sql).
--
-- Idempotent via MERGE; safe to re-run.
-- DB qualifier resolved via &{deployment_db} at deploy time.
-- ============================================================================

MERGE INTO IDENTIFIER('&{deployment_db}.CONFIG.IDR_CORE_STANDARDIZATION_RULES') AS t
USING (
    SELECT column1 AS RULE_ID, column2 AS SEMANTIC_CATEGORY, column3 AS RULE_NAME,
           column4 AS SQL_EXPRESSION, column5 AS DESCRIPTION
    FROM VALUES
        -- HEM (SHA-256 hashed lowercased+trimmed email)
        ('MARTECH_HEM_STD', 'EMAIL', 'HEM Hash',
         'SHA2_HEX(LOWER(TRIM({col})), 256)',
         'Compute SHA-256 hash of normalized email (HEM) for paid-media match'),

        -- Loyalty member number (UPPER + TRIM)
        ('MARTECH_LOYALTY_ID_STD', 'LOYALTY_ID', 'Loyalty Member Number Normalize',
         'UPPER(TRIM({col}))',
         'Uppercase and trim loyalty member numbers (e.g., LM-1234567)'),

        -- Cookie / anonymous ID (preserve case, trim only)
        ('MARTECH_COOKIE_ID_STD', 'COOKIE_ID', 'Cookie ID Trim',
         'TRIM({col})',
         'Trim whitespace from web cookies (case-sensitive)'),

        -- UID2 token (UPPER + TRIM; UID2 is base64url, treat as case-sensitive trim)
        ('MARTECH_UID2_STD', 'UID2', 'UID2 Trim',
         'TRIM({col})',
         'Trim UID2 tokens (preserve case)'),

        -- RampID (preserve case, trim)
        ('MARTECH_RAMPID_STD', 'RAMPID', 'RampID Trim',
         'TRIM({col})',
         'Trim RampIDs (preserve case)'),

        -- Device ID (preserve case for IDFA/GAID, trim)
        ('MARTECH_DEVICE_ID_STD', 'DEVICE_ID', 'Device ID Trim',
         'TRIM({col})',
         'Trim device IDs (IDFA/GAID/cookie; preserve case)')
) AS s
ON t.RULE_ID = s.RULE_ID
WHEN MATCHED THEN UPDATE SET
    t.SEMANTIC_CATEGORY = s.SEMANTIC_CATEGORY,
    t.RULE_NAME = s.RULE_NAME,
    t.SQL_EXPRESSION = s.SQL_EXPRESSION,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (RULE_ID, SEMANTIC_CATEGORY, RULE_NAME, SQL_EXPRESSION, DESCRIPTION)
    VALUES (s.RULE_ID, s.SEMANTIC_CATEGORY, s.RULE_NAME, s.SQL_EXPRESSION, s.DESCRIPTION);
