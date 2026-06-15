-- ============================================================================
-- Martech: Individual ML Blocking Strategies Seed
-- v1 portfolio of 4 strategies (B5/B6/B7/B8) per requirements.md FR-4.
-- Idempotent CREATE TABLE IF NOT EXISTS + MERGE.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES') (
    STRATEGY_ID            VARCHAR PRIMARY KEY,
    STRATEGY_NAME          VARCHAR NOT NULL,
    STRATEGY_DESCRIPTION   VARCHAR,
    BLOCK_KEY_EXPR         VARCHAR NOT NULL,           -- SQL fragment over a STD-row alias 'r'
    PREREQUISITE_FIELDS    ARRAY,                      -- documentation only
    PREREQUISITE_PREDICATE VARCHAR,                    -- SQL fragment, applied as WHERE clause
    MAX_BLOCK_SIZE         NUMBER DEFAULT 200,
    PRIORITY               NUMBER,
    IS_ACTIVE              BOOLEAN DEFAULT TRUE,
    CREATED_AT             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT             TIMESTAMP_NTZ
);

MERGE INTO IDENTIFIER('&{deployment_db}.CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES') AS t
USING (
    SELECT column1 AS STRATEGY_ID,
           column2 AS STRATEGY_NAME,
           column3 AS STRATEGY_DESCRIPTION,
           column4 AS BLOCK_KEY_EXPR,
           PARSE_JSON(column5) AS PREREQUISITE_FIELDS,
           column6 AS PREREQUISITE_PREDICATE,
           column7 AS MAX_BLOCK_SIZE,
           column8 AS PRIORITY,
           column9 AS IS_ACTIVE
    FROM VALUES
        ('B5',
         'EMAIL_HANDLE',
         'Same email handle across providers + gmail dot/plus variants + alpha-only collapse. Catches mlopez@gmail.com ↔ mlopez@yahoo.com.',
         'r.EMAIL_HANDLE_NORMALIZED',
         '["EMAIL_HANDLE_NORMALIZED","IS_BLOCKABLE_EMAIL"]',
         'r.EMAIL_HANDLE_NORMALIZED IS NOT NULL AND r.EMAIL_HANDLE_NORMALIZED <> '''' AND r.IS_BLOCKABLE_EMAIL = TRUE',
         200, 10, TRUE),

        ('B6',
         'POSTAL_SOUNDEX_LNAME',
         'Lastname typos at the same postal code. Catches Lopez ↔ Lopaz, Smith ↔ Smyth.',
         'r.POSTAL_CODE_STD || ''|'' || r.SOUNDEX_LNAME',
         '["POSTAL_CODE_STD","SOUNDEX_LNAME"]',
         'r.POSTAL_CODE_STD IS NOT NULL AND r.SOUNDEX_LNAME IS NOT NULL',
         200, 20, TRUE),

        ('B7',
         'PHONE_LAST7',
         'Phones with malformed prefixes (missing country code) + records with phone but no email/postal.',
         'r.PHONE_LAST7',
         '["PHONE_LAST7","IS_BLOCKABLE_PHONE"]',
         'r.PHONE_LAST7 IS NOT NULL AND r.IS_BLOCKABLE_PHONE = TRUE',
         200, 30, TRUE),

        ('B8',
         'NAME_STATE',
         'Cross-zip moves with stable name (within same state).',
         'r.FIRST_NAME_CANONICAL || ''|'' || r.LAST_NAME_STD || ''|'' || r.STATE_STD',
         '["FIRST_NAME_CANONICAL","LAST_NAME_STD","STATE_STD"]',
         'r.FIRST_NAME_CANONICAL IS NOT NULL AND r.LAST_NAME_STD IS NOT NULL AND r.STATE_STD IS NOT NULL',
         200, 40, TRUE)
) AS s
ON t.STRATEGY_ID = s.STRATEGY_ID
WHEN MATCHED THEN UPDATE SET
    t.STRATEGY_NAME          = s.STRATEGY_NAME,
    t.STRATEGY_DESCRIPTION   = s.STRATEGY_DESCRIPTION,
    t.BLOCK_KEY_EXPR         = s.BLOCK_KEY_EXPR,
    t.PREREQUISITE_FIELDS    = s.PREREQUISITE_FIELDS,
    t.PREREQUISITE_PREDICATE = s.PREREQUISITE_PREDICATE,
    t.MAX_BLOCK_SIZE         = s.MAX_BLOCK_SIZE,
    t.PRIORITY               = s.PRIORITY,
    t.IS_ACTIVE              = s.IS_ACTIVE,
    t.UPDATED_AT             = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (STRATEGY_ID, STRATEGY_NAME, STRATEGY_DESCRIPTION, BLOCK_KEY_EXPR,
     PREREQUISITE_FIELDS, PREREQUISITE_PREDICATE, MAX_BLOCK_SIZE, PRIORITY, IS_ACTIVE)
    VALUES (s.STRATEGY_ID, s.STRATEGY_NAME, s.STRATEGY_DESCRIPTION, s.BLOCK_KEY_EXPR,
            s.PREREQUISITE_FIELDS, s.PREREQUISITE_PREDICATE, s.MAX_BLOCK_SIZE, s.PRIORITY, s.IS_ACTIVE);
