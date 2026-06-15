-- ============================================================================
-- Martech: Individual ML Blocking Stop-list Seed
-- Static stop-list per requirements.md FR-6, FR-7. Idempotent MERGE.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STOPLIST') (
    STOPLIST_ID    NUMBER AUTOINCREMENT,
    ATTRIBUTE      VARCHAR NOT NULL,    -- 'EMAIL_HANDLE' | 'PHONE_AREA_CODE' | 'PHONE_LAST7'
    PATTERN_TYPE   VARCHAR NOT NULL,    -- 'EXACT' | 'STARTS_WITH' | 'CONTAINS' | 'ALL_SAME' | 'SEQUENTIAL'
    PATTERN        VARCHAR NOT NULL DEFAULT '_ANY_',  -- '_ANY_' for ALL_SAME / SEQUENTIAL (proc applies algorithmically)
    REASON         VARCHAR,
    IS_ACTIVE      BOOLEAN DEFAULT TRUE,
    CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ATTRIBUTE, PATTERN_TYPE, PATTERN)
);

MERGE INTO IDENTIFIER('&{deployment_db}.CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STOPLIST') AS t
USING (
    SELECT column1 AS ATTRIBUTE, column2 AS PATTERN_TYPE, column3 AS PATTERN, column4 AS REASON
    FROM VALUES
        -- ─── Email handle stop-list (post-normalization, alpha-only lowercase) ──
        ('EMAIL_HANDLE','EXACT','info','role inbox'),
        ('EMAIL_HANDLE','EXACT','admin','role inbox'),
        ('EMAIL_HANDLE','EXACT','contact','role inbox'),
        ('EMAIL_HANDLE','EXACT','sales','role inbox'),
        ('EMAIL_HANDLE','EXACT','support','role inbox'),
        ('EMAIL_HANDLE','EXACT','help','role inbox'),
        ('EMAIL_HANDLE','EXACT','hello','role inbox'),
        ('EMAIL_HANDLE','EXACT','hi','role inbox'),
        ('EMAIL_HANDLE','EXACT','mail','role inbox'),
        ('EMAIL_HANDLE','EXACT','noreply','automated sender'),
        ('EMAIL_HANDLE','EXACT','postmaster','role inbox'),
        ('EMAIL_HANDLE','EXACT','webmaster','role inbox'),
        ('EMAIL_HANDLE','EXACT','abuse','role inbox'),
        ('EMAIL_HANDLE','EXACT','marketing','role inbox'),
        ('EMAIL_HANDLE','EXACT','team','role inbox'),
        ('EMAIL_HANDLE','EXACT','office','role inbox'),
        ('EMAIL_HANDLE','EXACT','hr','role inbox'),
        ('EMAIL_HANDLE','EXACT','accounts','role inbox'),
        ('EMAIL_HANDLE','EXACT','accounting','role inbox'),
        ('EMAIL_HANDLE','EXACT','billing','role inbox'),
        ('EMAIL_HANDLE','EXACT','test','test/demo'),
        ('EMAIL_HANDLE','EXACT','example','test/demo'),
        ('EMAIL_HANDLE','EXACT','demo','test/demo'),
        ('EMAIL_HANDLE','STARTS_WITH','noreply','noreply prefix variants'),
        ('EMAIL_HANDLE','CONTAINS','dontreply','defensive'),

        -- ─── Phone stop-list ────────────────────────────────────────────────────
        ('PHONE_AREA_CODE','EXACT','800','toll-free'),
        ('PHONE_AREA_CODE','EXACT','833','toll-free'),
        ('PHONE_AREA_CODE','EXACT','844','toll-free'),
        ('PHONE_AREA_CODE','EXACT','855','toll-free'),
        ('PHONE_AREA_CODE','EXACT','866','toll-free'),
        ('PHONE_AREA_CODE','EXACT','877','toll-free'),
        ('PHONE_AREA_CODE','EXACT','888','toll-free'),
        ('PHONE_AREA_CODE','EXACT','900','premium-rate'),

        ('PHONE_LAST7','EXACT','0000000','zero placeholder'),
        ('PHONE_LAST7','EXACT','9999999','nine placeholder'),
        ('PHONE_LAST7','STARTS_WITH','555010','NANP fictional/test range 555-01XX'),

        -- Pattern flags (PATTERN='_ANY_' sentinel — proc applies the pattern algorithmically)
        ('PHONE_LAST7','ALL_SAME','_ANY_','all 7 digits identical (1111111, 2222222, ...)'),
        ('PHONE_LAST7','SEQUENTIAL','_ANY_','arithmetic sequence ascending or descending')
) AS s
ON t.ATTRIBUTE = s.ATTRIBUTE
   AND t.PATTERN_TYPE = s.PATTERN_TYPE
   AND t.PATTERN = s.PATTERN
WHEN MATCHED THEN UPDATE SET
    t.REASON    = s.REASON,
    t.IS_ACTIVE = TRUE
WHEN NOT MATCHED THEN INSERT
    (ATTRIBUTE, PATTERN_TYPE, PATTERN, REASON, IS_ACTIVE)
    VALUES (s.ATTRIBUTE, s.PATTERN_TYPE, s.PATTERN, s.REASON, TRUE);
