-- ============================================================================
-- Martech: Matching Rules Seed
-- 17 MARTECH_R* rules (per requirements.md FR-3) loaded into
-- <DB>.CONFIG.IDR_MATCHING_RULES (consumed by engine SP_RUN_MATCHING).
--
-- Idempotent: TRUNCATE + INSERT pattern (mirrors SSP).
-- DB qualifier resolved via &{deployment_db} at deploy time.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_MATCHING_RULES') (
    RULE_ID                  VARCHAR PRIMARY KEY,
    RULE_NAME                VARCHAR,
    RULE_DESCRIPTION         VARCHAR,
    RULE_PRIORITY            INTEGER,
    MATCH_TYPE               VARCHAR,
    ANCHOR_FIELD             VARCHAR,
    EXACT_MATCH_FIELDS       VARCHAR,
    FUZZY_MATCH_FIELD        VARCHAR,
    FUZZY_ALGORITHM          VARCHAR DEFAULT 'NONE',
    FUZZY_THRESHOLD          FLOAT,
    BASE_SCORE               FLOAT DEFAULT 1.0,
    REQUIRE_CROSS_SOURCE     BOOLEAN DEFAULT FALSE,
    REQUIRE_DIFFERENT_RECORD BOOLEAN DEFAULT TRUE,
    IS_ACTIVE                BOOLEAN DEFAULT TRUE,
    TARGET                   VARCHAR DEFAULT 'INDIVIDUAL',  -- 'INDIVIDUAL' | 'HOUSEHOLD' (future track)
    CREATED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- v1: idempotent column migration is owned by engine/sql/migrations/v1_idr_adjudication.sql
-- (runs as part of every use-case deploy_all.sh).

TRUNCATE TABLE IDENTIFIER('&{deployment_db}.CONFIG.IDR_MATCHING_RULES');

INSERT INTO IDENTIFIER('&{deployment_db}.CONFIG.IDR_MATCHING_RULES')
(RULE_ID, RULE_NAME, RULE_DESCRIPTION, RULE_PRIORITY, MATCH_TYPE, ANCHOR_FIELD,
 EXACT_MATCH_FIELDS, FUZZY_MATCH_FIELD, FUZZY_ALGORITHM, FUZZY_THRESHOLD,
 BASE_SCORE, REQUIRE_CROSS_SOURCE, REQUIRE_DIFFERENT_RECORD, IS_ACTIVE, CREATED_AT)
VALUES

-- ─── Single-field deterministic ─────────────────────────────────────────────
('MARTECH_R01', 'Email Exact',
 'Two records share the same lowercased+trimmed email — deterministic. Priority sits between R11 and R12 so the richer email-family rules (R08/R10/R11) win pair-level dedup when they fire (same email + name signals); R01 is the email-anchor fallback when only email matches.',
 115, 'DETERMINISTIC', 'EMAIL', NULL, NULL, 'NONE', NULL,
 1.00, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R02', 'HEM Exact',
 'Two records share the same SHA-256 hashed email — deterministic. INACTIVE: extractor does not currently emit a separate EMAIL_HEM identifier_type; functionally redundant with R01 in this demo.',
 20, 'DETERMINISTIC', 'EMAIL_HEM', NULL, NULL, 'NONE', NULL,
 1.00, FALSE, TRUE, FALSE, CURRENT_TIMESTAMP()),

('MARTECH_R03', 'Loyalty Member # Exact',
 'Two records share the same loyalty member number — first-party deterministic',
 30, 'DETERMINISTIC', 'LOYALTY_ID', NULL, NULL, 'NONE', NULL,
 1.00, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R04', 'Device ID Exact',
 'Two records share the same device ID (cookie / IDFA / GAID)',
 40, 'DETERMINISTIC', 'DEVICE_ID', NULL, NULL, 'NONE', NULL,
 0.95, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R05', 'UID2 Exact',
 'Two records share the same UID2 token — addressable deterministic',
 50, 'DETERMINISTIC', 'UID2', NULL, NULL, 'NONE', NULL,
 1.00, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R06', 'RampID Exact',
 'Two records share the same LiveRamp RampID — deterministic',
 60, 'DETERMINISTIC', 'RAMPID', NULL, NULL, 'NONE', NULL,
 1.00, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

-- ─── Composite name + email/phone ───────────────────────────────────────────
('MARTECH_R07', 'Full Name + Phone',
 'Anchor on phone, exact match on first + last name — deterministic',
 70, 'DETERMINISTIC', 'PHONE', 'NAME_FIRST,NAME_LAST', NULL, 'NONE', NULL,
 0.96, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R08', 'Nickname First + Email',
 'Anchor on email, exact last name, nickname-canonical first name (Bob<->Robert)',
 80, 'DETERMINISTIC', 'EMAIL', 'NAME_LAST', 'FIRST_NAME_CANONICAL', 'CANONICAL', NULL,
 0.95, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R09', 'Nickname First + Phone',
 'Anchor on phone, exact last name, nickname-canonical first name',
 90, 'DETERMINISTIC', 'PHONE', 'NAME_LAST', 'FIRST_NAME_CANONICAL', 'CANONICAL', NULL,
 0.93, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R10', 'Fuzzy First + Email',
 'Anchor on email, exact last name, Jaro-Winkler match on first name >= 0.92',
 100, 'DETERMINISTIC', 'EMAIL', 'NAME_LAST', 'NAME_FIRST', 'JARO_WINKLER', 0.92,
 0.93, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R11', 'First + Fuzzy Last + Email',
 'Anchor on email, exact first name, Jaro-Winkler match on last name >= 0.92',
 110, 'DETERMINISTIC', 'EMAIL', 'NAME_FIRST', 'NAME_LAST', 'JARO_WINKLER', 0.92,
 0.93, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R12', 'Fuzzy First + Phone',
 'Anchor on phone, exact last name, Jaro-Winkler match on first name >= 0.92',
 120, 'DETERMINISTIC', 'PHONE', 'NAME_LAST', 'NAME_FIRST', 'JARO_WINKLER', 0.92,
 0.91, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R13', 'First + Fuzzy Last + Phone',
 'Anchor on phone, exact first name, Jaro-Winkler match on last name >= 0.92',
 130, 'DETERMINISTIC', 'PHONE', 'NAME_FIRST', 'NAME_LAST', 'JARO_WINKLER', 0.92,
 0.91, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

-- ─── Household / address ────────────────────────────────────────────────────
('MARTECH_R14', 'Last Name + Full Address',
 'Anchor on postal code, exact last name + street + city + state — household-level deterministic. Run by SP_RUN_HOUSEHOLD_MATCHING (cluster grain), excluded from individual matching.',
 140, 'HOUSEHOLD', 'POSTAL_CODE', 'NAME_LAST,STREET_ADDRESS,CITY,STATE', NULL, 'NONE', NULL,
 0.90, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R15', 'Fuzzy Last Name + Full Address',
 'Anchor on postal, exact street/city/state, Jaro-Winkler last name >= 0.92. Run by SP_RUN_HOUSEHOLD_MATCHING (cluster grain), excluded from individual matching.',
 150, 'HOUSEHOLD', 'POSTAL_CODE', 'STREET_ADDRESS,CITY,STATE', 'NAME_LAST', 'JARO_WINKLER', 0.92,
 0.85, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

-- ─── ML & LLM ───────────────────────────────────────────────────────────────
('MARTECH_R16', 'ML Candidate Score >= 0.85',
 'LightGBM-style scorer over name/address/phone fuzzy features; emits edges with score >= 0.85',
 160, 'ML', NULL, NULL, NULL, 'NONE', 0.85,
 0.85, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()),

('MARTECH_R17', 'LLM Adjudicated MATCH',
 'Cortex AI verdict = MATCH on candidate pairs in band [0.55, 0.85)',
 170, 'LLM', NULL, NULL, NULL, 'NONE', NULL,
 0.90, FALSE, TRUE, TRUE, CURRENT_TIMESTAMP());
