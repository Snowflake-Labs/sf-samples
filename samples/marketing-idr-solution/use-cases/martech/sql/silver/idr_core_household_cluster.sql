-- ============================================================================
-- Martech: SILVER.IDR_CORE_HOUSEHOLD_CLUSTER + MEMBERSHIP + MATCH_RESULTS
--
-- Household-grain identity space. A "household" is the transitive closure of
-- HOUSEHOLD-tier matching rules (R14, R15, ...) over INDIVIDUAL clusters
-- produced by the engine's SP_UPDATE_CLUSTERS. Members are individual
-- cluster_ids — NOT raw source records.
--
-- Architecture mirrors the individual-grain engine:
--   IDR_CORE_HOUSEHOLD_MATCH_RESULTS  <-> IDR_CORE_MATCH_RESULTS
--   IDR_CORE_HOUSEHOLD_MEMBERSHIP     <-> IDR_CORE_CLUSTER_MEMBERSHIP
--   IDR_CORE_HOUSEHOLD_CLUSTER        <-> IDR_CORE_CLUSTER
--
-- Same column shape as engine match_results so future audit / lineage tooling
-- that consumes match-results semantics can be reused without forking.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_HOUSEHOLD_CLUSTER') (
    HOUSEHOLD_ID         VARCHAR PRIMARY KEY,
    HOUSEHOLD_TYPE       VARCHAR,                  -- 'SAME_SURNAME' | 'FUZZY_SURNAME'
    SHARED_STREET        VARCHAR,
    SHARED_CITY          VARCHAR,
    SHARED_STATE         VARCHAR,
    SHARED_POSTAL        VARCHAR,
    MEMBER_COUNT         NUMBER,
    PRIMARY_LAST_NAME    VARCHAR,
    STATUS               VARCHAR DEFAULT 'ACTIVE',
    FIRST_SEEN           TIMESTAMP_NTZ,
    LAST_SEEN            TIMESTAMP_NTZ,
    CREATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT           TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP') (
    HOUSEHOLD_ID            VARCHAR,
    INDIVIDUAL_CLUSTER_ID   VARCHAR,                -- FK to IDR_CORE_CLUSTER.cluster_id
    JOINED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (HOUSEHOLD_ID, INDIVIDUAL_CLUSTER_ID)
);

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS') (
    MATCH_ID                 VARCHAR DEFAULT UUID_STRING(),
    A_CLUSTER_ID             VARCHAR,                -- analog of NEW_SOURCE_RECORD_ID at cluster grain
    B_CLUSTER_ID             VARCHAR,                -- analog of MATCHED_SOURCE_RECORD_ID at cluster grain
    RULE_ID                  VARCHAR,                -- 'MARTECH_R14', 'MARTECH_R15', ...
    RULE_NAME                VARCHAR,
    MATCH_SCORE              FLOAT,                  -- base_score for non-fuzzy; JW value for fuzzy
    JW_LAST                  FLOAT,                  -- raw JW value when fuzzy_match_field on NAME_LAST
    MATCHED_ON               VARCHAR,                -- shared anchor value (e.g., postal code)
    IS_ACTIVE                BOOLEAN DEFAULT TRUE,
    IS_CURRENT               BOOLEAN DEFAULT TRUE,
    SUPERSEDED_BY            VARCHAR,
    CREATED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT               TIMESTAMP_NTZ,
    RUN_ID                   VARCHAR,
    PIPELINE_RUN_ID          VARCHAR
);
