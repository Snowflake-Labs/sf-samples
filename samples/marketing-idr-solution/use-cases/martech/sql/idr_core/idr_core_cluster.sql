-- ============================================================================
-- Martech: SILVER.IDR_CORE_CLUSTER
-- Resolved identity clusters; written by engine SP_UPDATE_CLUSTERS.
-- DDL copied from use-cases/ssp/deploy/03_ssp_tables.sql with DB qualifier
-- parameterized for martech.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_CLUSTER') (
    CLUSTER_ID         VARCHAR,
    MATCH_ID           VARCHAR,
    SOURCE_RECORD_IDS  VARIANT,
    IDENTIFIER_IDS     VARIANT,
    STATUS             VARCHAR,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    MERGED_INTO        VARCHAR,
    UPDATED_AT         TIMESTAMP_NTZ,
    BEST_DOB           DATE,
    BEST_LAST_NAME     VARCHAR,
    BEST_EMAIL         VARCHAR,
    BEST_FIRST_NAME    VARCHAR,
    BEST_PHONE         VARCHAR,
    EMAIL_DOMAIN       VARCHAR,
    BEST_CITY          VARCHAR,
    LAST_NAME_SOUNDEX  VARCHAR,
    BEST_ZIP           VARCHAR,
    FIRST_INITIAL      VARCHAR(1),
    DOB_MMDD           VARCHAR,
    EMAIL_LOCAL_PART   VARCHAR,
    ZIP3               VARCHAR,
    PHONE_LAST4        VARCHAR,
    PHONE_LAST7        VARCHAR,
    BLOCK_DOMAIN_SOUNDEX               VARCHAR,
    DOB_YYMM                           VARCHAR,
    PROFILE_UPDATED_AT                 TIMESTAMP_NTZ,
    BLOCK_PHONE7_SOUNDEX               VARCHAR,
    BLOCK_ZIP3_SOUNDEX_FI              VARCHAR,
    BLOCK_DOB_MMDD_SOUNDEX             VARCHAR(50),
    BLOCK_EMAIL_LOCAL_DOB_YYMM         VARCHAR,
    BLOCK_EMAIL_LOCAL_DOB_YYMM_SOUNDEX VARCHAR,
    BLOCK_EMAIL_LOCAL_DOB_MMDD_SOUNDEX VARCHAR,
    BLOCK_EMAIL_LOCAL_DOB_SOUNDEX      VARCHAR
);

ALTER TABLE IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_CLUSTER') CLUSTER BY (CLUSTER_ID);
