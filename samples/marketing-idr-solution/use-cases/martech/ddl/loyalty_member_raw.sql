-- ============================================================================
-- Martech: BRONZE.LOYALTY_MEMBER_RAW
-- Loyalty platform customer dimension; updated daily
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.LOYALTY_MEMBER_RAW') (
    MEMBER_ID       VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='LOYALTY_ID'),
    EMAIL           VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='EMAIL'),
    PHONE           VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='PHONE'),
    FIRST_NAME      VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='NAME_FIRST'),
    LAST_NAME       VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='NAME_LAST'),
    STREET_ADDRESS  VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='STREET_ADDRESS'),
    CITY            VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='CITY'),
    STATE           VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='STATE'),
    POSTAL_CODE     VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='POSTAL_CODE'),
    COUNTRY         VARCHAR,
    TIER            VARCHAR,
    POINTS          NUMBER,
    ENROLLED_AT     TIMESTAMP_NTZ,
    SOURCE_FILE     VARCHAR,
    INGESTED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) WITH TAG (
    IDENTIFIER('&{deployment_db}.SILVER.IDR_INCLUDE')='TRUE',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIMARY_KEY')='MEMBER_ID',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIORITY')='2',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_SOURCE_ROLE')='BASE'
);

CREATE STREAM IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.LOYALTY_MEMBER_STREAM')
    ON TABLE IDENTIFIER('&{deployment_db}.BRONZE.LOYALTY_MEMBER_RAW')
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE;
