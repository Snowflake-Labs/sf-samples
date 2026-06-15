-- ============================================================================
-- Martech: BRONZE.POS_TRANSACTION_RAW
-- In-store POS transactions; loyalty card scanned ~40% of the time
-- DB qualifier resolved via &{deployment_db} at deploy time (default: MARTECH)
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.POS_TRANSACTION_RAW') (
    TXN_ID                VARCHAR,
    STORE_ID              VARCHAR,
    TS                    TIMESTAMP_NTZ,
    TENDER_TYPE           VARCHAR,
    TOTAL                 NUMBER(12,2),
    LOYALTY_MEMBER_NUMBER VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='LOYALTY_ID'),
    CUSTOMER_EMAIL        VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='EMAIL'),
    CUSTOMER_PHONE        VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='PHONE'),
    CUSTOMER_FIRST_NAME   VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='NAME_FIRST'),
    CUSTOMER_LAST_NAME    VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='NAME_LAST'),
    LINE_ITEMS_JSON       VARIANT,
    SOURCE_FILE           VARCHAR,
    INGESTED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) WITH TAG (
    IDENTIFIER('&{deployment_db}.SILVER.IDR_INCLUDE')='TRUE',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIMARY_KEY')='TXN_ID',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIORITY')='1',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_SOURCE_ROLE')='BASE'
);

-- Append-only stream feeding the IDR_INCREMENTAL_TASK
CREATE STREAM IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.POS_TRANSACTION_STREAM')
    ON TABLE IDENTIFIER('&{deployment_db}.BRONZE.POS_TRANSACTION_RAW')
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE;
