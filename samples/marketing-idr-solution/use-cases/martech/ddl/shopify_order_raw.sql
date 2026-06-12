-- ============================================================================
-- Martech: BRONZE.SHOPIFY_ORDER_RAW
-- Shopify Order REST shape stored as VARIANT; flattened in silver via
-- STD_SHOPIFY_ORDER_RAW. One row per order.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.SHOPIFY_ORDER_RAW') (
    ORDER_ID         VARCHAR,
    ORDER_NUMBER     NUMBER,
    CREATED_AT_SRC   TIMESTAMP_NTZ,
    TOTAL_PRICE      NUMBER(12,2),
    FINANCIAL_STATUS VARCHAR,
    RAW_PAYLOAD      VARIANT,
    SOURCE_FILE      VARCHAR,
    INGESTED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) WITH TAG (
    IDENTIFIER('&{deployment_db}.SILVER.IDR_INCLUDE')='TRUE',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIMARY_KEY')='ORDER_ID',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIORITY')='4',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_SOURCE_ROLE')='USE_CASE'
);

CREATE STREAM IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.SHOPIFY_ORDER_STREAM')
    ON TABLE IDENTIFIER('&{deployment_db}.BRONZE.SHOPIFY_ORDER_RAW')
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE;
