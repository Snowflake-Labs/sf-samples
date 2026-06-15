-- ============================================================================
-- Martech: SHOPIFY column metadata seed
--
-- Why this exists:
--   SP_REFRESH_METADATA_CACHE discovers identifier columns by scanning
--   INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS on BRONZE tables. Bronze
--   SHOPIFY_ORDER_RAW has only RAW_PAYLOAD VARIANT — no flat columns to tag.
--   Without these rows, SP_EXTRACT_IDENTIFIERS produces 0 rows for shopify
--   and every shopify-sourced record is invisible to deterministic matching.
--
-- This seed populates the cache directly, pointing at the flat columns that
-- standardize produces on STD_SHOPIFY_ORDER_RAW. Idempotent (MERGE) so it can
-- run after every SP_REFRESH_METADATA_CACHE call without duplication.
-- ============================================================================

USE WAREHOUSE MARTECH_WH;

MERGE INTO MARTECH.SILVER.IDR_CORE_COLUMN_METADATA_CACHE t
USING (
    SELECT * FROM VALUES
        ('MARTECH.BRONZE.SHOPIFY_ORDER_RAW', 'SHOPIFY_ORDER_RAW', 'EMAIL',                 'EMAIL',       NULL,                NULL),
        ('MARTECH.BRONZE.SHOPIFY_ORDER_RAW', 'SHOPIFY_ORDER_RAW', 'PHONE',                 'PHONE',       NULL,                NULL),
        ('MARTECH.BRONZE.SHOPIFY_ORDER_RAW', 'SHOPIFY_ORDER_RAW', 'FIRST_NAME',            'NAME_FIRST',  NULL,                NULL),
        ('MARTECH.BRONZE.SHOPIFY_ORDER_RAW', 'SHOPIFY_ORDER_RAW', 'LAST_NAME',             'NAME_LAST',   NULL,                NULL),
        ('MARTECH.BRONZE.SHOPIFY_ORDER_RAW', 'SHOPIFY_ORDER_RAW', 'BILLING_POSTAL_CODE',   'POSTAL_CODE', NULL,                NULL),
        ('MARTECH.BRONZE.SHOPIFY_ORDER_RAW', 'SHOPIFY_ORDER_RAW', 'LOYALTY_MEMBER_NUMBER', 'LOYALTY_ID',  NULL,                NULL),
        ('MARTECH.BRONZE.SHOPIFY_ORDER_RAW', 'SHOPIFY_ORDER_RAW', 'SHOPIFY_CUSTOMER_ID',   NULL,          'SHOPIFY_CUSTOMER',  NULL)
    AS s(TABLE_FQN, TABLE_NAME, COLUMN_NAME, IDENTIFIER_TYPE, EXTERNAL_KEY_TO, SEMANTIC_CATEGORY)
) s
ON  t.TABLE_NAME  = s.TABLE_NAME
AND t.COLUMN_NAME = s.COLUMN_NAME
WHEN MATCHED THEN UPDATE SET
    t.TABLE_FQN         = s.TABLE_FQN,
    t.IDENTIFIER_TYPE   = s.IDENTIFIER_TYPE,
    t.EXTERNAL_KEY_TO   = s.EXTERNAL_KEY_TO,
    t.SEMANTIC_CATEGORY = s.SEMANTIC_CATEGORY,
    t.CACHED_AT         = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (TABLE_FQN, TABLE_NAME, COLUMN_NAME, IDENTIFIER_TYPE, EXTERNAL_KEY_TO, SEMANTIC_CATEGORY, CACHED_AT)
VALUES
    (s.TABLE_FQN, s.TABLE_NAME, s.COLUMN_NAME, s.IDENTIFIER_TYPE, s.EXTERNAL_KEY_TO, s.SEMANTIC_CATEGORY, CURRENT_TIMESTAMP());
