-- ============================================================================
-- Martech: Source Priority + IDR Source Config Seed
-- Loads <DB>.CONFIG.SOURCE_PRIORITY and <DB>.CONFIG.IDR_SOURCE_CONFIG
-- (consumed by engine when picking the "best" source row to seed a cluster's
-- golden record).
--
-- Order rationale:
--   1 POS_TRANSACTION   — first-party, in-store transaction; loyalty-confirmed
--                         records highest fidelity
--   2 LOYALTY_MEMBER    — first-party, customer dimension; canonical address/tier
--   3 SHOPIFY_ORDER     — first-party online; always email-anchored
--   4 WEB_CLICKSTREAM   — anonymous-leaning behavioral signals; lowest priority
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.SOURCE_PRIORITY') (
    SOURCE_SYSTEM VARCHAR,
    PRIORITY      INTEGER,
    DESCRIPTION   VARCHAR,
    IS_ACTIVE     BOOLEAN DEFAULT TRUE,
    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_SOURCE_CONFIG') (
    SOURCE_TABLE_NAME  VARCHAR,
    PRIMARY_KEY_COLUMN VARCHAR,
    FRIENDLY_NAME      VARCHAR,
    SOURCE_PREFIX      VARCHAR,
    SOURCE_PRIORITY    INTEGER,
    HAS_LOCATION       BOOLEAN DEFAULT FALSE,
    LOCATION_COLUMNS   VARIANT,
    IS_ACTIVE          BOOLEAN DEFAULT TRUE,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

TRUNCATE TABLE IDENTIFIER('&{deployment_db}.CONFIG.SOURCE_PRIORITY');
INSERT INTO IDENTIFIER('&{deployment_db}.CONFIG.SOURCE_PRIORITY')
(SOURCE_SYSTEM, PRIORITY, DESCRIPTION, IS_ACTIVE, CREATED_AT)
VALUES
('POS_TRANSACTION', 1, 'In-store POS transactions; loyalty-card-scanned records carry strongest 1P confirmation', TRUE, CURRENT_TIMESTAMP()),
('LOYALTY_MEMBER',  2, 'Loyalty platform customer dimension; canonical name/address/phone/email/tier', TRUE, CURRENT_TIMESTAMP()),
('SHOPIFY_ORDER',   3, 'Online order with email + billing/shipping address; strong web-side anchor', TRUE, CURRENT_TIMESTAMP()),
('WEB_CLICKSTREAM', 4, 'Snowplow / GA4 events; mostly anonymous, periodically logged-in', TRUE, CURRENT_TIMESTAMP());

TRUNCATE TABLE IDENTIFIER('&{deployment_db}.CONFIG.IDR_SOURCE_CONFIG');
INSERT INTO IDENTIFIER('&{deployment_db}.CONFIG.IDR_SOURCE_CONFIG')
(SOURCE_TABLE_NAME, PRIMARY_KEY_COLUMN, FRIENDLY_NAME, SOURCE_PREFIX,
 SOURCE_PRIORITY, HAS_LOCATION, LOCATION_COLUMNS, IS_ACTIVE, CREATED_AT)
SELECT 'POS_TRANSACTION_RAW', 'TXN_ID', 'POS Transaction', 'POS', 1,
       FALSE, NULL, TRUE, CURRENT_TIMESTAMP()
UNION ALL
SELECT 'LOYALTY_MEMBER_RAW', 'MEMBER_ID', 'Loyalty Member', 'LOY', 2,
       TRUE, PARSE_JSON('{"street":"STREET_ADDRESS","city":"CITY","state":"STATE","zip":"POSTAL_CODE","country":"COUNTRY"}'),
       TRUE, CURRENT_TIMESTAMP()
UNION ALL
SELECT 'SHOPIFY_ORDER_RAW', 'ORDER_ID', 'Shopify Order', 'SHO', 3,
       TRUE, PARSE_JSON('{"street":"BILLING_STREET_ADDRESS","city":"BILLING_CITY","state":"BILLING_STATE","zip":"BILLING_POSTAL_CODE","country":"BILLING_COUNTRY"}'),
       TRUE, CURRENT_TIMESTAMP()
UNION ALL
SELECT 'WEB_CLICKSTREAM_RAW', 'EVENT_ID', 'Web Clickstream', 'WEB', 4,
       FALSE, NULL, TRUE, CURRENT_TIMESTAMP();
