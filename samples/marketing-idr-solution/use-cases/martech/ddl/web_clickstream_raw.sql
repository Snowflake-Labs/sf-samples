-- ============================================================================
-- Martech: BRONZE.WEB_CLICKSTREAM_RAW
-- Snowplow / GA4-shaped web events. Mostly anonymous; logged-in fields
-- populate when the user authenticates within a session.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.WEB_CLICKSTREAM_RAW') (
    EVENT_ID              VARCHAR,
    TS                    TIMESTAMP_NTZ,
    ANONYMOUS_ID          VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='COOKIE_ID'),
    DEVICE_ID             VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='DEVICE_ID'),
    UID2                  VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='UID2'),
    RAMPID                VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='RAMPID'),
    SESSION_ID            VARCHAR,
    USER_AGENT            VARCHAR,
    IP                    VARCHAR,
    EVENT_NAME            VARCHAR,
    PAGE_URL              VARCHAR,
    LOGGED_IN_EMAIL       VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='EMAIL'),
    LOGGED_IN_PHONE       VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='PHONE'),
    LOGGED_IN_MEMBER_ID   VARCHAR WITH TAG (IDENTIFIER('&{deployment_db}.SILVER.IDR_IDENTIFIER')='LOYALTY_ID'),
    EVENT_PROPERTIES_JSON VARIANT,
    SOURCE_FILE           VARCHAR,
    INGESTED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) WITH TAG (
    IDENTIFIER('&{deployment_db}.SILVER.IDR_INCLUDE')='TRUE',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIMARY_KEY')='EVENT_ID',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_PRIORITY')='3',
    IDENTIFIER('&{deployment_db}.SILVER.IDR_SOURCE_ROLE')='USE_CASE'
);

CREATE STREAM IF NOT EXISTS IDENTIFIER('&{deployment_db}.BRONZE.WEB_CLICKSTREAM_STREAM')
    ON TABLE IDENTIFIER('&{deployment_db}.BRONZE.WEB_CLICKSTREAM_RAW')
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE;
