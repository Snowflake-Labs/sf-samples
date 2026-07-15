-- =============================================================================
-- META CAPI SCHEMA SETUP
-- Creates database, schema, and tables for Meta Conversions API pipeline
-- =============================================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS META_CAPI_DB;
CREATE SCHEMA IF NOT EXISTS META_CAPI_DB.PIPELINE;

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- EVENTS TABLE - Flexible schema supporting all Meta standard + custom events
-- Uses VARIANT columns for user_data and custom_data to support arbitrary fields
-- without schema changes when Meta adds new parameters.
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_CAPI_EVENTS (
    -- Primary Key (client-provided for deduplication, or auto-generated)
    EVENT_ID VARCHAR(100) NOT NULL,

    -- Server Event Parameters (Required)
    EVENT_NAME VARCHAR(50) NOT NULL,
    EVENT_TIME TIMESTAMP_NTZ NOT NULL,
    ACTION_SOURCE VARCHAR(20) NOT NULL,
    EVENT_SOURCE_URL VARCHAR(2000),

    -- User Data (pre-hashed PII + browser/device identifiers)
    -- Keys: em, ph, fn, ln, ge, db, ct, st, zp, country, external_id,
    --        client_ip_address, client_user_agent, fbc, fbp, + any future fields
    USER_DATA VARIANT,

    -- Custom Data (event-specific parameters)
    -- Keys: value, currency, content_ids, content_type, contents, num_items,
    --        search_string, order_id, predicted_ltv, delivery_category, + any future fields
    CUSTOM_DATA VARIANT,

    -- Processing Metadata
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSED_AT TIMESTAMP_NTZ,
    STATUS VARCHAR(20) DEFAULT 'PENDING',
    META_RESPONSE VARIANT,
    ERROR_MESSAGE VARCHAR(2000),
    RETRY_COUNT INTEGER DEFAULT 0,

    PRIMARY KEY (EVENT_ID)
);

-- =============================================================================
-- LOG TABLE - Track all API calls and responses
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_CAPI_LOG (
    LOG_ID NUMBER AUTOINCREMENT,
    BATCH_ID VARCHAR(100),
    EVENT_ID VARCHAR(100),
    STATUS VARCHAR(20),
    RESPONSE VARIANT,
    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (LOG_ID)
);

-- =============================================================================
-- CONFIG TABLE - Store pipeline configuration
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_CAPI_CONFIG (
    CONFIG_KEY VARCHAR(100) PRIMARY KEY,
    CONFIG_VALUE VARCHAR(2000),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- VALIDATION VIEW - Quick data quality check
-- =============================================================================
CREATE OR REPLACE VIEW V_CAPI_VALIDATION AS
SELECT 
    EVENT_ID,
    EVENT_NAME,
    CASE 
        WHEN EVENT_NAME IS NULL THEN 'Missing event_name'
        WHEN EVENT_TIME IS NULL THEN 'Missing event_time'
        WHEN ACTION_SOURCE IS NULL THEN 'Missing action_source'
        WHEN ACTION_SOURCE = 'website' AND EVENT_SOURCE_URL IS NULL THEN 'Website events require event_source_url'
        WHEN ACTION_SOURCE = 'website' AND USER_DATA:client_user_agent IS NULL THEN 'Website events require client_user_agent'
        WHEN EVENT_NAME IN ('Purchase', 'Donate', 'StartTrial', 'Subscribe') AND CUSTOM_DATA:value IS NULL THEN 'Value required for this event type'
        WHEN CUSTOM_DATA:value IS NOT NULL AND CUSTOM_DATA:currency IS NULL THEN 'Currency required when value is set'
        WHEN USER_DATA IS NULL OR (USER_DATA:em IS NULL AND USER_DATA:ph IS NULL AND USER_DATA:external_id IS NULL) THEN 'At least one user identifier required (em, ph, or external_id)'
        ELSE 'VALID'
    END AS VALIDATION_STATUS
FROM META_CAPI_EVENTS
WHERE STATUS = 'PENDING';

-- =============================================================================
-- INDEXES
-- =============================================================================
CREATE INDEX IF NOT EXISTS IDX_EVENTS_STATUS ON META_CAPI_EVENTS(STATUS);
CREATE INDEX IF NOT EXISTS IDX_EVENTS_CREATED ON META_CAPI_EVENTS(CREATED_AT);
CREATE INDEX IF NOT EXISTS IDX_EVENTS_NAME ON META_CAPI_EVENTS(EVENT_NAME);
CREATE INDEX IF NOT EXISTS IDX_LOG_BATCH ON META_CAPI_LOG(BATCH_ID);

-- =============================================================================
-- MIGRATION HELPER - Convert existing data from old schema to new
-- Run this ONCE if upgrading from the fixed-column schema
-- =============================================================================
-- CREATE TABLE META_CAPI_EVENTS_V2 AS
-- SELECT
--     EVENT_ID, EVENT_NAME, EVENT_TIME, ACTION_SOURCE, EVENT_SOURCE_URL,
--     OBJECT_CONSTRUCT_KEEP_NULL(
--         'em', EM, 'ph', PH, 'fn', FN, 'ln', LN,
--         'ge', GE, 'db', DB, 'ct', CT, 'st', ST, 'zp', ZP,
--         'country', COUNTRY, 'external_id', EXTERNAL_ID,
--         'client_ip_address', CLIENT_IP_ADDRESS,
--         'client_user_agent', CLIENT_USER_AGENT,
--         'fbc', FBC, 'fbp', FBP
--     ) AS USER_DATA,
--     OBJECT_CONSTRUCT_KEEP_NULL(
--         'value', VALUE, 'currency', CURRENCY,
--         'content_ids', CONTENT_IDS, 'content_type', CONTENT_TYPE,
--         'contents', CONTENTS, 'num_items', NUM_ITEMS,
--         'search_string', SEARCH_STRING, 'order_id', ORDER_ID,
--         'predicted_ltv', PREDICTED_LTV
--     ) AS CUSTOM_DATA,
--     CREATED_AT, PROCESSED_AT, STATUS, META_RESPONSE, ERROR_MESSAGE, RETRY_COUNT
-- FROM META_CAPI_EVENTS;
-- ALTER TABLE META_CAPI_EVENTS RENAME TO META_CAPI_EVENTS_OLD;
-- ALTER TABLE META_CAPI_EVENTS_V2 RENAME TO META_CAPI_EVENTS;
