-- =============================================================================
-- META CAPI DATA VALIDATION
-- Validate event data before sending to Meta
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- COMPREHENSIVE VALIDATION VIEW
-- =============================================================================
CREATE OR REPLACE VIEW V_CAPI_VALIDATION_DETAIL AS
SELECT 
    EVENT_ID,
    EVENT_NAME,
    EVENT_TIME,
    ACTION_SOURCE,
    
    -- Required field checks
    CASE WHEN EVENT_NAME IS NULL THEN 'FAIL' ELSE 'PASS' END AS CHK_EVENT_NAME,
    CASE WHEN EVENT_TIME IS NULL THEN 'FAIL' ELSE 'PASS' END AS CHK_EVENT_TIME,
    CASE WHEN ACTION_SOURCE IS NULL THEN 'FAIL' ELSE 'PASS' END AS CHK_ACTION_SOURCE,
    
    -- Website-specific checks
    CASE 
        WHEN ACTION_SOURCE = 'website' AND EVENT_SOURCE_URL IS NULL THEN 'FAIL'
        WHEN ACTION_SOURCE = 'website' THEN 'PASS'
        ELSE 'N/A'
    END AS CHK_SOURCE_URL,
    
    CASE 
        WHEN ACTION_SOURCE = 'website' AND CLIENT_USER_AGENT IS NULL THEN 'FAIL'
        WHEN ACTION_SOURCE = 'website' THEN 'PASS'
        ELSE 'N/A'
    END AS CHK_USER_AGENT,
    
    -- Value/currency checks
    CASE 
        WHEN EVENT_NAME IN ('Purchase', 'Donate', 'StartTrial', 'Subscribe') AND VALUE IS NULL THEN 'FAIL'
        ELSE 'PASS'
    END AS CHK_VALUE_REQUIRED,
    
    CASE 
        WHEN VALUE IS NOT NULL AND CURRENCY IS NULL THEN 'FAIL'
        ELSE 'PASS'
    END AS CHK_CURRENCY,
    
    -- PII quality checks
    CASE 
        WHEN EM IS NOT NULL AND LENGTH(EM) != 64 THEN 'WARN'
        ELSE 'PASS'
    END AS CHK_EMAIL_HASH,
    
    CASE 
        WHEN PH IS NOT NULL AND LENGTH(PH) != 64 THEN 'WARN'
        ELSE 'PASS'
    END AS CHK_PHONE_HASH,
    
    -- Match quality score
    CASE 
        WHEN EM IS NOT NULL OR PH IS NOT NULL THEN 'GOOD'
        WHEN FBP IS NOT NULL OR FBC IS NOT NULL THEN 'MODERATE'
        WHEN CLIENT_IP_ADDRESS IS NOT NULL AND CLIENT_USER_AGENT IS NOT NULL THEN 'LOW'
        ELSE 'POOR'
    END AS MATCH_QUALITY,
    
    STATUS,
    CREATED_AT
    
FROM META_CAPI_EVENTS
WHERE STATUS = 'PENDING';

-- =============================================================================
-- VALIDATION SUMMARY
-- =============================================================================
CREATE OR REPLACE VIEW V_VALIDATION_SUMMARY AS
SELECT 
    COUNT(*) AS TOTAL_PENDING,
    COUNT(CASE WHEN CHK_EVENT_NAME = 'FAIL' THEN 1 END) AS MISSING_EVENT_NAME,
    COUNT(CASE WHEN CHK_EVENT_TIME = 'FAIL' THEN 1 END) AS MISSING_EVENT_TIME,
    COUNT(CASE WHEN CHK_ACTION_SOURCE = 'FAIL' THEN 1 END) AS MISSING_ACTION_SOURCE,
    COUNT(CASE WHEN CHK_SOURCE_URL = 'FAIL' THEN 1 END) AS MISSING_SOURCE_URL,
    COUNT(CASE WHEN CHK_USER_AGENT = 'FAIL' THEN 1 END) AS MISSING_USER_AGENT,
    COUNT(CASE WHEN CHK_VALUE_REQUIRED = 'FAIL' THEN 1 END) AS MISSING_VALUE,
    COUNT(CASE WHEN CHK_CURRENCY = 'FAIL' THEN 1 END) AS MISSING_CURRENCY,
    COUNT(CASE WHEN MATCH_QUALITY = 'GOOD' THEN 1 END) AS HIGH_MATCH_EVENTS,
    COUNT(CASE WHEN MATCH_QUALITY = 'POOR' THEN 1 END) AS LOW_MATCH_EVENTS
FROM V_CAPI_VALIDATION_DETAIL;

-- =============================================================================
-- VALIDATE EVENT PROCEDURE
-- Returns specific validation errors
-- =============================================================================
CREATE OR REPLACE PROCEDURE validate_pending_events()
RETURNS TABLE (
    event_id VARCHAR,
    event_name VARCHAR,
    check_name VARCHAR,
    status VARCHAR,
    message VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        SELECT 
            EVENT_ID,
            EVENT_NAME,
            CHECK_NAME,
            CHECK_STATUS,
            MESSAGE
        FROM (
            SELECT EVENT_ID, EVENT_NAME, 'EVENT_NAME' AS CHECK_NAME, CHK_EVENT_NAME AS CHECK_STATUS, 'Event name is required' AS MESSAGE FROM V_CAPI_VALIDATION_DETAIL WHERE CHK_EVENT_NAME = 'FAIL'
            UNION ALL
            SELECT EVENT_ID, EVENT_NAME, 'EVENT_TIME', CHK_EVENT_TIME, 'Event time is required' FROM V_CAPI_VALIDATION_DETAIL WHERE CHK_EVENT_TIME = 'FAIL'
            UNION ALL
            SELECT EVENT_ID, EVENT_NAME, 'ACTION_SOURCE', CHK_ACTION_SOURCE, 'Action source is required' FROM V_CAPI_VALIDATION_DETAIL WHERE CHK_ACTION_SOURCE = 'FAIL'
            UNION ALL
            SELECT EVENT_ID, EVENT_NAME, 'SOURCE_URL', CHK_SOURCE_URL, 'Website events require event_source_url' FROM V_CAPI_VALIDATION_DETAIL WHERE CHK_SOURCE_URL = 'FAIL'
            UNION ALL
            SELECT EVENT_ID, EVENT_NAME, 'USER_AGENT', CHK_USER_AGENT, 'Website events require client_user_agent' FROM V_CAPI_VALIDATION_DETAIL WHERE CHK_USER_AGENT = 'FAIL'
            UNION ALL
            SELECT EVENT_ID, EVENT_NAME, 'VALUE', CHK_VALUE_REQUIRED, 'Value is required for this event type' FROM V_CAPI_VALIDATION_DETAIL WHERE CHK_VALUE_REQUIRED = 'FAIL'
            UNION ALL
            SELECT EVENT_ID, EVENT_NAME, 'CURRENCY', CHK_CURRENCY, 'Currency is required when value is set' FROM V_CAPI_VALIDATION_DETAIL WHERE CHK_CURRENCY = 'FAIL'
        )
        ORDER BY EVENT_ID, CHECK_NAME
    );
    RETURN TABLE(res);
END;
$$;

-- =============================================================================
-- HASH VALIDATION
-- Check if PII fields are properly hashed (SHA256 = 64 hex chars)
-- =============================================================================
CREATE OR REPLACE VIEW V_HASH_VALIDATION AS
SELECT 
    EVENT_ID,
    CASE WHEN EM IS NOT NULL AND LENGTH(EM) != 64 THEN 'Invalid email hash length: ' || LENGTH(EM) END AS EMAIL_ISSUE,
    CASE WHEN PH IS NOT NULL AND LENGTH(PH) != 64 THEN 'Invalid phone hash length: ' || LENGTH(PH) END AS PHONE_ISSUE,
    CASE WHEN FN IS NOT NULL AND LENGTH(FN) != 64 THEN 'Invalid first name hash length: ' || LENGTH(FN) END AS FN_ISSUE,
    CASE WHEN LN IS NOT NULL AND LENGTH(LN) != 64 THEN 'Invalid last name hash length: ' || LENGTH(LN) END AS LN_ISSUE
FROM META_CAPI_EVENTS
WHERE STATUS = 'PENDING'
AND (
    (EM IS NOT NULL AND LENGTH(EM) != 64) OR
    (PH IS NOT NULL AND LENGTH(PH) != 64) OR
    (FN IS NOT NULL AND LENGTH(FN) != 64) OR
    (LN IS NOT NULL AND LENGTH(LN) != 64)
);
