-- =============================================================================
-- META CAPI PROCESSING PROCEDURES
-- Batch processing and event management
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- MAIN PROCESSING PROCEDURE
-- Processes pending events and sends to Meta CAPI
-- USER_DATA and CUSTOM_DATA are passed through directly as pre-built VARIANT
-- =============================================================================
CREATE OR REPLACE PROCEDURE process_pending_capi_events(
    pixel_id VARCHAR,
    test_event_code VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    batch_id VARCHAR;
    processed_count INTEGER DEFAULT 0;
    sent_count INTEGER DEFAULT 0;
    failed_count INTEGER DEFAULT 0;
BEGIN
    batch_id := UUID_STRING();
    
    INSERT INTO META_CAPI_LOG (BATCH_ID, EVENT_ID, STATUS, RESPONSE, PROCESSED_AT)
    SELECT 
        :batch_id,
        r.EVENT_ID,
        r.STATUS,
        r.RESPONSE,
        CURRENT_TIMESTAMP()
    FROM (
        SELECT ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'event_name', EVENT_NAME,
                'event_time', DATE_PART('epoch_second', EVENT_TIME)::INTEGER,
                'event_id', EVENT_ID,
                'action_source', ACTION_SOURCE,
                'event_source_url', EVENT_SOURCE_URL,
                'user_data', USER_DATA,
                'custom_data', CUSTOM_DATA
            )
        ) AS events_array
        FROM META_CAPI_EVENTS
        WHERE STATUS = 'PENDING'
    ) batch,
    TABLE(send_to_meta_capi(:pixel_id, batch.events_array, :test_event_code)) r;
    
    UPDATE META_CAPI_EVENTS e
    SET 
        STATUS = l.STATUS,
        PROCESSED_AT = l.PROCESSED_AT,
        META_RESPONSE = l.RESPONSE,
        ERROR_MESSAGE = CASE 
            WHEN l.STATUS = 'FAILED' THEN l.RESPONSE:error::VARCHAR 
            ELSE NULL 
        END
    FROM META_CAPI_LOG l
    WHERE e.EVENT_ID = l.EVENT_ID
    AND l.BATCH_ID = :batch_id;
    
    SELECT COUNT(*) INTO processed_count FROM META_CAPI_LOG WHERE BATCH_ID = :batch_id;
    SELECT COUNT(*) INTO sent_count FROM META_CAPI_LOG WHERE BATCH_ID = :batch_id AND STATUS = 'SENT';
    SELECT COUNT(*) INTO failed_count FROM META_CAPI_LOG WHERE BATCH_ID = :batch_id AND STATUS = 'FAILED';
    
    RETURN OBJECT_CONSTRUCT(
        'batch_id', batch_id,
        'processed', processed_count,
        'sent', sent_count,
        'failed', failed_count,
        'timestamp', CURRENT_TIMESTAMP()
    );
END;
$$;

-- =============================================================================
-- RESEND FAILED EVENTS
-- Resets failed events for reprocessing
-- =============================================================================
CREATE OR REPLACE PROCEDURE resend_failed_events(
    max_retry INTEGER DEFAULT 3,
    limit_count INTEGER DEFAULT 1000
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    reset_count INTEGER;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE _failed_to_retry AS
    SELECT EVENT_ID FROM META_CAPI_DB.PIPELINE.META_CAPI_EVENTS
    WHERE STATUS = 'FAILED' AND RETRY_COUNT < :max_retry
    LIMIT :limit_count;
    
    UPDATE META_CAPI_DB.PIPELINE.META_CAPI_EVENTS 
    SET 
        STATUS = 'PENDING', 
        RETRY_COUNT = RETRY_COUNT + 1,
        ERROR_MESSAGE = NULL,
        META_RESPONSE = NULL
    WHERE EVENT_ID IN (SELECT EVENT_ID FROM _failed_to_retry);
    
    SELECT COUNT(*) INTO reset_count FROM _failed_to_retry;
    
    DROP TABLE IF EXISTS _failed_to_retry;
    
    RETURN OBJECT_CONSTRUCT(
        'events_reset', reset_count,
        'max_retry', max_retry,
        'timestamp', CURRENT_TIMESTAMP()
    );
END;
$$;

-- =============================================================================
-- VALIDATE EVENTS
-- Returns events with validation issues
-- =============================================================================
CREATE OR REPLACE PROCEDURE validate_capi_events()
RETURNS TABLE (event_id VARCHAR, event_name VARCHAR, issue VARCHAR)
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
            CASE 
                WHEN EVENT_NAME IS NULL THEN 'Missing event_name'
                WHEN EVENT_TIME IS NULL THEN 'Missing event_time'
                WHEN ACTION_SOURCE IS NULL THEN 'Missing action_source'
                WHEN ACTION_SOURCE = 'website' AND EVENT_SOURCE_URL IS NULL THEN 'Website events require event_source_url'
                WHEN ACTION_SOURCE = 'website' AND USER_DATA:client_user_agent IS NULL THEN 'Website events require client_user_agent'
                WHEN EVENT_NAME IN ('Purchase', 'Donate', 'StartTrial', 'Subscribe') AND CUSTOM_DATA:value IS NULL THEN 'Value required for this event type'
                WHEN CUSTOM_DATA:value IS NOT NULL AND CUSTOM_DATA:currency IS NULL THEN 'Currency required when value is set'
                WHEN USER_DATA IS NULL OR (USER_DATA:em IS NULL AND USER_DATA:ph IS NULL AND USER_DATA:external_id IS NULL) THEN 'At least one user identifier required'
            END AS ISSUE
        FROM META_CAPI_EVENTS
        WHERE STATUS = 'PENDING'
        HAVING ISSUE IS NOT NULL
    );
    RETURN TABLE(res);
END;
$$;
