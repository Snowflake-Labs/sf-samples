-- =============================================================================
-- META CAPI ERROR HANDLING
-- Recovery and retry procedures
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- RETRY FAILED EVENTS
-- Resets failed events for reprocessing with retry limits
-- =============================================================================
CREATE OR REPLACE PROCEDURE retry_failed_events(
    max_retries INTEGER DEFAULT 3,
    batch_limit INTEGER DEFAULT 1000
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    updated_count INTEGER;
    skipped_count INTEGER;
BEGIN
    -- Count events that exceeded retry limit
    SELECT COUNT(*) INTO skipped_count
    FROM META_CAPI_EVENTS
    WHERE STATUS = 'FAILED' AND RETRY_COUNT >= :max_retries;
    
    -- Reset eligible failed events
    UPDATE META_CAPI_EVENTS
    SET 
        STATUS = 'PENDING',
        RETRY_COUNT = RETRY_COUNT + 1,
        ERROR_MESSAGE = NULL,
        META_RESPONSE = NULL,
        PROCESSED_AT = NULL
    WHERE STATUS = 'FAILED'
    AND RETRY_COUNT < :max_retries
    LIMIT :batch_limit;
    
    SELECT COUNT(*) INTO updated_count
    FROM META_CAPI_EVENTS
    WHERE STATUS = 'PENDING' AND RETRY_COUNT > 0;
    
    RETURN OBJECT_CONSTRUCT(
        'events_reset', updated_count,
        'events_skipped', skipped_count,
        'max_retries', max_retries,
        'timestamp', CURRENT_TIMESTAMP()
    );
END;
$$;

-- =============================================================================
-- QUARANTINE PERMANENT FAILURES
-- Move events that exceed retry limit to quarantine
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_CAPI_QUARANTINE (
    EVENT_ID VARCHAR(100),
    EVENT_NAME VARCHAR(50),
    EVENT_TIME TIMESTAMP_NTZ,
    ACTION_SOURCE VARCHAR(20),
    LAST_ERROR VARCHAR(2000),
    RETRY_COUNT INTEGER,
    QUARANTINED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENT_DATA VARIANT
);

CREATE OR REPLACE PROCEDURE quarantine_failed_events(max_retries INTEGER DEFAULT 3)
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    quarantine_count INTEGER;
BEGIN
    -- Move to quarantine
    INSERT INTO META_CAPI_QUARANTINE (EVENT_ID, EVENT_NAME, EVENT_TIME, ACTION_SOURCE, LAST_ERROR, RETRY_COUNT, EVENT_DATA)
    SELECT 
        EVENT_ID,
        EVENT_NAME,
        EVENT_TIME,
        ACTION_SOURCE,
        ERROR_MESSAGE,
        RETRY_COUNT,
        OBJECT_CONSTRUCT(
            'em', EM, 'ph', PH, 'fn', FN, 'ln', LN,
            'value', VALUE, 'currency', CURRENCY,
            'content_ids', CONTENT_IDS,
            'meta_response', META_RESPONSE
        )
    FROM META_CAPI_EVENTS
    WHERE STATUS = 'FAILED' AND RETRY_COUNT >= :max_retries;
    
    -- Delete from main table
    DELETE FROM META_CAPI_EVENTS
    WHERE STATUS = 'FAILED' AND RETRY_COUNT >= :max_retries;
    
    SELECT COUNT(*) INTO quarantine_count FROM META_CAPI_QUARANTINE;
    
    RETURN quarantine_count;
END;
$$;

-- =============================================================================
-- ERROR ANALYSIS
-- Group errors by type for pattern detection
-- =============================================================================
CREATE OR REPLACE VIEW V_ERROR_ANALYSIS AS
SELECT 
    COALESCE(
        META_RESPONSE:error:message::VARCHAR,
        META_RESPONSE:error::VARCHAR,
        ERROR_MESSAGE,
        'Unknown error'
    ) AS ERROR_TYPE,
    COUNT(*) AS ERROR_COUNT,
    MIN(PROCESSED_AT) AS FIRST_OCCURRENCE,
    MAX(PROCESSED_AT) AS LAST_OCCURRENCE,
    ARRAY_AGG(EVENT_ID) WITHIN GROUP (ORDER BY PROCESSED_AT DESC) AS SAMPLE_EVENT_IDS
FROM META_CAPI_EVENTS
WHERE STATUS = 'FAILED'
GROUP BY ERROR_TYPE
ORDER BY ERROR_COUNT DESC;

-- =============================================================================
-- RESET SPECIFIC EVENTS
-- Manually reset events by ID
-- =============================================================================
CREATE OR REPLACE PROCEDURE reset_events_by_id(event_ids ARRAY)
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    reset_count INTEGER;
BEGIN
    UPDATE META_CAPI_EVENTS
    SET 
        STATUS = 'PENDING',
        ERROR_MESSAGE = NULL,
        META_RESPONSE = NULL,
        PROCESSED_AT = NULL
    WHERE EVENT_ID IN (SELECT VALUE FROM TABLE(FLATTEN(INPUT => :event_ids)));
    
    SELECT COUNT(*) INTO reset_count
    FROM META_CAPI_EVENTS
    WHERE EVENT_ID IN (SELECT VALUE FROM TABLE(FLATTEN(INPUT => :event_ids)))
    AND STATUS = 'PENDING';
    
    RETURN reset_count;
END;
$$;

-- =============================================================================
-- PURGE OLD DATA
-- Clean up old logs and processed events
-- =============================================================================
CREATE OR REPLACE PROCEDURE purge_old_data(retention_days INTEGER DEFAULT 30)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    deleted_logs INTEGER;
    deleted_events INTEGER;
BEGIN
    -- Delete old logs
    DELETE FROM META_CAPI_LOG
    WHERE PROCESSED_AT < DATEADD('day', -:retention_days, CURRENT_TIMESTAMP());
    
    SELECT ROW_COUNT() INTO deleted_logs;
    
    -- Delete old sent events (keep failed for analysis)
    DELETE FROM META_CAPI_EVENTS
    WHERE STATUS = 'SENT'
    AND PROCESSED_AT < DATEADD('day', -:retention_days, CURRENT_TIMESTAMP());
    
    SELECT ROW_COUNT() INTO deleted_events;
    
    RETURN OBJECT_CONSTRUCT(
        'deleted_logs', deleted_logs,
        'deleted_events', deleted_events,
        'retention_days', retention_days,
        'timestamp', CURRENT_TIMESTAMP()
    );
END;
$$;
