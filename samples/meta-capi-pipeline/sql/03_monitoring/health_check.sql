-- =============================================================================
-- META CAPI HEALTH CHECK QUERIES
-- Monitoring and observability for the pipeline
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- HEALTH CHECK PROCEDURE
-- Returns comprehensive pipeline status
-- =============================================================================
CREATE OR REPLACE PROCEDURE capi_health_check()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    pending_count INTEGER;
    sent_24h INTEGER;
    failed_24h INTEGER;
    total_24h INTEGER;
    success_rate FLOAT;
    avg_per_hour FLOAT;
    last_batch TIMESTAMP_NTZ;
    task_state VARCHAR;
BEGIN
    -- Count pending
    SELECT COUNT(*) INTO pending_count 
    FROM META_CAPI_EVENTS WHERE STATUS = 'PENDING';
    
    -- Count sent in last 24h
    SELECT COUNT(*) INTO sent_24h 
    FROM META_CAPI_EVENTS 
    WHERE STATUS = 'SENT' 
    AND PROCESSED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP());
    
    -- Count failed in last 24h
    SELECT COUNT(*) INTO failed_24h 
    FROM META_CAPI_EVENTS 
    WHERE STATUS = 'FAILED' 
    AND PROCESSED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP());
    
    -- Calculate success rate
    total_24h := sent_24h + failed_24h;
    success_rate := CASE 
        WHEN total_24h > 0 THEN ROUND((sent_24h * 100.0) / total_24h, 2)
        ELSE 100.0
    END;
    
    -- Average events per hour
    avg_per_hour := ROUND(total_24h / 24.0, 1);
    
    -- Last batch time
    SELECT MAX(PROCESSED_AT) INTO last_batch FROM META_CAPI_LOG;
    
    -- Task state
    SELECT STATE INTO task_state 
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'META_CAPI_SEND_EVENTS'
    )) 
    LIMIT 1;
    
    RETURN OBJECT_CONSTRUCT(
        'pending_events', pending_count,
        'sent_24h', sent_24h,
        'failed_24h', failed_24h,
        'success_rate', success_rate,
        'avg_events_per_hour', avg_per_hour,
        'last_batch_time', last_batch,
        'task_state', COALESCE(task_state, 'unknown')
    );
END;
$$;

-- =============================================================================
-- SUMMARY VIEWS
-- =============================================================================

-- Events by status
CREATE OR REPLACE VIEW V_EVENTS_BY_STATUS AS
SELECT 
    STATUS,
    COUNT(*) AS EVENT_COUNT,
    MIN(CREATED_AT) AS OLDEST_EVENT,
    MAX(CREATED_AT) AS NEWEST_EVENT
FROM META_CAPI_EVENTS
GROUP BY STATUS;

-- Events by type (last 7 days)
CREATE OR REPLACE VIEW V_EVENTS_BY_TYPE AS
SELECT 
    EVENT_NAME,
    COUNT(*) AS EVENT_COUNT,
    COUNT(CASE WHEN STATUS = 'SENT' THEN 1 END) AS SENT_COUNT,
    COUNT(CASE WHEN STATUS = 'FAILED' THEN 1 END) AS FAILED_COUNT
FROM META_CAPI_EVENTS
WHERE CREATED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY EVENT_NAME
ORDER BY EVENT_COUNT DESC;

-- Hourly throughput
CREATE OR REPLACE VIEW V_HOURLY_THROUGHPUT AS
SELECT 
    DATE_TRUNC('hour', PROCESSED_AT) AS HOUR,
    COUNT(*) AS TOTAL_EVENTS,
    COUNT(CASE WHEN STATUS = 'SENT' THEN 1 END) AS SENT,
    COUNT(CASE WHEN STATUS = 'FAILED' THEN 1 END) AS FAILED
FROM META_CAPI_EVENTS
WHERE PROCESSED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('hour', PROCESSED_AT)
ORDER BY HOUR DESC;

-- Recent batches
CREATE OR REPLACE VIEW V_RECENT_BATCHES AS
SELECT 
    BATCH_ID,
    MIN(PROCESSED_AT) AS BATCH_TIME,
    COUNT(*) AS EVENT_COUNT,
    COUNT(CASE WHEN STATUS = 'SENT' THEN 1 END) AS SENT,
    COUNT(CASE WHEN STATUS = 'FAILED' THEN 1 END) AS FAILED
FROM META_CAPI_LOG
WHERE PROCESSED_AT > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY BATCH_ID
ORDER BY BATCH_TIME DESC
LIMIT 20;

-- =============================================================================
-- QUICK STATUS QUERIES
-- =============================================================================

-- Current status summary
-- SELECT * FROM V_EVENTS_BY_STATUS;

-- Events by type
-- SELECT * FROM V_EVENTS_BY_TYPE;

-- Last 24 hours throughput
-- SELECT * FROM V_HOURLY_THROUGHPUT WHERE HOUR > DATEADD('day', -1, CURRENT_TIMESTAMP());

-- Recent batch results
-- SELECT * FROM V_RECENT_BATCHES;
