-- =============================================================================
-- META CAPI ALERTING
-- Procedures for detecting and alerting on pipeline issues
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- CHECK FOR ALERTS
-- Returns any active alert conditions
-- =============================================================================
CREATE OR REPLACE PROCEDURE check_capi_alerts()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    alerts ARRAY DEFAULT ARRAY_CONSTRUCT();
    pending_count INTEGER;
    failed_24h INTEGER;
    sent_24h INTEGER;
    success_rate FLOAT;
    hours_since_batch FLOAT;
BEGIN
    -- Check pending backlog
    SELECT COUNT(*) INTO pending_count FROM META_CAPI_EVENTS WHERE STATUS = 'PENDING';
    IF (pending_count > 10000) THEN
        alerts := ARRAY_APPEND(alerts, OBJECT_CONSTRUCT(
            'severity', 'WARNING',
            'alert', 'HIGH_PENDING_BACKLOG',
            'message', 'More than 10,000 events pending',
            'value', pending_count
        ));
    END IF;
    
    -- Check success rate
    SELECT COUNT(*) INTO failed_24h FROM META_CAPI_EVENTS 
    WHERE STATUS = 'FAILED' AND PROCESSED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP());
    
    SELECT COUNT(*) INTO sent_24h FROM META_CAPI_EVENTS 
    WHERE STATUS = 'SENT' AND PROCESSED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP());
    
    IF (sent_24h + failed_24h > 0) THEN
        success_rate := (sent_24h * 100.0) / (sent_24h + failed_24h);
        
        IF (success_rate < 80) THEN
            alerts := ARRAY_APPEND(alerts, OBJECT_CONSTRUCT(
                'severity', 'CRITICAL',
                'alert', 'LOW_SUCCESS_RATE',
                'message', 'Success rate below 80%',
                'value', ROUND(success_rate, 2)
            ));
        ELSEIF (success_rate < 95) THEN
            alerts := ARRAY_APPEND(alerts, OBJECT_CONSTRUCT(
                'severity', 'WARNING',
                'alert', 'DEGRADED_SUCCESS_RATE',
                'message', 'Success rate below 95%',
                'value', ROUND(success_rate, 2)
            ));
        END IF;
    END IF;
    
    -- Check for stale processing
    SELECT DATEDIFF('hour', MAX(PROCESSED_AT), CURRENT_TIMESTAMP()) INTO hours_since_batch
    FROM META_CAPI_LOG;
    
    IF (hours_since_batch > 2 AND pending_count > 0) THEN
        alerts := ARRAY_APPEND(alerts, OBJECT_CONSTRUCT(
            'severity', 'WARNING',
            'alert', 'STALE_PROCESSING',
            'message', 'No batches processed in over 2 hours with pending events',
            'value', hours_since_batch
        ));
    END IF;
    
    RETURN OBJECT_CONSTRUCT(
        'alert_count', ARRAY_SIZE(alerts),
        'alerts', alerts,
        'checked_at', CURRENT_TIMESTAMP()
    );
END;
$$;

-- =============================================================================
-- ALERT HISTORY TABLE
-- Store historical alerts for trending
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_CAPI_ALERTS (
    ALERT_ID NUMBER AUTOINCREMENT,
    SEVERITY VARCHAR(20),
    ALERT_TYPE VARCHAR(50),
    MESSAGE VARCHAR(500),
    VALUE VARIANT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACKNOWLEDGED_AT TIMESTAMP_NTZ,
    ACKNOWLEDGED_BY VARCHAR(100),
    PRIMARY KEY (ALERT_ID)
);

-- =============================================================================
-- LOG ALERTS
-- Captures current alerts to history
-- =============================================================================
CREATE OR REPLACE PROCEDURE log_capi_alerts()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    alert_result VARIANT;
    alert_count INTEGER DEFAULT 0;
    i INTEGER;
    alert VARIANT;
BEGIN
    CALL check_capi_alerts() INTO alert_result;
    
    FOR i IN 0 TO ARRAY_SIZE(alert_result:alerts) - 1 DO
        alert := alert_result:alerts[i];
        
        INSERT INTO META_CAPI_ALERTS (SEVERITY, ALERT_TYPE, MESSAGE, VALUE)
        VALUES (
            alert:severity::VARCHAR,
            alert:alert::VARCHAR,
            alert:message::VARCHAR,
            alert:value
        );
        
        alert_count := alert_count + 1;
    END FOR;
    
    RETURN alert_count;
END;
$$;

-- =============================================================================
-- SCHEDULED ALERT CHECK (Optional)
-- Run every 15 minutes
-- =============================================================================
-- CREATE OR REPLACE TASK meta_capi_alert_check
--     WAREHOUSE = <WAREHOUSE>
--     SCHEDULE = 'USING CRON */15 * * * * UTC'
-- AS
--     CALL log_capi_alerts();
