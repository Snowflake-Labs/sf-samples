-- =============================================================================
-- META CAPI SCHEDULED TASK
-- Automatic processing of pending events
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- CREATE TASK - Hourly processing
-- Replace <WAREHOUSE> and <PIXEL_ID> with actual values
-- =============================================================================
CREATE OR REPLACE TASK meta_capi_send_events
    WAREHOUSE = <WAREHOUSE>
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour at minute 0
    COMMENT = 'Processes pending Meta CAPI events hourly'
AS
    CALL process_pending_capi_events(
        (SELECT CONFIG_VALUE FROM META_CAPI_CONFIG WHERE CONFIG_KEY = 'PIXEL_ID')
    );

-- =============================================================================
-- ALTERNATIVE SCHEDULES
-- =============================================================================

-- Every 15 minutes
-- SCHEDULE = 'USING CRON */15 * * * * UTC'

-- Every 5 minutes
-- SCHEDULE = 'USING CRON */5 * * * * UTC'

-- Daily at midnight UTC
-- SCHEDULE = 'USING CRON 0 0 * * * UTC'

-- Every 30 minutes during business hours (9 AM - 6 PM UTC)
-- SCHEDULE = 'USING CRON */30 9-18 * * * UTC'

-- =============================================================================
-- TASK MANAGEMENT
-- =============================================================================

-- Start the task
ALTER TASK meta_capi_send_events RESUME;

-- Stop the task
-- ALTER TASK meta_capi_send_events SUSPEND;

-- Check task status
-- SHOW TASKS LIKE 'meta_capi_send_events';

-- View task history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'META_CAPI_SEND_EVENTS',
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
-- )) ORDER BY SCHEDULED_TIME DESC;

-- Manual execution
-- EXECUTE TASK meta_capi_send_events;
