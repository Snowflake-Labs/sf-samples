/*
 * ============================================================================
 * SP_EVENT_LOG - Event Table for Stored Procedure Logging
 * ============================================================================
 * 
 * Purpose:
 *   Captures log messages from stored procedures using snowflake.log().
 *   Enables searchable, persistent logging with automatic timestamps.
 * 
 * Setup:
 *   Database LOG_LEVEL must be set to capture logs at the desired level.
 *   Event table must be set at database level.
 * 
 * Query Examples:
 *   -- View recent logs
 *   SELECT TIMESTAMP, RECORD['severity_text']::STRING AS LEVEL, VALUE AS MESSAGE
 *   FROM IDR_DEMO.SILVER.SP_EVENT_LOG
 *   ORDER BY TIMESTAMP DESC LIMIT 20;
 *   
 *   -- Filter by procedure
 *   SELECT TIMESTAMP, VALUE AS MESSAGE
 *   FROM IDR_DEMO.SILVER.SP_EVENT_LOG
 *   WHERE VALUE LIKE '%SP_STANDARDIZE_DATA%'
 *   ORDER BY TIMESTAMP DESC;
 *   
 *   -- Filter by severity
 *   SELECT TIMESTAMP, VALUE AS MESSAGE
 *   FROM IDR_DEMO.SILVER.SP_EVENT_LOG
 *   WHERE RECORD['severity_text']::STRING = 'ERROR'
 *   ORDER BY TIMESTAMP DESC;
 * 
 * ============================================================================
 */

USE ROLE SYSADMIN;
USE DATABASE IDR_DEMO;
USE SCHEMA SILVER;

CREATE EVENT TABLE IF NOT EXISTS SP_EVENT_LOG;

ALTER DATABASE IDR_DEMO SET LOG_LEVEL = DEBUG;
ALTER DATABASE IDR_DEMO SET EVENT_TABLE = IDR_DEMO.SILVER.SP_EVENT_LOG;

SELECT 'SP_EVENT_LOG event table created and configured' AS STATUS;
