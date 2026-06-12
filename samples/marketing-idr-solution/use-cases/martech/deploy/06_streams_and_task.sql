-- ============================================================================
-- Martech: 06_streams_and_task.sql
-- Creates the IDR_INCREMENTAL_TASK (suspended on deploy).
-- Streams are created in 03_martech_tables.sql via ddl/*.sql.
-- ============================================================================

USE DATABASE IDENTIFIER('&{deployment_db}');

!source ../sql/streams_and_task.sql
