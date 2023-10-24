-------------------------------------------------
-- NAME:	 PERF-IS-45-vwh-sizing.txt
-------------------------------------------------
-- DESCRIPTION:
--	Determine optimal throughput for a request on a virtual warehouse.  Process runs 
--	requests on different warehouse sizes, then reports performance metrics for each 
--	execution on a warehouse
--					
-- OUTPUT:					
--	Looking for optimal throughput, short execution duration and zero spilling, on a 
--	warehouse configuration
--	
-- SETUP:
--	(1) Setup and execute Initialization script
--	(2) Setup sizing script
--		(a) Set Query Tag environment variable
--		(b) Paste SQL into each section of a warehouse execution section of script
--		(c) Execute and review results… short execution time and zero spilling
--
-- NEXT STEPS:					
--	(1) If results not sufficient, add larger warehouses to script
--	(2) Upon sufficient results, determine warehouse size with optimal throughput
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------

-----------------------------------------
-- SETUP
-----------------------------------------
CREATE WAREHOUSE IF NOT EXISTS SIZING_VWH;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER SESSION SET QUERY_TAG = 'WH_SIZING';

-----------------------------------------
-- XSMALL
-----------------------------------------
ALTER WAREHOUSE SIZING_VWH SUSPEND;
ALTER WAREHOUSE SIZING_VWH SET WAREHOUSE_SIZE = XSMALL;
ALTER WAREHOUSE SIZING_VWH RESUME;

SELECT ...;

SET XSMALL_QUERY_ID = '';

-----------------------------------------
-- SMALL
-----------------------------------------
ALTER WAREHOUSE SIZING_VWH SUSPEND;
ALTER WAREHOUSE SIZING_VWH SET WAREHOUSE_SIZE = SMALL;
ALTER WAREHOUSE SIZING_VWH RESUME;

SELECT ...;

SET SMALL_QUERY_ID = '';

-----------------------------------------
-- MEDIUM
-----------------------------------------
ALTER WAREHOUSE SIZING_VWH SUSPEND;
ALTER WAREHOUSE SIZING_VWH SET WAREHOUSE_SIZE = MEDIUM;
ALTER WAREHOUSE SIZING_VWH RESUME;

SELECT ...;

SET MEDIUM_QUERY_ID = '';

-----------------------------------------
-- LARGE
-----------------------------------------
ALTER WAREHOUSE SIZING_VWH SUSPEND;
ALTER WAREHOUSE SIZING_VWH SET WAREHOUSE_SIZE = LARGE;
ALTER WAREHOUSE SIZING_VWH RESUME;

SELECT ...;

SET LARGE_QUERY_ID = '';

-----------------------------------------
-- XLARGE
-----------------------------------------
ALTER WAREHOUSE SIZING_VWH SUSPEND;
ALTER WAREHOUSE SIZING_VWH SET WAREHOUSE_SIZE = XLARGE;
ALTER WAREHOUSE SIZING_VWH RESUME;

SELECT ...;

SET XLARGE_QUERY_ID = '';

-----------------------------------------
-- COLLECT METRICS
-----------------------------------------
SELECT	query_id,
		query_text, 
		warehouse_size,
		(TOTAL_ELAPSED_TIME / 1000) Time_in_seconds, 
		partitions_total, 
		partitions_scanned,
		bytes_spilled_to_local_storage, 
		bytes_spilled_to_remote_storage, 
		query_load_percent
FROM	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp()))
WHERE	query_tag = 'WH_SIZING' 
AND		QUERY_ID IN ($XSMALL_QUERY_ID,
		$SMALL_QUERY_ID,
		$MEDIUM_QUERY_ID,
		$LARGE_QUERY_ID,
		$XLARGE_QUERY_ID)
ORDER	BY start_time DESC;

-----------------------------------------
-- RESET
-----------------------------------------
ALTER WAREHOUSE SIZING_VWH SUSPEND;
ALTER WAREHOUSE SIZING_VWH SET WAREHOUSE_SIZE = XSMALL;


