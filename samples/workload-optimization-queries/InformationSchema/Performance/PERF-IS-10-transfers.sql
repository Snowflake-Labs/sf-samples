-------------------------------------------------
-- NAME:	 PERF-IS-10-transfers.txt
-------------------------------------------------
-- DESCRIPTION:
--	Report of Inbound/Outbound data transfers
--
-- OUTPUT:
--	Look for outliers to understand cost of transferring data out
--
-- NEXT STEPS:
--	Review SQL and volume of data transfer
--	Work with users to understand need for transferring large volumes of data
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT
	-- HEADER
	QUERY_ID,
	QUERY_TYPE,
	QUERY_TEXT,
	DATABASE_NAME,
	SCHEMA_NAME,
	SESSION_ID,
	USER_NAME,
	ROLE_NAME,
	WAREHOUSE_NAME,
	WAREHOUSE_SIZE,
	WAREHOUSE_TYPE,
	CLUSTER_NUMBER,
	QUERY_TAG,
	EXECUTION_STATUS,
	ERROR_CODE,
	ERROR_MESSAGE,
	START_TIME,
	END_TIME,
	TOTAL_ELAPSED_TIME,
	-- DETAIL
	OUTBOUND_DATA_TRANSFER_CLOUD,
	OUTBOUND_DATA_TRANSFER_REGION,
	OUTBOUND_DATA_TRANSFER_BYTES,
	INBOUND_DATA_TRANSFER_CLOUD,
	INBOUND_DATA_TRANSFER_REGION,
	INBOUND_DATA_TRANSFER_BYTES

FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp()))
ORDER BY
--    OUTBOUND_DATA_TRANSFER_BYTES DESC
    INBOUND_DATA_TRANSFER_BYTES DESC
LIMIT 100
;