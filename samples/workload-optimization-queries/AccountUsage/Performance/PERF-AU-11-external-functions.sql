-------------------------------------------------
-- NAME:	 PERF-AU-11-external-functions.txt
-------------------------------------------------
-- DESCRIPTION:
--	Report of function execution metric characteristics
--
-- OUTPUT:
--	Execution time, Bytes transferred, execution frequency
--	Combination of metrics helps understand needs for optimizing
--
-- NEXT STEPS:
--	Optimize processing code
--	Optimize underlying data objects
--	Determine optimal warehouse configuration
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
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
	EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,
	EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,
	EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,
	EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,
	EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES

FROM
    TABLE($query_history)
WHERE
    START_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    EXTERNAL_FUNCTION_TOTAL_SENT_BYTES DESC
--  EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES DESC
LIMIT 100
;