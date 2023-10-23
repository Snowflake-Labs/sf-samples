-------------------------------------------------
-- NAME:	 PERF-AU-20-copy-job-history.txt
-------------------------------------------------
-- DESCRIPTION:
--	Data loading information for Pipes
--
-- OUTPUT:
--	Pipe identification information
--	Pipe processing metrics
--
-- NEXT STEPS:
--	If process failed, use information to troubleshoot
--	If performance issue, then use information to optimize process
--	if resolving an analytic performance issue, then maybe add order by for cluster key
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
SELECT 
	-- HEADER
	PIPE_CATALOG_NAME,
	PIPE_SCHEMA_NAME,
	PIPE_NAME,
	--TABLE_SCHEMA_ID,
	TABLE_SCHEMA_NAME,
	--TABLE_CATALOG_ID,
	TABLE_CATALOG_NAME,
	--TABLE_ID,
	TABLE_NAME,
	FILE_NAME,
	STAGE_LOCATION,
	LAST_LOAD_TIME,
	ROW_COUNT,
	ROW_PARSED,
	FILE_SIZE,
	FIRST_ERROR_MESSAGE,
	FIRST_ERROR_LINE_NUMBER,
	FIRST_ERROR_CHARACTER_POS,
	FIRST_ERROR_COLUMN_NAME,
	ERROR_COUNT,
	ERROR_LIMIT,
	STATUS,
	PIPE_RECEIVED_TIME,
	FIRST_COMMIT_TIME
from
	TABLE($COPY_HISTORY) 
WHERE
    LAST_LOAD_TIME BETWEEN $TS_START AND $TS_END
ORDER BY
    PIPE_NAME,TABLE_SCHEMA_ID,TABLE_ID,FILE_NAME
LIMIT 1000
;	
