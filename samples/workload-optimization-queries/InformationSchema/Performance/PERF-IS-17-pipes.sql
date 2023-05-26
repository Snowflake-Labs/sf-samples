-------------------------------------------------
-- NAME:	 PERF-IS-17-pipes.txt
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	

SELECT 
	P.PIPE_NAME,
	P.PIPE_OWNER,
	P.PIPE_CATALOG,
	P.PIPE_SCHEMA,
	P.DEFINITION,
	P.PATTERN,
	P.IS_AUTOINGEST_ENABLED,
	P.NOTIFICATION_CHANNEL_NAME,
	P.CREATED,
	P.LAST_ALTERED,
--	P.DELETED,
	P.COMMENT,
	
	C.FILE_NAME,
	C.STAGE_LOCATION,
	C.LAST_LOAD_TIME,
	C.ROW_COUNT,
	C.ROW_PARSED,
	C.FILE_SIZE,
	C.FIRST_ERROR_MESSAGE,
	C.FIRST_ERROR_LINE_NUMBER,
	C.FIRST_ERROR_CHARACTER_POS,
	C.FIRST_ERROR_COLUMN_NAME,
	C.ERROR_COUNT,
	C.ERROR_LIMIT,
	C.STATUS,
	C.TABLE_CATALOG_NAME,
	C.TABLE_SCHEMA_NAME,
	C.TABLE_NAME,
	C.PIPE_RECEIVED_TIME
--	C.FIRST_COMMIT_TIME
	
FROM
	TABLE($PIPES) P
	JOIN TABLE(SNOWFLAKE.INFORMATION_SCHEMA.COPY_HISTORY(
		TABLE_NAME=>$TABLE_NAME,
		START_TIME=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		END_TIME=>CURRENT_TIMESTAMP())) C 
	ON
		P.PIPE_NAME = C.PIPE_NAME AND
		P.PIPE_CATALOG = C.PIPE_CATALOG_NAME AND
		P.PIPE_SCHEMA = C.PIPE_SCHEMA_NAME
WHERE
    P.CREATED BETWEEN $TS_START AND $TS_END AND
    C.LAST_LOAD_TIME BETWEEN $TS_START AND $TS_END
    --AND P.PIPE_NAME = ''
ORDER BY
    P.PIPE_NAME
;


