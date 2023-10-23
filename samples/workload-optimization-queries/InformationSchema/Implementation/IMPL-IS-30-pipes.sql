-------------------------------------------------
-- NAME:	 IMPL-IS-30-pipes.txt
-------------------------------------------------
-- DESCRIPTION:
--	pipe information
--
-- OUTPUT:
--	pipe, pipe information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to pipe level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT 
	P.PIPE_ID,
	P.PIPE_NAME,
	P.PIPE_SCHEMA_ID,
	P.PIPE_SCHEMA,
	P.PIPE_CATALOG_ID,
	P.PIPE_CATALOG,
	P.IS_AUTOINGEST_ENABLED,
	P.NOTIFICATION_CHANNEL_NAME,
	P.PIPE_OWNER,
	P.DEFINITION,
	P.CREATED,
	P.LAST_ALTERED,
	P.COMMENT,
	P.PATTERN,
	P.DELETED,
	U.START_TIME,
	U.END_TIME,
	U.CREDITS_USED,
	U.BYTES_INSERTED,
	U.FILES_INSERTED
FROM
	TABLE($PIPES) P
	JOIN
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
		DATE_RANGE_START=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		DATE_RANGE_END=>CURRENT_TIMESTAMP())) U
--WHERE
--    P.PIPE_NAME = ''
ORDER BY
    P.PIPE_NAME,U.START_TIME
LIMIT 100
;