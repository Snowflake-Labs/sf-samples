-------------------------------------------------
-- NAME:	 IMPL-AU-17-file-format.txt
-------------------------------------------------
-- DESCRIPTION:
--	file format information
--
-- OUTPUT:
--	file format, file format information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to file format level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-17-file-formats.txt
-------------------------------------------------
-- List of file formats in account
-------------------------------------------------

SELECT
	FILE_FORMAT_OWNER,
	FILE_FORMAT_CATALOG,
    FILE_FORMAT_SCHEMA,
	FILE_FORMAT_NAME, 
    FILE_FORMAT_TYPE,
    RECORD_DELIMITER,
    FIELD_DELIMITER,
    SKIP_HEADER,
    DATE_FORMAT,
    TIME_FORMAT,
    BINARY_FORMAT,
    ESCAPE,
    ESCAPE_UNENCLOSED_FIELD,
    TRIM_SPACE,
    FIELD_OPTIONALLY_ENCLOSED_BY,
    NULL_IF,
    COMPRESSION,
    ERROR_ON_COLUMN_COUNT_MISMATCH
FROM
	TABLE($FILE_FORMATS)
WHERE
--	FILE_FORMAT_OWNER = <NAME> AND
--	FILE_FORMAT_CATALOG = <NAME> AND
--  FILE_FORMAT_SCHEMA = <NAME> AND
--	FILE_FORMAT_NAME = <NAME> AND
	DELETED IS NULL 
ORDER BY 
    1,2,3,4;