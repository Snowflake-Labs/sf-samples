-------------------------------------------------
-- NAME:	 IMPL-AU-16-stored-proc.txt
-------------------------------------------------
-- DESCRIPTION:
--	stored procedure information
--
-- OUTPUT:
--	stored procedure, stored procedure information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to stored procedure level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-16-stored-proc.txt
-------------------------------------------------
-- List of stored procedures in account
-------------------------------------------------

SELECT
	FUNCTION_OWNER,
    FUNCTION_CATALOG,
    FUNCTION_SCHEMA,
    FUNCTION_NAME,
    ARGUMENT_SIGNATURE,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    CHARACTER_OCTET_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_PRECISION_RADIX,
    NUMERIC_SCALE,
    FUNCTION_LANGUAGE,
    FUNCTION_DEFINITION,
    VOLATILITY,
    IS_NULL_CALL
FROM
	TABLE($FUNCTIONS)
WHERE
--  NAME = '<DATABASE-NAME>' AND
--  NAME = '<SCHEMA-NAME>' AND
--  NAME = '<UDF-OR-SP-NAME>' AND
	DELETED IS NULL AND
    FUNCTION_LANGUAGE = 'JAVASCRIPT'
ORDER BY
    1,2,3;