-------------------------------------------------
-- NAME:	 IMPL-IS-15-function.txt
-------------------------------------------------
-- DESCRIPTION:
--	function information
--
-- OUTPUT:
--	function, function information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to function level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
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
--	DELETED IS NULL AND
    FUNCTION_LANGUAGE != 'JAVASCRIPT'
ORDER BY
    1,2,3;