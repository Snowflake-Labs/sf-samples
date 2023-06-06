-------------------------------------------------
-- NAME:	 IMPL-AU-05-schema.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers schemas
--
-- OUTPUT:
--	database, schemas
--
-- NEXT STEPS:
--	Use for runbook reporting
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-04-schema.txt
-------------------------------------------------
-- LISTS SCHEMAS IN ACCOUNT
-------------------------------------------------

SELECT
	CATALOG_NAME,
    SCHEMA_OWNER,
    SCHEMA_NAME,
    RETENTION_TIME,
    IS_TRANSIENT,
    IS_MANAGED_ACCESS,
    DEFAULT_CHARACTER_SET_CATALOG,
    DEFAULT_CHARACTER_SET_SCHEMA,
    DEFAULT_CHARACTER_SET_NAME,
    SQL_PATH,
    COMMENT
FROM
	TABLE($SCHEMATA) 
WHERE
    DELETED IS NULL
ORDER BY
    1,2,3;
