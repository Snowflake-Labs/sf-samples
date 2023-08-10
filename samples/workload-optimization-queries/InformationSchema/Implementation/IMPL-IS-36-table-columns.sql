-------------------------------------------------
-- NAME:	 IMPL-IS-35-table-columns.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers table columns
--
-- OUTPUT:
--	account, tables, column information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	account filter = specific accounts
--	no account filter = all accounts
--	can narrow results to object level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT
	C.TABLE_CATALOG DATABASE_NAME,
	C.TABLE_SCHEMA SCHEMA_NAME, 
	C.TABLE_NAME,
    C.COLUMN_NAME,
    C.DATA_TYPE,
    C.ORDINAL_POSITION
FROM
	TABLE($COLUMNS) C 
--WHERE
--  T.TABLE_CATALOG = <DATABASE-NAME> AND
--  T.TABLE_SCHEMA = <SCHEMA-NAME> AND
--  T.TABLE_NAME = <TABLE-NAME>
ORDER BY
	1,2,3,6;