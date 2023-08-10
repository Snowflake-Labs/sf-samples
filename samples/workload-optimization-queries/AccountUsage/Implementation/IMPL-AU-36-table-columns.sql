-------------------------------------------------
-- NAME:	 IMPL-AU-35-table-columns.txt
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
	T.TABLE_OWNER,
	T.TABLE_CATALOG DATABASE_NAME,
	T.TABLE_SCHEMA SCHEMA_NAME, 
	T.TABLE_NAME,
	T.TABLE_TYPE,
    T.TABLE_ID,
    C.COLUMN_ID,
    C.COLUMN_NAME,
    C.DATA_TYPE,
    C.ORDINAL_POSITION,
    T.ROW_COUNT,
    T.BYTES
FROM
	TABLE($TABLES) T
	JOIN TABLE($COLUMNS) C ON T.TABLE_ID = C.TABLE_ID
WHERE
--  T.TABLE_CATALOG = <DATABASE-NAME> AND
--  T.TABLE_SCHEMA = <SCHEMA-NAME> AND
--  T.TABLE_NAME = <TABLE-NAME> AND
	T.TABLE_TYPE = 'TABLE'
ORDER BY
	1,2,3,6,10;