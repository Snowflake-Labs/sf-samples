-------------------------------------------------
-- NAME:	 IMPL-AU-04-databases.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers databases
--
-- OUTPUT:
--	database, database information
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
-- IMPL-AU-04-databases.txt
-------------------------------------------------
-- LISTS DATABASES IN ACCOUNT
-------------------------------------------------

SELECT
	DATABASE_OWNER,
	DATABASE_NAME,
	IS_TRANSIENT,
	COMMENT,
	RETENTION_TIME
FROM
	TABLE($DATABASES)
WHERE
--  DATABASE_NAME = <DATABASE-NAME> AND
	DELETED IS NULL
ORDER BY 
    1,2;