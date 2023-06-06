-------------------------------------------------
-- NAME:	 IMPL-IS-79-repl-config.txt
-------------------------------------------------
-- DESCRIPTION:
--	Identifies replication accounts and primary
--
-- OUTPUT:
--	replication_group, snowflake_region, account_name, database_name,
--  comment, created,
--  is_primary, primary, replication_allowed_to_accounts, failover_allowed_to_accounts
--
-- OPTIONS:
--	Monitor for current primary and accounts for replication/failover
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------

SELECT	REPLICATION_GROUP, 
		SNOWFLAKE_REGION, 
		ACCOUNT_NAME, 
		DATABASE_NAME,
		COMMENT, CREATED,
		IS_PRIMARY, 
		PRIMARY, 
		REPLICATION_ALLOWED_TO_ACCOUNTS, 
		FAILOVER_ALLOWED_TO_ACCOUNTS
FROM	TABLE($REPLICATION_DATABASES)
WHERE	DATABASE_NAME = $DATABASE_NAME;