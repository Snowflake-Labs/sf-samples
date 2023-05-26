-------------------------------------------------
-- NAME:	 PERF-IS-75-repl-usage-hist.txt
-------------------------------------------------
-- DESCRIPTION:
--	USAGE HISTORY BY REPLICATION PHASES
--
-- NEXT STEPS:
--	Monitoring phases for status
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
-- Retrieve the database refresh history for the database that is currently active 
-- in the user session:

-- Only returns results for account administrators (users with the ACCOUNTADMIN role).
-- When calling an Information Schema table function, the session must have an 
-- INFORMATION_SCHEMA schema in use or the function name must be fully-qualified. 

-- Following is the list of phases in the order processed:
--SECONDARY_UPLOADING_INVENTORY
--PRIMARY_UPLOADING_METADATA
--PRIMARY_UPLOADING_DATA
--SECONDARY_DOWNLOADING_METADATA
--SECONDARY_DOWNLOADING_DATA
--COMPLETED / FAILED / CANCELED


select *
from table(information_schema.database_refresh_history());
