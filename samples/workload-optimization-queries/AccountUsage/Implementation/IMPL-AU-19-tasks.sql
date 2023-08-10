-------------------------------------------------
-- NAME:	 IMPL-AU-19-tasks.txt
-------------------------------------------------
-- DESCRIPTION:
--	task information
--
-- OUTPUT:
--	task, task information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to task level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-19-tasks.txt
-------------------------------------------------
-- List of tasks in account
-------------------------------------------------

SELECT
	DATABASE_NAME,
	SCHEMA_NAME,
    NAME TASK_NAME,
    QUERY_TEXT,
    CONDITION_TEXT
FROM
	TABLE($TASK_HISTORY)
--WHERE
--  DATABASE_NAME = '<DATABASE-NAME>' AND
--  SCHEMA_NAME = '<SCHEMA-NAME>' AND
--  NAME = '<TASK-NAME>' AND
GROUP BY 
    1,2,3,4,5
ORDER BY
    1,2,3;