-------------------------------------------------
-- NAME:	 IMPL-IS-19-tasks.txt
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
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	

SELECT
	DATABASE_NAME,
	SCHEMA_NAME,
    NAME TASK_NAME,
    QUERY_TEXT,
    CONDITION_TEXT
FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.TASK_HISTORY(
        scheduled_time_range_start=> dateadd('day',-6,current_timestamp()),
        scheduled_time_range_end=> current_timestamp()))
--WHERE
--  DATABASE_NAME = '<DATABASE-NAME>' AND
--  SCHEMA_NAME = '<SCHEMA-NAME>' AND
--  NAME = '<TASK-NAME>' AND
GROUP BY 
    1,2,3,4,5
ORDER BY
    1,2,3;
    
    
    
--TASK_HISTORY(
--      [ SCHEDULED_TIME_RANGE_START => <constant_expr> ]
--      [, SCHEDULED_TIME_RANGE_END => <constant_expr> ]
--      [, RESULT_LIMIT => <integer> ]
--      [, TASK_NAME => '<string>' ] )