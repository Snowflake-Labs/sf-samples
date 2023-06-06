-------------------------------------------------
-- NAME:	 IMPL-AU-20-roles.txt
-------------------------------------------------
-- DESCRIPTION:
--	role information
--
-- OUTPUT:
--	role, role information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to role level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-20-roles.txt
-------------------------------------------------
-- List of roles in account
-------------------------------------------------

SELECT
    R.NAME,
    R.CREATED_ON,
    R.COMMENT
FROM
	TABLE($ROLES) R
WHERE
    DELETED_ON IS NULL
ORDER BY 
    1,2,3;