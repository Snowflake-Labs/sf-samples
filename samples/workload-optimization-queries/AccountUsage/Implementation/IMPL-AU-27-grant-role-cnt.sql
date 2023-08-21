-------------------------------------------------
-- NAME:	 IMPL-AU-27-grant-role-cnt.txt
-------------------------------------------------
-- DESCRIPTION:
--	count of roles granted
--
-- OUTPUT:
--	role, count of grants
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
-- IMPL-AU-27-grant-role-cnt.txt
-------------------------------------------------
-- Count of Roles granted to roles
-------------------------------------------------

SELECT
    NAME ROLE,
    count(*) ROLE_GRANTED
FROM
	TABLE($GRANTS_TO_ROLES)
WHERE
--	AOR.NAME = $ACCOUNT_NAME AND
--    G.DELETED_ON IS NULL AND
    DELETED_ON IS NULL AND
    GRANTED_ON = 'ROLE'
GROUP BY
    1
ORDER BY
    1;