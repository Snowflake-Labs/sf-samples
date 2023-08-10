-------------------------------------------------
-- NAME:	 IMPL-AU-26-grant-user-role-cnt.txt
-------------------------------------------------
-- DESCRIPTION:
--	count of roles granted to users
--
-- OUTPUT:
--	user, count of roles
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to user level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-26-grant-user-role-cnt.txt
-------------------------------------------------
-- Count of Roles granted to users
-------------------------------------------------

SELECT
    GRANTEE_NAME,
    count(*) ROLES_GRANTED
FROM
	TABLE($GRANTS_TO_USERS)
WHERE
--  GRANTED_TO IN ('USERNAME') AND
    DELETED_ON IS NULL
group by
    1
ORDER BY
    1;