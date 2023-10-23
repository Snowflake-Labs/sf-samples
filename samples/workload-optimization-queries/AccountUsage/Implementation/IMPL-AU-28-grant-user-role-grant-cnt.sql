-------------------------------------------------
-- NAME:	 IMPL-AU-28-grant-user-role-grant-cnt.txt
-------------------------------------------------
-- DESCRIPTION:
--	count of grants by user and role
--
-- OUTPUT:
--	user, role, count of grants
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
-- IMPL-AU-28-grant-user-role-grant-cnt.txt
-------------------------------------------------
-- Count of privileges to users/roles
-------------------------------------------------

SELECT
    U.GRANTEE_NAME USER_NAME,
    R.NAME ROLE,
    count(R.PRIVILEGE) PRIVS_GRANTED
FROM
	TABLE($GRANTS_TO_ROLES) R
	JOIN TABLE($GRANTS_TO_USERS) U ON R.NAME = U.ROLE
WHERE
--    U.GRANTEE_NAME =N <NAMNE> AND
    U.DELETED_ON IS NULL AND
    R.DELETED_ON IS NULL AND
    R.GRANTED_ON != 'ROLE'
GROUP BY
    1,2
ORDER BY
    1,2;