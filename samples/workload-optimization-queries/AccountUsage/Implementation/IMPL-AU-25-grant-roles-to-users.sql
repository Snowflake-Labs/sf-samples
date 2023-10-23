-------------------------------------------------
-- NAME:	 IMPL-AU-25-grant-roles-to-users.txt
-------------------------------------------------
-- DESCRIPTION:
--	roles granted to roles
--
-- OUTPUT:
--	grantor/granted role
--	roles granted to users
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
	
--------------------------------------------------
-- IMPL-AU-25-grant-roles-to-users.txt
-------------------------------------------------
--Grant of roles to users
-------------------------------------------------

SELECT
    GRANTED_TO,
    ROLE GRANTED_ROLE,
    GRANTED_BY,
    GRANTEE_NAME,
    CREATED_ON
FROM
	TABLE($GRANTS_TO_USERS)
WHERE
--  GRANTED_TO IN ('USERNAME') AND
    DELETED_ON IS NULL
ORDER BY
    1,2;