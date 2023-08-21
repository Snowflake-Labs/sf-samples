-------------------------------------------------
-- NAME:	 IMPL-AU-22-grant-role-to-role.txt
-------------------------------------------------
-- DESCRIPTION:
--	roles granted to roles
--
-- OUTPUT:
--	grantor/grantee role
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
-- IMPL-AU-22-grant-role-to-roles.txt
-------------------------------------------------
-- Roles granted to roles
-------------------------------------------------

SELECT
    NAME ROLE,
    PRIVILEGE,
    TABLE_CATALOG,
    TABLE_SCHEMA,
    GRANTED_ON,
    GRANTED_TO,
    GRANT_OPTION,
    GRANTED_BY,
    GRANTEE_NAME,
    GRANT_OPTION,
    CREATED_ON
FROM
	TABLE($GRANTS_TO_ROLES)
WHERE
--    G.DELETED_ON IS NULL AND
    DELETED_ON IS NULL AND
    GRANTED_ON = 'ROLE'
ORDER BY 1,2;