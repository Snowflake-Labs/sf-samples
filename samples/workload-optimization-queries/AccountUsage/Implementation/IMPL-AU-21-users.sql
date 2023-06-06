-------------------------------------------------
-- NAME:	 IMPL-AU-21-users.txt
-------------------------------------------------
-- DESCRIPTION:
--	user information
--
-- OUTPUT:
--	user, user information
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
-- IMPL-AU-21-users.txt
-------------------------------------------------
-- List of users in account
-------------------------------------------------

SELECT
    NAME AS USER_NAME,
	LOGIN_NAME,
    DISPLAY_NAME,
    DEFAULT_ROLE,
    DEFAULT_WAREHOUSE,
    PASSWORD_LAST_SET_TIME,
    LAST_SUCCESS_LOGIN,
    DISABLED,
    BYPASS_MFA_UNTIL,
    HAS_RSA_PUBLIC_KEY,
    EXT_AUTHN_UID,
    SNOWFLAKE_LOCK,
    HAS_PASSWORD,
    MUST_CHANGE_PASSWORD,
    DEFAULT_NAMESPACE,
    BYPASS_MFA_UNTIL,
    EXT_AUTHN_DUO,
    EXPIRES_AT,
    LOCKED_UNTIL_TIME,
    PASSWORD_LAST_SET_TIME
FROM
	TABLE($USERS)
WHERE
       DELETED_ON IS NULL 
ORDER BY 1;



