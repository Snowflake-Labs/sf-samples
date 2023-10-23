-------------------------------------------------
-- NAME:	 PERF-IS-27-failed-logins.txt
-------------------------------------------------
-- DESCRIPTION:
--	Login information
--
-- OUTPUT:
--	Who, where, when of what happened with logins
--
-- NEXT STEPS:
--	Use information to narrow analysis and identify volume of issues
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT EVENT_TIMESTAMP
	, first_authentication_factor
	, first_authentication_factor
	, client_ip
	, reported_client_type
	, reported_client_version
	, error_code
    , error_message
	, event_type
	, event_id
	, is_success
	, user_name 
FROM 
	TABLE(information_schema.login_history(
		TIME_RANGE_START=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		TIME_RANGE_END=>CURRENT_TIMESTAMP()))
WHERE IS_SUCCESS='false'
order by EVENT_TIMESTAMP
;