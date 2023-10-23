-------------------------------------------------
-- NAME:	 PERF-AU-27-failed-logins.txt
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
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
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
FROM TABLE($LOGIN_HISTORY)
where EVENT_TIMESTAMP between $TS_START AND $TS_END
AND IS_SUCCESS='false'
order by EVENT_TIMESTAMP
;