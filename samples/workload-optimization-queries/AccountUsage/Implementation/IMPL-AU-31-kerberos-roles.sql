-------------------------------------------------
-- NAME:	 IMPL-AU-31-kerberos-roles.txt
-------------------------------------------------
-- DESCRIPTION:
--	special grant reporting
--	count of total grants at role level, where customer duplicates all grants into a 
--	single role	from multiple roles
--
-- OUTPUT:
--	user, role, count of grants
--
-- NEXT STEPS:
--	work with customer to eliminate duplicate roles
--	work with customer to leverage secondary role feature
--
-- OPTIONS:
--	can narrow results to user level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
select U.GRANTEE_NAME AS USERNAME,
U.ROLE,
COUNT(*) AS NUMBER_OF_GRANTS
from TABLE($GRANTS_TO_USERS) U
join TABLE($GRANTS_TO_ROLES) R
ON U.ROLE = R.GRANTEE_NAME
WHERE U.DELETED_ON IS NULL
AND R.DELETED_ON IS NULL
AND U.ROLE LIKE '%'||U.GRANTEE_NAME
AND R.GRANTEE_NAME LIKE '%'||U.GRANTEE_NAME
GROUP BY 1,2
ORDER BY 1,2;