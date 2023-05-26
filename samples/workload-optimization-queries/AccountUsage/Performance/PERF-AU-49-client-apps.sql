-------------------------------------------------
-- NAME:	 PERF-AU-49-client-apps.txt
-------------------------------------------------
-- DESCRIPTION:
--	Lists accounts, warehouses, and client apps used
--					
-- OUTPUT:					
--	Account, warehouse name, and Client application, with count of usage
--	
-- OPTIONS:
--	Can filter on account names
--  Can filter on warehouse name
--  Can add select and filter for username
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------

select
	J.WAREHOUSE_NAME,
	CASE
		WHEN S.CLIENT_APPLICATION_ID IS NULL then 'NULL'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'jar%' then 'JAR'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'Tableau%' then 'TABLEAU'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'Alloy%' then 'ALLOY'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'Liberty%' then 'LIBERTY'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'Python%' then 'PYTHON'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'Apache%' then 'APACHE'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'SnowSight%' then 'SNOWSIGHT'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'JDBC%' then 'JDBC'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'ODBC%' then 'ODBC'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'Go%' then 'GO'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'JavaScript%' then 'JAVASCRIPT'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'SnowSQL%' then 'SNOWSQL'
		WHEN S.CLIENT_APPLICATION_ID LIKE 'Snowflake UI%' then 'WEBUI'
		ELSE S.CLIENT_APPLICATION_ID 
	END,
	count(*)
from
	TABLE($QUERY_HISTORY) J
	JOIN TABLE($SESSIONS) S ON S.SESSION_ID=J.SESSION_ID
WHERE
	J.START_TIME BETWEEN $TS_START AND $TS_END
group by
	1,2
having 
	count(*)>100
order by 
	3 desc
--limit 10
;
