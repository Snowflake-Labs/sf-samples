-------------------------------------------------
-- NAME:	 PERF-IS-47-all-deployment-last-login.txt
-------------------------------------------------
-- DESCRIPTION:
--	Lists last access by warehouse and username
--					
-- OUTPUT:					
--	Lists last access by warehouse and username
--	
-- NEXT STEPS:					
--	Determine account usage, or non usage
--
-- OPTIONS:
--	Run for each account
--  Can modify to show last access on each warehouse
--  CAN ADD FOILTER TO SEE SPECIFIC USER
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 11APR22	WNA		created/updated for repository
-------------------------------------------------


SELECT warehouse_name,user_name,max(start_time)
FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,current_timestamp()),
		END_TIME_RANGE_END=>current_timestamp()))
group by 1,2
order by 1,3 desc;