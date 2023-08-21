-------------------------------------------------
-- NAME:	 PERF-AU-47-all-deployment-last-login.txt
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

select warehouse_name,user_name,max(start_time)
from  TABLE($QUERY_HISTORY)
group by 1,2
order by 1,3 desc;