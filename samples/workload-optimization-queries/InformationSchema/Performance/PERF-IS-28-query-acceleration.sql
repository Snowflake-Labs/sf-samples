-------------------------------------------------
-- NAME:	 PERF-IS-28-query-acceleration.txt
-------------------------------------------------
-- DESCRIPTION:
--	For large tables, where columns outside cluster key are highly selective 100-200K 
--	distinct values, then use system function to see if there is benefit using service
--
-- NEXT STEPS:
--	Review reference material to further understand Search Optimization
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
select system$estimate_query_acceleration(‘query_id’);

