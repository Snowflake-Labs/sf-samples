-------------------------------------------------
-- NAME:	 COST-IS-37-autoclustering.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily gs credits
--
-- OUTPUT:
--	auto clustering credits by account/day, database/stage/table
--
-- NEXT STEPS:
--	Use for reporting purposes
--	Look for outliers of runaway cost
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT
    start_time,
--    database_name,
--    schema_name,
    table_name,
    num_bytes_reclustered,
    num_rows_reclustered,
    credits_used
FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
       DATE_RANGE_START => dateadd('day',-2,current_date()),
       DATE_RANGE_END => current_date()))
ORDER BY 1,2;