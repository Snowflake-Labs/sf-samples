-------------------------------------------------
-- NAME:	 COST-AU-37-autoclustering.txt
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
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
SELECT
    start_time,
    database_name,
    schema_name,
    table_name,
    num_bytes_reclustered,
    num_rows_reclustered,
    credits_used
FROM
	TABLE($AUTOMATIC_CLUSTERING_HISTORY)
where
    start_time between $TS_START and $TS_END
ORDER BY 1,2;