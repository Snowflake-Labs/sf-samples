-------------------------------------------------
-- NAME:	 IMPL-IS-34-table-cluster-count.txt
-------------------------------------------------
-- DESCRIPTION:
--	count of clustered versus not clustered
--
-- OUTPUT:
--	schema, count by category
--
-- NEXT STEPS:
--	use information to determine customers habit of using cluster keys
--
-- OPTIONS:
--	can narrow results to schema level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    table_type,
	SUM(CASE WHEN CLUSTERING_KEY IS NOT NULL THEN 1 ELSE 0 END) AS CLUSTERED,
	SUM(CASE WHEN CLUSTERING_KEY IS NOT NULL THEN 0 ELSE 1 END) AS NOT_CLUSTERED
FROM
	TABLE($TABLES)
WHERE
	DELETED IS NULL AND
	TABLE_TYPE = 'TABLE'
--	TABLE_CATALOG = <NAME> AND
--  TABLE_SCHEMA = <NAME> AND
--  TABLE_OWNER = <NAME> AND
GROUP BY
    1,2,3
ORDER BY
	1,2,3;
	
	
