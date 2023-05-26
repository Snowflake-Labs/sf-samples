-------------------------------------------------
-- NAME:	 IMPL-IS-13-object-count.txt
-------------------------------------------------
-- DESCRIPTION:
--	counting of customers objects by type
--
-- OUTPUT:
--	objects, object kind, count
--
-- NEXT STEPS:
--	Use for runbook reporting
--
-- OPTIONS:
--	can narrow results to storage level
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
    COUNT(*)
FROM
	TABLE($TABLES)
--WHERE
--	DELETED IS NULL
--	TABLE_CATALOG = <NAME> AND
--  TABLE_SCHEMA = <NAME> AND
--  TABLE_OWNER = <NAME> AND
GROUP BY
    1,2,3
ORDER BY
	1,2,3;