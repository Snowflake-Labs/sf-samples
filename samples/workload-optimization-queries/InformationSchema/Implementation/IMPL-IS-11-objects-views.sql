-------------------------------------------------
-- NAME:	 IMPL-IS-11-objects-views.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers views
--
-- OUTPUT:
--	views, view information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	

SELECT
	T.TABLE_OWNER,
    T.TABLE_CATALOG,
    T.TABLE_SCHEMA,
    T.TABLE_NAME,
    T.table_type,
    T.is_transient,
--    T.clustering_key,
--    T.row_count,
--    T.bytes,
    T.retention_time,
    T.self_referencing_column_name,
    T.reference_generation,
    T.user_defined_type_CATALOG,
    T.user_defined_type_schema,
    T.user_defined_type_name,
    T.is_insertable_into,
    T.is_typed,
    T.commit_action,
    T.auto_clustering_on,
    T.comment,
    V.VIEW_DEFINITION,
    V.CHECK_OPTION,
    V.IS_UPDATABLE,
    V.INSERTABLE_INTO,
    V.IS_SECURE,
    T.CREATED            
FROM
	TABLE($TABLES) T
    JOIN TABLE($VIEWS) V ON T.TABLE_ID = V.TABLE_ID
WHERE
--	TABLE_CATALOG = <NAME> AND
--  TABLE_SCHEMA = <NAME> AND
--  TABLE_NAME= <NAME> AND
--  TABLE_OWNER = <NAME> AND
	T.DELETED IS NULL AND
	T.TABLE_TYPE = 'VIEW'
ORDER BY
	1,2,3; 