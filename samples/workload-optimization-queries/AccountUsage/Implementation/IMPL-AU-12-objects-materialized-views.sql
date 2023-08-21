-------------------------------------------------
-- NAME:	 IMPL-AU-12-objects-materialized-views.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers materialized views
--
-- OUTPUT:
--	materialized views, materialized view information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-12-materialized-views.txt
-------------------------------------------------
-- LISTS MATERIALIZED VIEWS IN ACCOUNT
-------------------------------------------------


SELECT
	T.TABLE_OWNER,
    T.TABLE_CATALOG,
    T.TABLE_SCHEMA,
    T.TABLE_NAME,
    T.table_type,
    T.is_transient,
    T.clustering_key,
    T.row_count,
    T.bytes,
    T.retention_time,
    S.ACTIVE_BYTES,
    S.TIME_TRAVEL_BYTES,
    S.FAILSAFE_BYTES,
    S.RETAINED_FOR_CLONE_BYTES,
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
    T.CREATED            
FROM
	TABLE($TABLES) T
    JOIN TABLE($TABLE_STORAGE_METRICS) S ON
        T.TABLE_CATALOG_ID = S.TABLE_CATALOG_ID AND
        T.TABLE_SCHEMA_ID = S.TABLE_SCHEMA_ID AND
        T.TABLE_ID = S.ID 
WHERE
--	TABLE_CATALOG = <NAME> AND
--  TABLE_SCHEMA = <NAME> AND
--  TABLE_NAME= <NAME> AND
--  TABLE_OWNER = <NAME> AND
    T.DELETED IS NULL AND
	T.TABLE_TYPE = 'MATERIALIZED VIEW'
ORDER BY
	1,2,3;