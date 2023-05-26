-------------------------------------------------
-- NAME:	 IMPL-AU-08-object-tables.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers tables
--
-- OUTPUT:
--	tables, table information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--	go to column view for more detail
--
-- OPTIONS:
--	can narrow results to object level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-08-tables.txt
-------------------------------------------------
-- LISTS TABLES IN ACCOUNT
-------------------------------------------------

SELECT
	TABLE_OWNER,
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    table_type,
    is_transient,
    clustering_key,
    row_count,
    bytes,
    retention_time,
    self_referencing_column_name,
    reference_generation,
    user_defined_type_CATALOG,
    user_defined_type_schema,
    user_defined_type_name,
    is_insertable_into,
    is_typed,
    commit_action,
    auto_clustering_on,
    comment,
    CREATED            
FROM
	TABLE($TABLES)
WHERE
--	TABLE_CATALOG = <NAME> AND
--  TABLE_SCHEMA = <NAME> AND
--  TABLE_NAME= <NAME> AND
--  TABLE_OWNER = <NAME> AND
	DELETED IS NULL AND
	TABLE_TYPE = 'BASE TABLE'
ORDER BY
	1,2,3;    
	
	
