-- =====================================================
-- SP_REFRESH_METADATA_CACHE
-- Refreshes IDR metadata cache tables from Snowflake tags
-- Dynamically discovers BRONZE tables with IDR_INCLUDE tag
-- Call this when:
--   - Adding new tables to IDR
--   - Changing column tags (IDR_IDENTIFIER values)
--   - Initial deployment
-- =====================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_REFRESH_METADATA_CACHE(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    DB VARCHAR DEFAULT DEPLOYMENT_DB;
    table_count INTEGER;
    column_count INTEGER;
    table_sql VARCHAR DEFAULT '';
    column_sql VARCHAR DEFAULT '';
    tbl_name VARCHAR;
    first_table BOOLEAN DEFAULT TRUE;
    tables_found INTEGER DEFAULT 0;
BEGIN
    EXECUTE IMMEDIATE 'SHOW TABLES IN SCHEMA ' || DB || '.BRONZE';

    LET show_res RESULTSET := (EXECUTE IMMEDIATE
        'SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))');
    LET show_cur CURSOR FOR show_res;

    FOR rec IN show_cur DO
        tbl_name := rec."name";

        LET tag_check RESULTSET := (EXECUTE IMMEDIATE
            'SELECT COUNT(*) AS CNT FROM TABLE(' || DB ||
            '.INFORMATION_SCHEMA.TAG_REFERENCES(''' || DB || '.BRONZE.' || tbl_name ||
            ''', ''TABLE'')) WHERE TAG_SCHEMA = ''SILVER'' AND TAG_NAME = ''IDR_INCLUDE'' AND TAG_VALUE = ''TRUE''');
        LET tag_cur CURSOR FOR tag_check;
        OPEN tag_cur;
        LET has_tag INTEGER := 0;
        FETCH tag_cur INTO has_tag;
        CLOSE tag_cur;

        IF (has_tag > 0) THEN
            tables_found := tables_found + 1;
            IF (first_table) THEN
                table_sql := 'SELECT OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, TAG_NAME, TAG_VALUE FROM TABLE(' || DB || '.INFORMATION_SCHEMA.TAG_REFERENCES(''' || DB || '.BRONZE.' || tbl_name || ''', ''TABLE'')) WHERE TAG_SCHEMA = ''SILVER''';
                column_sql := 'SELECT OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME, TAG_NAME, TAG_VALUE FROM TABLE(' || DB || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(''' || DB || '.BRONZE.' || tbl_name || ''', ''TABLE'')) WHERE TAG_SCHEMA = ''SILVER'' AND COLUMN_NAME IS NOT NULL';
                first_table := FALSE;
            ELSE
                table_sql := table_sql || ' UNION ALL SELECT OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, TAG_NAME, TAG_VALUE FROM TABLE(' || DB || '.INFORMATION_SCHEMA.TAG_REFERENCES(''' || DB || '.BRONZE.' || tbl_name || ''', ''TABLE'')) WHERE TAG_SCHEMA = ''SILVER''';
                column_sql := column_sql || ' UNION ALL SELECT OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME, TAG_NAME, TAG_VALUE FROM TABLE(' || DB || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(''' || DB || '.BRONZE.' || tbl_name || ''', ''TABLE'')) WHERE TAG_SCHEMA = ''SILVER'' AND COLUMN_NAME IS NOT NULL';
            END IF;
        END IF;
    END FOR;

    IF (tables_found = 0) THEN
        RETURN 'No tables with IDR_INCLUDE=TRUE found in ' || DB || '.BRONZE';
    END IF;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || DB || '.SILVER.IDR_CORE_TABLE_METADATA_CACHE';

    EXECUTE IMMEDIATE 'INSERT INTO ' || DB || '.SILVER.IDR_CORE_TABLE_METADATA_CACHE
        (TABLE_FQN, TABLE_DATABASE, TABLE_SCHEMA, TABLE_NAME, SOURCE_ROLE, PRIORITY, PRIMARY_KEY_COLUMN, CACHED_AT)
    WITH all_table_tags AS (' || table_sql || ')
    SELECT 
        OBJECT_DATABASE || ''.'' || OBJECT_SCHEMA || ''.'' || OBJECT_NAME AS TABLE_FQN,
        OBJECT_DATABASE AS TABLE_DATABASE,
        OBJECT_SCHEMA AS TABLE_SCHEMA,
        OBJECT_NAME AS TABLE_NAME,
        MAX(CASE WHEN TAG_NAME = ''IDR_SOURCE_ROLE'' THEN TAG_VALUE END) AS SOURCE_ROLE,
        MAX(CASE WHEN TAG_NAME = ''IDR_PRIORITY'' THEN TRY_CAST(TAG_VALUE AS INT) END) AS PRIORITY,
        MAX(CASE WHEN TAG_NAME = ''IDR_PRIMARY_KEY'' THEN TAG_VALUE END) AS PRIMARY_KEY_COLUMN,
        CURRENT_TIMESTAMP() AS CACHED_AT
    FROM all_table_tags
    WHERE OBJECT_NAME IN (
        SELECT DISTINCT OBJECT_NAME FROM all_table_tags 
        WHERE TAG_NAME = ''IDR_INCLUDE'' AND TAG_VALUE = ''TRUE''
    )
    GROUP BY 1, 2, 3, 4';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || DB || '.SILVER.IDR_CORE_COLUMN_METADATA_CACHE';

    EXECUTE IMMEDIATE 'INSERT INTO ' || DB || '.SILVER.IDR_CORE_COLUMN_METADATA_CACHE
        (TABLE_FQN, TABLE_NAME, COLUMN_NAME, IDENTIFIER_TYPE, EXTERNAL_KEY_TO, SEMANTIC_CATEGORY, CACHED_AT)
    WITH all_column_tags AS (' || column_sql || ')
    SELECT 
        OBJECT_DATABASE || ''.'' || OBJECT_SCHEMA || ''.'' || OBJECT_NAME AS TABLE_FQN,
        OBJECT_NAME AS TABLE_NAME,
        COLUMN_NAME,
        UPPER(TRIM(MAX(CASE WHEN TAG_NAME = ''IDR_IDENTIFIER'' THEN TAG_VALUE END))) AS IDENTIFIER_TYPE,
        MAX(CASE WHEN TAG_NAME = ''IDR_EXTERNAL_KEY'' THEN TAG_VALUE END) AS EXTERNAL_KEY_TO,
        MAX(CASE WHEN TAG_NAME = ''SEMANTIC_CATEGORY'' THEN TAG_VALUE END) AS SEMANTIC_CATEGORY,
        CURRENT_TIMESTAMP() AS CACHED_AT
    FROM all_column_tags
    WHERE TAG_NAME IN (''IDR_IDENTIFIER'', ''IDR_EXTERNAL_KEY'', ''SEMANTIC_CATEGORY'')
    GROUP BY 1, 2, 3
    HAVING MAX(CASE WHEN TAG_NAME = ''IDR_IDENTIFIER'' THEN TAG_VALUE END) IS NOT NULL';

    LET res1 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS CNT FROM ' || DB || '.SILVER.IDR_CORE_TABLE_METADATA_CACHE');
    LET c1 CURSOR FOR res1;
    OPEN c1;
    FETCH c1 INTO table_count;
    CLOSE c1;

    LET res2 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS CNT FROM ' || DB || '.SILVER.IDR_CORE_COLUMN_METADATA_CACHE');
    LET c2 CURSOR FOR res2;
    OPEN c2;
    FETCH c2 INTO column_count;
    CLOSE c2;

    RETURN 'Metadata cache refreshed: ' || table_count || ' tables (' || tables_found || ' discovered), ' || column_count || ' columns';
END;
$$;
