-------------------------------------------------
-- NAME:	 PERF-AU-83-partition-count-approx-size.sql
-------------------------------------------------
-- DESCRIPTION:
--	returns count of partitions in tables & approximate size of micropartition (in MB)
--  Due to need for dynamically generated sql statements, script uses temporary tables.
--
-- INPUT:
--  Define database, db+schema or db+schema+table name as input for v_input (line 47)
--
-- OUTPUT:
--	database, schema, table name, partition count, approximate partition size
--  & clustering information
--	
--
-- NEXT STEPS:
--	use information to determine whether table rebuild is warranted 
--
-- OPTIONS:
--	(1) script will retrieve all tables in database if only database name is used in v_input (line 47)
--	(2) script will retrieve all tables in schema if database name and schema name are used in v_input
--  (3) if v_input = 'database.schema.table', script will return only information for that one table
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 15MAR23  JP      Initial version
-------------------------------------------------
    
    
    DECLARE

    v_cnt INT DEFAULT 1;
    v_cnt_skipped INT DEFAULT 0;
    v_max INT;
    v_db STRING;
    v_sch STRING;
    v_tbl STRING;
    v_col STRING;
    v_sql TEXT;
    v_error TEXT DEFAULT '';
    res RESULTSET;


    BEGIN

    LET v_input  STRING  := 'database.schema.table';  -- Insert database, db+schema or fully qualified table name to investigate
    LET v_first  STRING  := SPLIT_PART(UPPER(v_input), '.', 1);
    LET v_second STRING  := SPLIT_PART(UPPER(v_input), '.', 2);
    LET v_third  STRING  := SPLIT_PART(UPPER(v_input), '.', 3);

    CREATE OR REPLACE TEMPORARY TABLE tmp_source_statements (id INT IDENTITY(1,1), sql TEXT);

    INSERT INTO tmp_source_statements (sql)
    SELECT 'CREATE OR REPLACE TEMPORARY TABLE output (
        table_catalog TEXT
        , table_schema TEXT
        , table_name TEXT
        , partition_count NUMBER(32, 0)
    );';

    INSERT INTO tmp_source_statements (sql)
    SELECT 'explain select top 1 * from '||t.table_catalog||'.'||t.table_schema||'.'||t.table_name||';'
    FROM snowflake.account_usage.tables t 
    WHERE  t.table_catalog = :v_first
        AND t.table_schema = CASE WHEN :v_second = '' THEN t.table_schema ELSE :v_second END
        AND t.table_name = CASE WHEN :v_third = '' THEN t.table_name ELSE :v_third END
        AND t.table_schema NOT IN ('INFORMATION_SCHEMA')
        AND t.table_type = 'BASE TABLE'
        AND t.deleted IS NULL;

    -- execute statements
    v_max := (SELECT max(id) FROM tmp_source_statements);

    FOR i IN 1 TO v_max DO
        BEGIN
        v_sql := (SELECT sql FROM tmp_source_statements WHERE id = :i);

        execute immediate(v_sql);

        insert into output (table_catalog, table_schema, table_name, partition_count) 
        select split_part("objects", '.', 1) as table_catalog
            , split_part("objects", '.', 2) as table_schema
            , split_part("objects", '.', 3) as table_name
            , "partitionsTotal" as partition_count
        from table(result_scan(last_query_id()))
        order by table_catalog 
            , table_schema 
            , table_name 
        limit 1;
        
        v_cnt := v_cnt+1;
                
        EXCEPTION 
        WHEN statement_error THEN
            v_error := v_error || SQLERRM||'
    ';
            v_cnt_skipped := v_cnt_skipped + 1;
        WHEN other THEN
            v_cnt_skipped := v_cnt_skipped + 1;      
        END;
        
    END FOR;

    v_sql := 'SELECT o.*
        , t.row_count
        , CAST(t.bytes/1024/1024 AS NUMBER(16, 2)) AS table_size_mb
        , CAST(CASE
                    WHEN o.partition_count = 0 THEN 0
                    ELSE (t.bytes / o.partition_count)/ 1024 / 1024
                END  AS NUMBER(16, 2))              AS bytes_per_partition_mb
        , t.clustering_key
        , t.auto_clustering_on
        , t.is_transient
        , t.retention_time
        , t.last_altered::DATE as last_altered_date
        , t.table_owner
    FROM   output o
    LEFT JOIN '||:v_first||'.information_schema.tables t
        ON o.table_catalog = t.table_catalog
        AND o.table_schema = t.table_schema
        AND o.table_name = t.table_name;'; 
        
        res := (EXECUTE IMMEDIATE (v_sql));
        RETURN TABLE (res);
    
    END;
