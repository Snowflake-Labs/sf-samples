-- =============================================================================
-- META CAPI DISCOVERY MODULE
-- Identifies candidate tables and maps columns to Meta event schema
-- =============================================================================

USE DATABASE META_CAPI_DB;
USE SCHEMA PIPELINE;

-- =============================================================================
-- DISCOVER CANDIDATE TABLES
-- Scans a target database/schema for tables with columns matching Meta event fields
-- Uses IDENTIFIER() to dynamically query the correct INFORMATION_SCHEMA
-- =============================================================================
CREATE OR REPLACE PROCEDURE DISCOVER_CAPI_CANDIDATE_TABLES(
    TARGET_DATABASE VARCHAR,
    TARGET_SCHEMA VARCHAR
)
RETURNS TABLE (
    TABLE_CATALOG VARCHAR,
    TABLE_SCHEMA VARCHAR,
    TABLE_NAME VARCHAR,
    ROW_COUNT NUMBER,
    MATCH_SCORE VARCHAR,
    MATCHED_COLUMNS VARCHAR,
    SUGGESTED_EVENT_TYPE VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
    columns_table VARCHAR;
    tables_table VARCHAR;
BEGIN
    columns_table := :TARGET_DATABASE || '.INFORMATION_SCHEMA.COLUMNS';
    tables_table := :TARGET_DATABASE || '.INFORMATION_SCHEMA.TABLES';
    
    res := (
        SELECT 
            c.TABLE_CATALOG,
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            t.ROW_COUNT,
            CASE 
                WHEN SUM(CASE WHEN LOWER(c.COLUMN_NAME) IN ('email', 'em', 'hashed_email', 'email_hash') THEN 1 ELSE 0 END) > 0
                     AND SUM(CASE WHEN LOWER(c.COLUMN_NAME) IN ('event_time', 'timestamp', 'created_at', 'order_date', 'purchase_date') THEN 1 ELSE 0 END) > 0
                     AND SUM(CASE WHEN LOWER(c.COLUMN_NAME) IN ('value', 'revenue', 'total', 'amount', 'order_total', 'price') THEN 1 ELSE 0 END) > 0
                THEN 'EXCELLENT'
                WHEN SUM(CASE WHEN LOWER(c.COLUMN_NAME) IN ('email', 'em', 'hashed_email', 'phone', 'ph') THEN 1 ELSE 0 END) > 0
                     AND SUM(CASE WHEN LOWER(c.COLUMN_NAME) IN ('event_time', 'timestamp', 'created_at', 'order_date') THEN 1 ELSE 0 END) > 0
                THEN 'GOOD'
                WHEN SUM(CASE WHEN LOWER(c.COLUMN_NAME) IN ('email', 'em', 'hashed_email', 'phone', 'first_name', 'last_name') THEN 1 ELSE 0 END) > 0
                THEN 'MODERATE'
                ELSE 'LOW'
            END AS MATCH_SCORE,
            LISTAGG(
                CASE WHEN LOWER(c.COLUMN_NAME) IN (
                    'email', 'em', 'hashed_email', 'email_hash', 'phone', 'ph',
                    'first_name', 'fn', 'last_name', 'ln', 'zip', 'zp', 'city', 'ct',
                    'state', 'st', 'country', 'value', 'revenue', 'total', 'amount',
                    'currency', 'order_id', 'event_time', 'timestamp', 'created_at',
                    'order_date', 'purchase_date', 'event_name', 'action_source'
                ) THEN c.COLUMN_NAME END,
                ', '
            ) AS MATCHED_COLUMNS,
            CASE
                WHEN SUM(CASE WHEN LOWER(c.COLUMN_NAME) IN ('value', 'revenue', 'total', 'amount', 'order_total') THEN 1 ELSE 0 END) > 0
                THEN 'Purchase'
                WHEN SUM(CASE WHEN LOWER(c.COLUMN_NAME) LIKE '%lead%' OR LOWER(c.COLUMN_NAME) LIKE '%signup%' THEN 1 ELSE 0 END) > 0
                THEN 'Lead'
                ELSE 'ViewContent'
            END AS SUGGESTED_EVENT_TYPE
        FROM IDENTIFIER(:columns_table) c
        JOIN IDENTIFIER(:tables_table) t
            ON c.TABLE_CATALOG = t.TABLE_CATALOG
            AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
            AND c.TABLE_NAME = t.TABLE_NAME
        WHERE c.TABLE_SCHEMA = :TARGET_SCHEMA
            AND t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, t.ROW_COUNT
        HAVING MATCH_SCORE IN ('EXCELLENT', 'GOOD', 'MODERATE')
        ORDER BY 
            CASE MATCH_SCORE 
                WHEN 'EXCELLENT' THEN 1 
                WHEN 'GOOD' THEN 2 
                WHEN 'MODERATE' THEN 3 
            END
    );
    RETURN TABLE(res);
END;
$$;

-- =============================================================================
-- ANALYZE TABLE MAPPING
-- Deep analysis of a specific table for Meta event mapping
-- =============================================================================
CREATE OR REPLACE PROCEDURE analyze_table_mapping(
    source_database VARCHAR,
    source_schema VARCHAR,
    source_table VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    col_mappings VARIANT;
    columns_ref VARCHAR;
BEGIN
    columns_ref := :source_database || '.INFORMATION_SCHEMA.COLUMNS';
    
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'source_column', COLUMN_NAME,
        'data_type', DATA_TYPE,
        'suggested_target', 
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE '%email%' OR LOWER(COLUMN_NAME) = 'em' THEN 'EM (email hash)'
            WHEN LOWER(COLUMN_NAME) LIKE '%phone%' OR LOWER(COLUMN_NAME) = 'ph' THEN 'PH (phone hash)'
            WHEN LOWER(COLUMN_NAME) LIKE '%first%name%' OR LOWER(COLUMN_NAME) = 'fn' THEN 'FN (first name hash)'
            WHEN LOWER(COLUMN_NAME) LIKE '%last%name%' OR LOWER(COLUMN_NAME) = 'ln' THEN 'LN (last name hash)'
            WHEN LOWER(COLUMN_NAME) LIKE '%amount%' OR LOWER(COLUMN_NAME) LIKE '%value%' OR LOWER(COLUMN_NAME) LIKE '%total%' THEN 'VALUE'
            WHEN LOWER(COLUMN_NAME) LIKE '%currency%' THEN 'CURRENCY'
            WHEN LOWER(COLUMN_NAME) LIKE '%event%' OR LOWER(COLUMN_NAME) LIKE '%action%' THEN 'EVENT_NAME'
            WHEN LOWER(COLUMN_NAME) LIKE '%time%' OR LOWER(COLUMN_NAME) LIKE '%date%' OR LOWER(COLUMN_NAME) LIKE '%created%' THEN 'EVENT_TIME'
            WHEN LOWER(COLUMN_NAME) LIKE '%order%id%' THEN 'ORDER_ID'
            WHEN LOWER(COLUMN_NAME) LIKE '%user%id%' OR LOWER(COLUMN_NAME) LIKE '%customer%id%' THEN 'EXTERNAL_ID'
            WHEN LOWER(COLUMN_NAME) LIKE '%ip%' THEN 'CLIENT_IP_ADDRESS'
            WHEN LOWER(COLUMN_NAME) LIKE '%agent%' OR LOWER(COLUMN_NAME) LIKE '%browser%' THEN 'CLIENT_USER_AGENT'
            WHEN LOWER(COLUMN_NAME) LIKE '%city%' THEN 'CT (city hash)'
            WHEN LOWER(COLUMN_NAME) LIKE '%state%' THEN 'ST (state hash)'
            WHEN LOWER(COLUMN_NAME) LIKE '%zip%' OR LOWER(COLUMN_NAME) LIKE '%postal%' THEN 'ZP (zip hash)'
            WHEN LOWER(COLUMN_NAME) LIKE '%country%' THEN 'COUNTRY'
            ELSE 'UNMAPPED'
        END,
        'requires_hashing', 
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%first%', '%last%', '%city%', '%state%', '%zip%') THEN TRUE
            ELSE FALSE
        END
    )) INTO col_mappings
    FROM IDENTIFIER(:columns_ref)
    WHERE TABLE_SCHEMA = :source_schema
    AND TABLE_NAME = :source_table;

    result := OBJECT_CONSTRUCT(
        'source', OBJECT_CONSTRUCT(
            'database', :source_database,
            'schema', :source_schema,
            'table', :source_table
        ),
        'mapping_suggestions', col_mappings,
        'sample_query', CONCAT(
            'SELECT * FROM ', :source_database, '.', :source_schema, '.', :source_table, ' LIMIT 10'
        )
    );
    
    RETURN result;
END;
$$;
