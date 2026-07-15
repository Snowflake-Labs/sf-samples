-- =============================================================================
-- RECOMMENDATION-DRIVEN TABLE DISCOVERY
-- Finds tables that match pending Meta recommendations
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- PROCEDURE: Discover tables matching pending recommendations
-- Returns tables grouped by which Meta use case they can satisfy
-- =============================================================================
CREATE OR REPLACE PROCEDURE discover_tables_for_recommendations(
    search_database VARCHAR DEFAULT NULL,
    search_schema VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    use_case_name VARCHAR,
    use_case_ref VARCHAR,
    required_event_type VARCHAR,
    required_parameters VARCHAR,
    database_name VARCHAR,
    schema_name VARCHAR,
    table_name VARCHAR,
    match_score INTEGER,
    matched_columns ARRAY,
    missing_required ARRAY,
    fit_assessment VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH pending_recos AS (
            SELECT 
                RECOMMENDATION_ID,
                USE_CASE_NAME,
                USE_CASE_REF,
                PRIMARY_EVENT_TYPE,
                REQUIRED_PARAMETERS,
                DESCRIPTION
            FROM V_PENDING_RECOMMENDATIONS
        ),
        table_column_analysis AS (
            SELECT 
                c.TABLE_CATALOG AS DB,
                c.TABLE_SCHEMA AS SCH,
                c.TABLE_NAME AS TBL,
                COUNT(DISTINCT CASE 
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%email%', '%mail%', 'em') THEN 'email'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%phone%', '%mobile%', 'ph') THEN 'phone'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%first%name%', 'fname', 'fn') THEN 'first_name'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%last%name%', 'lname', 'ln') THEN 'last_name'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%value%', '%amount%', '%total%', '%revenue%', '%price%') THEN 'value'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%currency%', 'curr') THEN 'currency'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%time%', '%date%', '%created%', '%timestamp%') THEN 'event_time'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%order%id%', '%transaction%id%') THEN 'order_id'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%user%id%', '%customer%id%', '%external%id%') THEN 'external_id'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%ip%', '%client%ip%') THEN 'ip_address'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%agent%', '%browser%', '%user%agent%') THEN 'user_agent'
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%ltv%', '%lifetime%value%', '%predicted%') THEN 'predicted_ltv'
                END) AS MATCH_SCORE,
                ARRAY_AGG(DISTINCT CASE 
                    WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%first%name%', '%last%name%', 
                        '%value%', '%amount%', '%total%', '%time%', '%date%', '%order%id%', '%user%id%',
                        '%ltv%', '%lifetime%', '%predicted%') 
                    THEN c.COLUMN_NAME 
                END) AS MATCHED_COLUMNS,
                MAX(CASE WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%value%', '%amount%', '%total%', '%revenue%', '%price%') THEN 1 ELSE 0 END) AS HAS_VALUE,
                MAX(CASE WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%time%', '%date%', '%created%', '%timestamp%') THEN 1 ELSE 0 END) AS HAS_TIME,
                MAX(CASE WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%ltv%', '%lifetime%value%', '%predicted%') THEN 1 ELSE 0 END) AS HAS_LTV,
                MAX(CASE WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%email%', '%mail%') THEN 1 ELSE 0 END) AS HAS_EMAIL,
                MAX(CASE WHEN LOWER(c.COLUMN_NAME) LIKE ANY ('%event%id%') THEN 1 ELSE 0 END) AS HAS_EVENT_ID
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE (c.TABLE_CATALOG = :search_database OR :search_database IS NULL)
            AND (c.TABLE_SCHEMA = :search_schema OR :search_schema IS NULL)
            AND c.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
            GROUP BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME
            HAVING MATCH_SCORE >= 2
        ),
        matched_tables AS (
            SELECT 
                r.USE_CASE_NAME,
                r.USE_CASE_REF,
                r.PRIMARY_EVENT_TYPE AS REQUIRED_EVENT_TYPE,
                (SELECT LISTAGG(p.value:key::VARCHAR, ', ') 
                 FROM TABLE(FLATTEN(r.REQUIRED_PARAMETERS)) p) AS REQUIRED_PARAMETERS,
                t.DB AS DATABASE_NAME,
                t.SCH AS SCHEMA_NAME,
                t.TBL AS TABLE_NAME,
                t.MATCH_SCORE,
                t.MATCHED_COLUMNS,
                CASE 
                    WHEN r.PRIMARY_EVENT_TYPE = 'Purchase' AND t.HAS_VALUE = 0 
                        THEN ARRAY_CONSTRUCT('value (required for Purchase events)')
                    WHEN r.PRIMARY_EVENT_TYPE = 'AppendValue' AND t.HAS_LTV = 0 
                        THEN ARRAY_CONSTRUCT('predicted_ltv (required for pLTV)')
                    WHEN r.PRIMARY_EVENT_TYPE = 'AppendValue' AND t.HAS_EVENT_ID = 0 
                        THEN ARRAY_CONSTRUCT('event_id (required for AppendValue)')
                    ELSE ARRAY_CONSTRUCT()
                END AS MISSING_REQUIRED,
                CASE 
                    WHEN t.MATCH_SCORE >= 5 AND ARRAY_SIZE(MISSING_REQUIRED) = 0 THEN 'EXCELLENT'
                    WHEN t.MATCH_SCORE >= 4 AND ARRAY_SIZE(MISSING_REQUIRED) = 0 THEN 'GOOD'
                    WHEN t.MATCH_SCORE >= 3 THEN 'MODERATE'
                    ELSE 'LOW'
                END AS FIT_ASSESSMENT
            FROM pending_recos r
            CROSS JOIN table_column_analysis t
            WHERE 
                (r.PRIMARY_EVENT_TYPE = 'Purchase' AND t.HAS_VALUE = 1 AND t.HAS_TIME = 1)
                OR (r.PRIMARY_EVENT_TYPE = 'Lead' AND t.HAS_EMAIL = 1)
                OR (r.PRIMARY_EVENT_TYPE = 'AppendValue' AND (t.HAS_LTV = 1 OR t.HAS_VALUE = 1))
                OR (r.PRIMARY_EVENT_TYPE NOT IN ('Purchase', 'Lead', 'AppendValue') AND t.MATCH_SCORE >= 2)
        )
        SELECT 
            USE_CASE_NAME,
            USE_CASE_REF,
            REQUIRED_EVENT_TYPE,
            REQUIRED_PARAMETERS,
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            MATCH_SCORE,
            MATCHED_COLUMNS,
            MISSING_REQUIRED,
            FIT_ASSESSMENT
        FROM matched_tables
        ORDER BY 
            USE_CASE_NAME,
            CASE FIT_ASSESSMENT 
                WHEN 'EXCELLENT' THEN 1 
                WHEN 'GOOD' THEN 2 
                WHEN 'MODERATE' THEN 3 
                ELSE 4 
            END,
            MATCH_SCORE DESC
    );
    RETURN TABLE(res);
END;
$$;

-- =============================================================================
-- PROCEDURE: Get detailed column mapping for a specific table and use case
-- =============================================================================
CREATE OR REPLACE PROCEDURE analyze_table_for_recommendation(
    use_case_ref VARCHAR,
    source_database VARCHAR,
    source_schema VARCHAR,
    source_table VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    meta_req VARIANT;
    column_analysis VARIANT;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'use_case_name', USE_CASE_NAME,
        'use_case_ref', USE_CASE_REF,
        'description', DESCRIPTION,
        'required_events', REQUIRED_EVENTS,
        'recommended_events', RECOMMENDED_EVENTS
    ) INTO meta_req
    FROM V_PENDING_RECOMMENDATIONS
    WHERE USE_CASE_REF = :use_case_ref
    LIMIT 1;
    
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'source_column', COLUMN_NAME,
        'data_type', DATA_TYPE,
        'suggested_target', 
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%mail%', 'em') THEN 'em'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%phone%', '%mobile%', 'ph') THEN 'ph'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%first%name%', 'fname', 'fn') THEN 'fn'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%last%name%', 'lname', 'ln') THEN 'ln'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%value%', '%amount%', '%total%', '%revenue%') THEN 'value'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%currency%') THEN 'currency'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%time%', '%date%', '%created%', '%timestamp%') THEN 'event_time'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%order%id%', '%transaction%id%') THEN 'order_id'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%user%id%', '%customer%id%') THEN 'external_id'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%ip%') THEN 'client_ip_address'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%agent%', '%browser%') THEN 'client_user_agent'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%ltv%', '%lifetime%', '%predicted%') THEN 'predicted_ltv'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%city%') THEN 'ct'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%state%', '%province%') THEN 'st'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%zip%', '%postal%') THEN 'zp'
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%country%') THEN 'country'
            ELSE NULL
        END,
        'requires_hashing',
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%first%', '%last%', '%city%', '%state%', '%zip%', '%country%') THEN TRUE
            ELSE FALSE
        END,
        'is_pii',
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%name%', '%address%', '%city%', '%state%', '%zip%') THEN TRUE
            ELSE FALSE
        END
    )) INTO column_analysis
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = :source_database
    AND TABLE_SCHEMA = :source_schema
    AND TABLE_NAME = :source_table;
    
    RETURN OBJECT_CONSTRUCT(
        'meta_requirements', meta_req,
        'source', OBJECT_CONSTRUCT(
            'database', source_database,
            'schema', source_schema,
            'table', source_table
        ),
        'column_analysis', column_analysis,
        'next_step', 'CALL generate_reco_config(''' || use_case_ref || ''', ''' || source_database || ''', ''' || source_schema || ''', ''' || source_table || ''')'
    );
END;
$$;
