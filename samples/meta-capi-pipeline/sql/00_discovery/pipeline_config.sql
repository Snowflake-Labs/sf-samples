-- =============================================================================
-- META CAPI PIPELINE CONFIG GENERATOR
-- Generates reviewable config files for human verification before deployment
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- GENERATE PIPELINE CONFIG (Human Review)
-- Creates a config object for review - does NOT create any objects
-- =============================================================================
CREATE OR REPLACE PROCEDURE generate_pipeline_config(
    source_database VARCHAR,
    source_schema VARCHAR,
    source_table VARCHAR,
    event_type VARCHAR DEFAULT 'Purchase',
    action_source VARCHAR DEFAULT 'website'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    config VARIANT;
    col_mappings VARIANT;
BEGIN
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'source_column', COLUMN_NAME,
        'source_type', DATA_TYPE,
        'target_column', 
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE '%email%' OR LOWER(COLUMN_NAME) = 'em' THEN 'EM'
            WHEN LOWER(COLUMN_NAME) LIKE '%phone%' OR LOWER(COLUMN_NAME) = 'ph' THEN 'PH'
            WHEN LOWER(COLUMN_NAME) LIKE '%first%name%' OR LOWER(COLUMN_NAME) = 'fn' THEN 'FN'
            WHEN LOWER(COLUMN_NAME) LIKE '%last%name%' OR LOWER(COLUMN_NAME) = 'ln' THEN 'LN'
            WHEN LOWER(COLUMN_NAME) LIKE '%amount%' OR LOWER(COLUMN_NAME) LIKE '%value%' OR LOWER(COLUMN_NAME) LIKE '%total%' OR LOWER(COLUMN_NAME) LIKE '%revenue%' THEN 'VALUE'
            WHEN LOWER(COLUMN_NAME) LIKE '%currency%' THEN 'CURRENCY'
            WHEN LOWER(COLUMN_NAME) LIKE '%time%' OR LOWER(COLUMN_NAME) LIKE '%date%' OR LOWER(COLUMN_NAME) LIKE '%created%' THEN 'EVENT_TIME'
            WHEN LOWER(COLUMN_NAME) LIKE '%order%id%' THEN 'ORDER_ID'
            WHEN LOWER(COLUMN_NAME) LIKE '%user%id%' OR LOWER(COLUMN_NAME) LIKE '%customer%id%' THEN 'EXTERNAL_ID'
            WHEN LOWER(COLUMN_NAME) LIKE '%ip%' THEN 'CLIENT_IP_ADDRESS'
            WHEN LOWER(COLUMN_NAME) LIKE '%agent%' OR LOWER(COLUMN_NAME) LIKE '%browser%' THEN 'CLIENT_USER_AGENT'
            WHEN LOWER(COLUMN_NAME) LIKE '%city%' THEN 'CT'
            WHEN LOWER(COLUMN_NAME) LIKE '%state%' OR LOWER(COLUMN_NAME) LIKE '%province%' THEN 'ST'
            WHEN LOWER(COLUMN_NAME) LIKE '%zip%' OR LOWER(COLUMN_NAME) LIKE '%postal%' THEN 'ZP'
            WHEN LOWER(COLUMN_NAME) LIKE '%country%' THEN 'COUNTRY'
            WHEN LOWER(COLUMN_NAME) LIKE '%gender%' THEN 'GE'
            WHEN LOWER(COLUMN_NAME) LIKE '%birth%' OR LOWER(COLUMN_NAME) LIKE '%dob%' THEN 'DB'
            ELSE NULL
        END,
        'requires_hashing', 
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%first%', '%last%', '%city%', '%state%', '%zip%', '%gender%', '%birth%', '%country%') THEN TRUE
            ELSE FALSE
        END,
        'include', 
        CASE 
            WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%first%', '%last%', '%amount%', '%value%', '%total%', '%time%', '%date%', '%created%', '%order%', '%user%id%', '%customer%') THEN TRUE
            ELSE FALSE
        END,
        'notes', ''
    )) INTO col_mappings
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = :source_database
    AND TABLE_SCHEMA = :source_schema
    AND TABLE_NAME = :source_table;

    config := OBJECT_CONSTRUCT(
        '_metadata', OBJECT_CONSTRUCT(
            'generated_at', CURRENT_TIMESTAMP(),
            'generated_by', CURRENT_USER(),
            'status', 'PENDING_REVIEW',
            'instructions', 'Review mappings below. Edit as needed, then call deploy_pipeline_from_config() to activate.'
        ),
        'source', OBJECT_CONSTRUCT(
            'database', :source_database,
            'schema', :source_schema,
            'table', :source_table,
            'full_path', CONCAT(:source_database, '.', :source_schema, '.', :source_table)
        ),
        'target', OBJECT_CONSTRUCT(
            'database', 'META_CAPI_DB',
            'schema', 'PIPELINE', 
            'table', 'META_CAPI_EVENTS'
        ),
        'event_settings', OBJECT_CONSTRUCT(
            'event_type', :event_type,
            'action_source', :action_source
        ),
        'column_mappings', col_mappings,
        'pipeline_settings', OBJECT_CONSTRUCT(
            'pipeline_name', UPPER(REPLACE(:source_table, ' ', '_')) || '_TO_META',
            'schedule', 'ON_NEW_DATA',
            'batch_size', 10000,
            'enabled', FALSE
        ),
        'validation_rules', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('rule', 'EVENT_TIME must not be NULL', 'severity', 'ERROR'),
            OBJECT_CONSTRUCT('rule', 'At least one PII field (EM, PH, or EXTERNAL_ID) required', 'severity', 'ERROR'),
            OBJECT_CONSTRUCT('rule', 'VALUE requires CURRENCY to be set', 'severity', 'WARNING')
        )
    );
    
    RETURN config;
END;
$$;

-- =============================================================================
-- SAVE CONFIG TO TABLE
-- Persists config for later review and deployment
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_CAPI_DB.PIPELINE.PIPELINE_CONFIGS (
    CONFIG_ID VARCHAR(100) DEFAULT UUID_STRING() PRIMARY KEY,
    CONFIG_NAME VARCHAR(200),
    SOURCE_TABLE VARCHAR(500),
    CONFIG VARIANT,
    STATUS VARCHAR(20) DEFAULT 'PENDING_REVIEW',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    REVIEWED_AT TIMESTAMP_NTZ,
    REVIEWED_BY VARCHAR(100),
    DEPLOYED_AT TIMESTAMP_NTZ,
    NOTES VARCHAR(2000)
);

CREATE OR REPLACE PROCEDURE save_pipeline_config(
    config VARIANT,
    config_name VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    config_id VARCHAR;
    name VARCHAR;
BEGIN
    config_id := UUID_STRING();
    name := COALESCE(:config_name, config:pipeline_settings:pipeline_name::VARCHAR);
    
    INSERT INTO META_CAPI_DB.PIPELINE.PIPELINE_CONFIGS (CONFIG_ID, CONFIG_NAME, SOURCE_TABLE, CONFIG, STATUS)
    VALUES (
        :config_id,
        :name,
        config:source:full_path::VARCHAR,
        :config,
        'PENDING_REVIEW'
    );
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'SAVED',
        'config_id', config_id,
        'config_name', name,
        'next_steps', ARRAY_CONSTRUCT(
            '1. Review config: SELECT CONFIG FROM PIPELINE_CONFIGS WHERE CONFIG_ID = ''' || config_id || '''',
            '2. Edit if needed: CALL update_pipeline_config(''' || config_id || ''', <updated_config>)',
            '3. Approve: CALL approve_pipeline_config(''' || config_id || ''')',
            '4. Deploy: CALL deploy_pipeline_from_config(''' || config_id || ''')'
        )
    );
END;
$$;

-- =============================================================================
-- UPDATE CONFIG
-- Allows editing config before deployment
-- =============================================================================
CREATE OR REPLACE PROCEDURE update_pipeline_config(
    config_id VARCHAR,
    updated_config VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE META_CAPI_DB.PIPELINE.PIPELINE_CONFIGS
    SET CONFIG = :updated_config,
        STATUS = 'PENDING_REVIEW'
    WHERE CONFIG_ID = :config_id;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'UPDATED',
        'config_id', config_id,
        'message', 'Config updated. Review and approve before deploying.'
    );
END;
$$;

-- =============================================================================
-- APPROVE CONFIG
-- Marks config as reviewed and ready for deployment
-- =============================================================================
CREATE OR REPLACE PROCEDURE approve_pipeline_config(
    config_id VARCHAR,
    reviewer_notes VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE META_CAPI_DB.PIPELINE.PIPELINE_CONFIGS
    SET STATUS = 'APPROVED',
        REVIEWED_AT = CURRENT_TIMESTAMP(),
        REVIEWED_BY = CURRENT_USER(),
        NOTES = :reviewer_notes
    WHERE CONFIG_ID = :config_id;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'APPROVED',
        'config_id', config_id,
        'reviewed_by', CURRENT_USER(),
        'message', 'Config approved. Ready for deployment.',
        'deploy_command', 'CALL deploy_pipeline_from_config(''' || config_id || ''')'
    );
END;
$$;

-- =============================================================================
-- DEPLOY FROM CONFIG
-- Creates actual pipeline objects from approved config
-- Builds USER_DATA and CUSTOM_DATA VARIANT columns from column_mappings
-- =============================================================================
CREATE OR REPLACE PROCEDURE deploy_pipeline_from_config(config_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    config VARIANT;
    status VARCHAR;
    pipe_name VARCHAR;
    stream_name VARCHAR;
    task_name VARCHAR;
    view_name VARCHAR;
    source_path VARCHAR;
    user_data_parts VARCHAR DEFAULT '';
    custom_data_parts VARCHAR DEFAULT '';
    event_id_expr VARCHAR;
    event_time_expr VARCHAR;
    event_source_url_expr VARCHAR;
BEGIN
    SELECT c.CONFIG, c.STATUS INTO config, status
    FROM META_CAPI_DB.PIPELINE.PIPELINE_CONFIGS c
    WHERE c.CONFIG_ID = :config_id;
    
    IF (status != 'APPROVED') THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Config must be APPROVED before deployment. Current status: ' || status,
            'action', 'CALL approve_pipeline_config(''' || config_id || ''')'
        );
    END IF;
    
    pipe_name := config:pipeline_settings:pipeline_name::VARCHAR;
    stream_name := pipe_name || '_STREAM';
    task_name := pipe_name || '_TASK';
    view_name := pipe_name || '_VW';
    source_path := config:source:full_path::VARCHAR;
    
    -- Determine EVENT_ID, EVENT_TIME, EVENT_SOURCE_URL from mappings
    event_id_expr := '''evt_'' || UUID_STRING()';
    event_time_expr := 'CURRENT_TIMESTAMP()';
    event_source_url_expr := 'NULL';
    
    -- Route column_mappings into USER_DATA or CUSTOM_DATA based on target_column
    FOR mapping IN (SELECT VALUE FROM TABLE(FLATTEN(input => config:column_mappings)) WHERE VALUE:include::BOOLEAN = TRUE) DO
        LET target VARCHAR := mapping:target_column::VARCHAR;
        LET source VARCHAR := mapping:source_column::VARCHAR;
        LET hashed BOOLEAN := mapping:requires_hashing::BOOLEAN;
        LET col_expr VARCHAR;
        
        IF (target IS NULL) THEN
            CONTINUE;
        END IF;
        
        -- Top-level fields handled separately
        IF (target = 'EVENT_ID') THEN
            event_id_expr := 'COALESCE(' || source || '::VARCHAR, ''evt_'' || UUID_STRING())';
            CONTINUE;
        END IF;
        IF (target = 'EVENT_TIME') THEN
            event_time_expr := source || '::TIMESTAMP_NTZ';
            CONTINUE;
        END IF;
        IF (target = 'EVENT_SOURCE_URL') THEN
            event_source_url_expr := source;
            CONTINUE;
        END IF;
        
        -- Build expression (hashed or raw)
        IF (hashed) THEN
            col_expr := 'ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || source || ')), 256))';
        ELSE
            col_expr := source;
        END IF;
        
        -- Route to USER_DATA or CUSTOM_DATA based on target field
        IF (target IN ('EM', 'PH', 'FN', 'LN', 'GE', 'DB', 'CT', 'ST', 'ZP', 'COUNTRY', 'EXTERNAL_ID', 'CLIENT_IP_ADDRESS', 'CLIENT_USER_AGENT', 'FBC', 'FBP')) THEN
            LET key_name VARCHAR := LOWER(target);
            user_data_parts := user_data_parts || '''' || key_name || ''', ' || col_expr || ', ';
        ELSE
            LET key_name VARCHAR := LOWER(target);
            custom_data_parts := custom_data_parts || '''' || key_name || ''', ' || col_expr || ', ';
        END IF;
    END FOR;
    
    -- Finalize VARIANT expressions
    user_data_parts := RTRIM(user_data_parts, ', ');
    custom_data_parts := RTRIM(custom_data_parts, ', ');
    
    LET user_data_sql VARCHAR := CASE WHEN user_data_parts = '' THEN 'NULL' ELSE 'OBJECT_CONSTRUCT_KEEP_NULL(' || user_data_parts || ')' END;
    LET custom_data_sql VARCHAR := CASE WHEN custom_data_parts = '' THEN 'NULL' ELSE 'OBJECT_CONSTRUCT_KEEP_NULL(' || custom_data_parts || ')' END;
    
    -- Create stream
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM META_CAPI_DB.PIPELINE.' || stream_name || 
        ' ON TABLE ' || source_path || ' APPEND_ONLY = TRUE';
    
    -- Create mapping view with USER_DATA and CUSTOM_DATA VARIANT columns
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE VIEW META_CAPI_DB.PIPELINE.' || view_name || ' AS
    SELECT 
        ' || event_id_expr || ' AS EVENT_ID,
        ''' || config:event_settings:event_type::VARCHAR || ''' AS EVENT_NAME,
        ' || event_time_expr || ' AS EVENT_TIME,
        ''' || config:event_settings:action_source::VARCHAR || ''' AS ACTION_SOURCE,
        ' || event_source_url_expr || ' AS EVENT_SOURCE_URL,
        ' || user_data_sql || ' AS USER_DATA,
        ' || custom_data_sql || ' AS CUSTOM_DATA
    FROM META_CAPI_DB.PIPELINE.' || stream_name;
    
    -- Create task
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE TASK META_CAPI_DB.PIPELINE.' || task_name || '
        WAREHOUSE = COMPUTE_WH
        SCHEDULE = ''60 MINUTE''
        WHEN SYSTEM$STREAM_HAS_DATA(''META_CAPI_DB.PIPELINE.' || stream_name || ''')
    AS
        INSERT INTO META_CAPI_DB.PIPELINE.META_CAPI_EVENTS 
        (EVENT_ID, EVENT_NAME, EVENT_TIME, ACTION_SOURCE, EVENT_SOURCE_URL, USER_DATA, CUSTOM_DATA, STATUS, CREATED_AT)
        SELECT EVENT_ID, EVENT_NAME, EVENT_TIME, ACTION_SOURCE, EVENT_SOURCE_URL, USER_DATA, CUSTOM_DATA, ''PENDING'', CURRENT_TIMESTAMP()
        FROM META_CAPI_DB.PIPELINE.' || view_name;
    
    -- Resume task
    EXECUTE IMMEDIATE 'ALTER TASK META_CAPI_DB.PIPELINE.' || task_name || ' RESUME';
    
    -- Update config status
    UPDATE META_CAPI_DB.PIPELINE.PIPELINE_CONFIGS
    SET STATUS = 'DEPLOYED',
        DEPLOYED_AT = CURRENT_TIMESTAMP()
    WHERE CONFIG_ID = :config_id;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'DEPLOYED',
        'config_id', config_id,
        'pipeline_name', pipe_name,
        'objects_created', OBJECT_CONSTRUCT(
            'stream', stream_name,
            'view', view_name,
            'task', task_name
        ),
        'message', 'Pipeline deployed and active.'
    );
END;
$$;

-- =============================================================================
-- LIST CONFIGS
-- Shows all pipeline configs and their status
-- =============================================================================
CREATE OR REPLACE PROCEDURE list_pipeline_configs()
RETURNS TABLE (
    config_id VARCHAR,
    config_name VARCHAR,
    source_table VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP_NTZ,
    created_by VARCHAR,
    reviewed_by VARCHAR,
    deployed_at TIMESTAMP_NTZ
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        SELECT 
            CONFIG_ID,
            CONFIG_NAME,
            SOURCE_TABLE,
            STATUS,
            CREATED_AT,
            CREATED_BY,
            REVIEWED_BY,
            DEPLOYED_AT
        FROM META_CAPI_DB.PIPELINE.PIPELINE_CONFIGS
        ORDER BY CREATED_AT DESC
    );
    RETURN TABLE(res);
END;
$$;
