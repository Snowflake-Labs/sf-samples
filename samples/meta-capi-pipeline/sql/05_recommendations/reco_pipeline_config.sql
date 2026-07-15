-- =============================================================================
-- RECOMMENDATION PIPELINE CONFIGS
-- Separate from PIPELINE_CONFIGS - never edits existing pipelines
-- Human-in-the-loop workflow for deploying recommendation-based pipelines
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- TABLE: Dedicated config table for recommendation-based pipelines
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_RECO_CONFIGS (
    CONFIG_ID VARCHAR(100) DEFAULT UUID_STRING() PRIMARY KEY,
    CONFIG_NAME VARCHAR(200),
    
    RECOMMENDATION_ID VARCHAR(100),
    USE_CASE_REF VARCHAR(100),
    USE_CASE_NAME VARCHAR(200),
    
    SOURCE_DATABASE VARCHAR(100),
    SOURCE_SCHEMA VARCHAR(100),
    SOURCE_TABLE VARCHAR(100),
    
    CONFIG VARIANT,
    
    STATUS VARCHAR(20) DEFAULT 'PENDING_REVIEW',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    REVIEWED_AT TIMESTAMP_NTZ,
    REVIEWED_BY VARCHAR(100),
    REVIEW_NOTES VARCHAR(2000),
    APPROVED_AT TIMESTAMP_NTZ,
    APPROVED_BY VARCHAR(100),
    APPROVAL_NOTES VARCHAR(2000),
    DEPLOYED_AT TIMESTAMP_NTZ,
    DEPLOYED_BY VARCHAR(100),
    
    DEPLOYED_OBJECTS VARIANT
);

-- =============================================================================
-- PROCEDURE: Generate config for review (creates NO objects)
-- =============================================================================
CREATE OR REPLACE PROCEDURE generate_reco_config(
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
    config VARIANT;
    meta_req VARIANT;
    reco_id VARCHAR;
    reco_name VARCHAR;
BEGIN
    SELECT 
        RECOMMENDATION_ID,
        USE_CASE_NAME,
        OBJECT_CONSTRUCT(
            'use_case_name', USE_CASE_NAME,
            'use_case_ref', USE_CASE_REF,
            'description', DESCRIPTION,
            'event_type', PRIMARY_EVENT_TYPE,
            'required_parameters', REQUIRED_PARAMETERS,
            'recommended_events', RECOMMENDED_EVENTS
        )
    INTO reco_id, reco_name, meta_req
    FROM V_PENDING_RECOMMENDATIONS
    WHERE USE_CASE_REF = :use_case_ref
    LIMIT 1;
    
    IF (reco_id IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'No pending recommendation found for use_case_ref: ' || use_case_ref,
            'action', 'Run CALL get_use_case_recommendations() first'
        );
    END IF;
    
    SELECT OBJECT_CONSTRUCT(
        '_metadata', OBJECT_CONSTRUCT(
            'generated_at', CURRENT_TIMESTAMP(),
            'generated_by', CURRENT_USER(),
            'status', 'PENDING_REVIEW',
            'recommendation_id', :reco_id,
            'use_case_ref', :use_case_ref,
            'instructions', 'Review column mappings, verify PII fields, then call save_reco_config() to persist'
        ),
        'meta_requirements', :meta_req,
        'source', OBJECT_CONSTRUCT(
            'database', :source_database,
            'schema', :source_schema,
            'table', :source_table,
            'full_path', :source_database || '.' || :source_schema || '.' || :source_table
        ),
        'target', OBJECT_CONSTRUCT(
            'database', 'META_CAPI_DB',
            'schema', 'PIPELINE',
            'table', 'META_CAPI_EVENTS'
        ),
        'event_settings', OBJECT_CONSTRUCT(
            'event_type', meta_req:event_type::VARCHAR,
            'action_source', 'website'
        ),
        'column_mappings', (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'source_column', COLUMN_NAME,
                'source_type', DATA_TYPE,
                'target_field', 
                CASE 
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%mail%', 'em') THEN 'EM'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%phone%', '%mobile%', 'ph') THEN 'PH'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%first%name%', 'fname', 'fn') THEN 'FN'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%last%name%', 'lname', 'ln') THEN 'LN'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%value%', '%amount%', '%total%', '%revenue%') THEN 'VALUE'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%currency%') THEN 'CURRENCY'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%time%', '%date%', '%created%', '%timestamp%') THEN 'EVENT_TIME'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%order%id%', '%transaction%id%') THEN 'ORDER_ID'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%user%id%', '%customer%id%') THEN 'EXTERNAL_ID'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%ip%') THEN 'CLIENT_IP_ADDRESS'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%agent%', '%browser%') THEN 'CLIENT_USER_AGENT'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%ltv%', '%lifetime%', '%predicted%') THEN 'PREDICTED_LTV'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%city%') THEN 'CT'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%state%', '%province%') THEN 'ST'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%zip%', '%postal%') THEN 'ZP'
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%country%') THEN 'COUNTRY'
                    ELSE NULL
                END,
                'requires_hashing',
                CASE 
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%first%', '%last%', '%city%', '%state%', '%zip%', '%country%') THEN TRUE
                    ELSE FALSE
                END,
                'include',
                CASE 
                    WHEN LOWER(COLUMN_NAME) LIKE ANY ('%email%', '%phone%', '%first%', '%last%', '%value%', '%amount%', '%total%', '%time%', '%date%', '%order%', '%user%id%', '%customer%', '%ltv%', '%predicted%') THEN TRUE
                    ELSE FALSE
                END,
                'notes', ''
            ))
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = :source_database
            AND TABLE_SCHEMA = :source_schema
            AND TABLE_NAME = :source_table
        ),
        'pipeline_settings', OBJECT_CONSTRUCT(
            'pipeline_name', 'RECO_' || UPPER(REPLACE(:use_case_ref, ' ', '_')),
            'schedule', 'ON_NEW_DATA',
            'batch_size', 10000,
            'enabled', FALSE
        ),
        'review_checklist', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('check', 'Verify all Meta required parameters are mapped', 'completed', FALSE),
            OBJECT_CONSTRUCT('check', 'Confirm PII columns are set for SHA256 hashing', 'completed', FALSE),
            OBJECT_CONSTRUCT('check', 'Check event_type matches Meta recommendation', 'completed', FALSE),
            OBJECT_CONSTRUCT('check', 'Verify action_source is correct (website/app/etc)', 'completed', FALSE)
        )
    ) INTO config;
    
    RETURN config;
END;
$$;

-- =============================================================================
-- PROCEDURE: Save config for team review
-- =============================================================================
CREATE OR REPLACE PROCEDURE save_reco_config(
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
    
    INSERT INTO META_RECO_CONFIGS (
        CONFIG_ID,
        CONFIG_NAME,
        RECOMMENDATION_ID,
        USE_CASE_REF,
        USE_CASE_NAME,
        SOURCE_DATABASE,
        SOURCE_SCHEMA,
        SOURCE_TABLE,
        CONFIG,
        STATUS,
        CREATED_BY
    )
    VALUES (
        :config_id,
        :name,
        config:_metadata:recommendation_id::VARCHAR,
        config:_metadata:use_case_ref::VARCHAR,
        config:meta_requirements:use_case_name::VARCHAR,
        config:source:database::VARCHAR,
        config:source:schema::VARCHAR,
        config:source:table::VARCHAR,
        :config,
        'PENDING_REVIEW',
        CURRENT_USER()
    );
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'SAVED',
        'config_id', config_id,
        'config_name', name,
        'use_case', config:meta_requirements:use_case_name::VARCHAR,
        'next_steps', ARRAY_CONSTRUCT(
            '1. Review: SELECT CONFIG FROM META_RECO_CONFIGS WHERE CONFIG_ID = ''' || config_id || '''',
            '2. Edit if needed: CALL update_reco_config(''' || config_id || ''', <updated_config>)',
            '3. Approve: CALL approve_reco_config(''' || config_id || ''', ''<approval_notes>'')',
            '4. Deploy: CALL deploy_reco_config(''' || config_id || ''')'
        )
    );
END;
$$;

-- =============================================================================
-- PROCEDURE: Update config (only if PENDING_REVIEW)
-- =============================================================================
CREATE OR REPLACE PROCEDURE update_reco_config(
    config_id VARCHAR,
    updated_config VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    current_status VARCHAR;
BEGIN
    SELECT STATUS INTO current_status
    FROM META_RECO_CONFIGS
    WHERE CONFIG_ID = :config_id;
    
    IF (current_status IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Config not found: ' || config_id
        );
    END IF;
    
    IF (current_status != 'PENDING_REVIEW') THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Config can only be updated when status is PENDING_REVIEW. Current status: ' || current_status
        );
    END IF;
    
    UPDATE META_RECO_CONFIGS
    SET CONFIG = :updated_config,
        REVIEWED_AT = CURRENT_TIMESTAMP(),
        REVIEWED_BY = CURRENT_USER()
    WHERE CONFIG_ID = :config_id;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'UPDATED',
        'config_id', config_id,
        'reviewed_by', CURRENT_USER(),
        'message', 'Config updated. Ready for approval.'
    );
END;
$$;

-- =============================================================================
-- PROCEDURE: Human approval step (REQUIRED before deployment)
-- =============================================================================
CREATE OR REPLACE PROCEDURE approve_reco_config(
    config_id VARCHAR,
    approval_notes VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    current_status VARCHAR;
    config_name VARCHAR;
BEGIN
    SELECT STATUS, CONFIG_NAME INTO current_status, config_name
    FROM META_RECO_CONFIGS
    WHERE CONFIG_ID = :config_id;
    
    IF (current_status IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Config not found: ' || config_id
        );
    END IF;
    
    IF (current_status NOT IN ('PENDING_REVIEW')) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Config can only be approved when status is PENDING_REVIEW. Current status: ' || current_status
        );
    END IF;
    
    UPDATE META_RECO_CONFIGS
    SET STATUS = 'APPROVED',
        APPROVED_AT = CURRENT_TIMESTAMP(),
        APPROVED_BY = CURRENT_USER(),
        APPROVAL_NOTES = :approval_notes
    WHERE CONFIG_ID = :config_id;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'APPROVED',
        'config_id', config_id,
        'config_name', config_name,
        'approved_by', CURRENT_USER(),
        'approved_at', CURRENT_TIMESTAMP(),
        'message', 'Config approved and ready for deployment.',
        'next_step', 'CALL deploy_reco_config(''' || config_id || ''')'
    );
END;
$$;

-- =============================================================================
-- PROCEDURE: Deploy ONLY if APPROVED (creates objects with RECO_ prefix)
-- =============================================================================
CREATE OR REPLACE PROCEDURE deploy_reco_config(config_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    config VARIANT;
    current_status VARCHAR;
    use_case_ref VARCHAR;
    pipe_name VARCHAR;
    stream_name VARCHAR;
    task_name VARCHAR;
    view_name VARCHAR;
    source_path VARCHAR;
BEGIN
    SELECT c.CONFIG, c.STATUS, c.USE_CASE_REF 
    INTO config, current_status, use_case_ref
    FROM META_RECO_CONFIGS c
    WHERE c.CONFIG_ID = :config_id;
    
    IF (config IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Config not found: ' || config_id
        );
    END IF;
    
    IF (current_status != 'APPROVED') THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Config must be APPROVED before deployment. Current status: ' || current_status,
            'action', 'CALL approve_reco_config(''' || config_id || ''') first'
        );
    END IF;
    
    pipe_name := config:pipeline_settings:pipeline_name::VARCHAR;
    stream_name := pipe_name || '_STREAM';
    task_name := pipe_name || '_TASK';
    view_name := pipe_name || '_VIEW';
    source_path := config:source:full_path::VARCHAR;
    
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM META_CAPI_DB.PIPELINE.' || stream_name || 
        ' ON TABLE ' || source_path || ' APPEND_ONLY = TRUE';
    
    -- Build USER_DATA and CUSTOM_DATA from column_mappings
    LET user_data_parts VARCHAR := '';
    LET custom_data_parts VARCHAR := '';
    LET event_id_expr VARCHAR := '''evt_'' || UUID_STRING()';
    LET event_time_expr VARCHAR := 'CURRENT_TIMESTAMP()';
    LET event_source_url_expr VARCHAR := 'NULL';
    
    FOR mapping IN (SELECT VALUE FROM TABLE(FLATTEN(input => config:column_mappings)) WHERE VALUE:include::BOOLEAN = TRUE) DO
        LET target VARCHAR := mapping:target_column::VARCHAR;
        LET source VARCHAR := mapping:source_column::VARCHAR;
        LET hashed BOOLEAN := mapping:requires_hashing::BOOLEAN;
        LET col_expr VARCHAR;
        
        IF (target IS NULL) THEN CONTINUE; END IF;
        IF (target = 'EVENT_ID') THEN event_id_expr := 'COALESCE(' || source || '::VARCHAR, ''evt_'' || UUID_STRING())'; CONTINUE; END IF;
        IF (target = 'EVENT_TIME') THEN event_time_expr := source || '::TIMESTAMP_NTZ'; CONTINUE; END IF;
        IF (target = 'EVENT_SOURCE_URL') THEN event_source_url_expr := source; CONTINUE; END IF;
        
        IF (hashed) THEN col_expr := 'ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || source || ')), 256))';
        ELSE col_expr := source; END IF;
        
        IF (target IN ('EM', 'PH', 'FN', 'LN', 'GE', 'DB', 'CT', 'ST', 'ZP', 'COUNTRY', 'EXTERNAL_ID', 'CLIENT_IP_ADDRESS', 'CLIENT_USER_AGENT', 'FBC', 'FBP')) THEN
            user_data_parts := user_data_parts || '''' || LOWER(target) || ''', ' || col_expr || ', ';
        ELSE
            custom_data_parts := custom_data_parts || '''' || LOWER(target) || ''', ' || col_expr || ', ';
        END IF;
    END FOR;
    
    user_data_parts := RTRIM(user_data_parts, ', ');
    custom_data_parts := RTRIM(custom_data_parts, ', ');
    LET user_data_sql VARCHAR := CASE WHEN user_data_parts = '' THEN 'NULL' ELSE 'OBJECT_CONSTRUCT_KEEP_NULL(' || user_data_parts || ')' END;
    LET custom_data_sql VARCHAR := CASE WHEN custom_data_parts = '' THEN 'NULL' ELSE 'OBJECT_CONSTRUCT_KEEP_NULL(' || custom_data_parts || ')' END;
    
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
    
    UPDATE META_RECO_CONFIGS
    SET STATUS = 'DEPLOYED',
        DEPLOYED_AT = CURRENT_TIMESTAMP(),
        DEPLOYED_BY = CURRENT_USER(),
        DEPLOYED_OBJECTS = OBJECT_CONSTRUCT(
            'stream', stream_name,
            'view', view_name,
            'task', task_name
        )
    WHERE CONFIG_ID = :config_id;
    
    CALL mark_recommendation_implemented(:use_case_ref);
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'DEPLOYED',
        'config_id', config_id,
        'pipeline_name', pipe_name,
        'deployed_by', CURRENT_USER(),
        'deployed_at', CURRENT_TIMESTAMP(),
        'objects_created', OBJECT_CONSTRUCT(
            'stream', 'META_CAPI_DB.PIPELINE.' || stream_name,
            'view', 'META_CAPI_DB.PIPELINE.' || view_name,
            'task', 'META_CAPI_DB.PIPELINE.' || task_name
        ),
        'recommendation_status', 'IMPLEMENTED',
        'next_step', 'Run ALTER TASK META_CAPI_DB.PIPELINE.' || task_name || ' RESUME to activate'
    );
END;
$$;

-- =============================================================================
-- PROCEDURE: List all recommendation configs
-- =============================================================================
CREATE OR REPLACE PROCEDURE list_reco_configs()
RETURNS TABLE (
    config_id VARCHAR,
    config_name VARCHAR,
    use_case_name VARCHAR,
    source_table VARCHAR,
    status VARCHAR,
    created_by VARCHAR,
    created_at TIMESTAMP_NTZ,
    approved_by VARCHAR,
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
            USE_CASE_NAME,
            SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE AS SOURCE_TABLE,
            STATUS,
            CREATED_BY,
            CREATED_AT,
            APPROVED_BY,
            DEPLOYED_AT
        FROM META_RECO_CONFIGS
        ORDER BY CREATED_AT DESC
    );
    RETURN TABLE(res);
END;
$$;

-- =============================================================================
-- VIEW: Summary of recommendation configs
-- =============================================================================
CREATE OR REPLACE VIEW V_RECO_CONFIG_STATUS AS
SELECT 
    CONFIG_NAME,
    USE_CASE_NAME,
    SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE AS SOURCE_TABLE,
    STATUS,
    CREATED_BY,
    CREATED_AT,
    APPROVED_BY,
    APPROVED_AT,
    DEPLOYED_BY,
    DEPLOYED_AT
FROM META_RECO_CONFIGS
ORDER BY 
    CASE STATUS 
        WHEN 'PENDING_REVIEW' THEN 1 
        WHEN 'APPROVED' THEN 2 
        WHEN 'DEPLOYED' THEN 3 
    END,
    CREATED_AT DESC;
