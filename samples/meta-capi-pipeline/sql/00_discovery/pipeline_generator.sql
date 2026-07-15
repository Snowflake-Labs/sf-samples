-- =============================================================================
-- META CAPI PIPELINE GENERATOR
-- Creates automated pipelines from source tables to META_CAPI_EVENTS
-- Uses VARIANT columns (USER_DATA, CUSTOM_DATA) for flexible field support
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- GENERATE LOADING PIPELINE
-- Creates a stream + mapping view + task for continuous data loading
--
-- column_mapping keys:
--   Standard: event_id, event_time, event_source_url
--   user_data: email, phone, first_name, last_name, gender, date_of_birth,
--              city, state, zip, country, external_id, ip_address, user_agent,
--              fbc, fbp, + any custom user_data keys
--   custom_data: value, currency, content_ids, content_type, contents,
--                num_items, search_string, order_id, predicted_ltv, + any custom keys
-- =============================================================================
CREATE OR REPLACE PROCEDURE generate_loading_pipeline(
    source_database VARCHAR,
    source_schema VARCHAR,
    source_table VARCHAR,
    column_mapping VARIANT,
    event_type VARCHAR DEFAULT 'Purchase',
    action_source VARCHAR DEFAULT 'website',
    pipeline_name VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    pipe_name VARCHAR;
    stream_name VARCHAR;
    task_name VARCHAR;
    view_name VARCHAR;
    user_data_sql VARCHAR;
    custom_data_sql VARCHAR;
BEGIN
    pipe_name := COALESCE(:pipeline_name, REPLACE(:source_table, ' ', '_') || '_PIPELINE');
    stream_name := pipe_name || '_STREAM';
    task_name := pipe_name || '_TASK';
    view_name := pipe_name || '_VW';
    
    -- Build USER_DATA OBJECT_CONSTRUCT from column_mapping
    user_data_sql := 'OBJECT_CONSTRUCT_KEEP_NULL(';
    
    -- PII fields (hashed)
    IF (column_mapping:email IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''em'', ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || column_mapping:email::VARCHAR || ')), 256)), ';
    END IF;
    IF (column_mapping:phone IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''ph'', ARRAY_CONSTRUCT(SHA2(REGEXP_REPLACE(' || column_mapping:phone::VARCHAR || ', ''[^0-9]'', ''''), 256)), ';
    END IF;
    IF (column_mapping:first_name IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''fn'', ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || column_mapping:first_name::VARCHAR || ')), 256)), ';
    END IF;
    IF (column_mapping:last_name IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''ln'', ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || column_mapping:last_name::VARCHAR || ')), 256)), ';
    END IF;
    IF (column_mapping:gender IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''ge'', ARRAY_CONSTRUCT(SHA2(LOWER(' || column_mapping:gender::VARCHAR || '), 256)), ';
    END IF;
    IF (column_mapping:date_of_birth IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''db'', ARRAY_CONSTRUCT(SHA2(' || column_mapping:date_of_birth::VARCHAR || '::VARCHAR, 256)), ';
    END IF;
    IF (column_mapping:city IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''ct'', ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || column_mapping:city::VARCHAR || ')), 256)), ';
    END IF;
    IF (column_mapping:state IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''st'', ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || column_mapping:state::VARCHAR || ')), 256)), ';
    END IF;
    IF (column_mapping:zip IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''zp'', ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || column_mapping:zip::VARCHAR || ')), 256)), ';
    END IF;
    IF (column_mapping:country IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''country'', ARRAY_CONSTRUCT(SHA2(LOWER(TRIM(' || column_mapping:country::VARCHAR || ')), 256)), ';
    END IF;
    IF (column_mapping:external_id IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''external_id'', ARRAY_CONSTRUCT(' || column_mapping:external_id::VARCHAR || '::VARCHAR), ';
    END IF;
    
    -- Non-PII fields (not hashed)
    IF (column_mapping:ip_address IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''client_ip_address'', ' || column_mapping:ip_address::VARCHAR || ', ';
    END IF;
    IF (column_mapping:user_agent IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''client_user_agent'', ' || column_mapping:user_agent::VARCHAR || ', ';
    END IF;
    IF (column_mapping:fbc IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''fbc'', ' || column_mapping:fbc::VARCHAR || ', ';
    END IF;
    IF (column_mapping:fbp IS NOT NULL) THEN
        user_data_sql := user_data_sql || '''fbp'', ' || column_mapping:fbp::VARCHAR || ', ';
    END IF;
    
    -- Any additional user_data fields from custom_user_data mapping
    IF (column_mapping:custom_user_data IS NOT NULL) THEN
        FOR rec IN (SELECT key, value FROM TABLE(FLATTEN(input => :column_mapping:custom_user_data))) DO
            user_data_sql := user_data_sql || '''' || rec.key || ''', ' || rec.value::VARCHAR || ', ';
        END FOR;
    END IF;
    
    -- Remove trailing comma and close
    user_data_sql := RTRIM(user_data_sql, ', ') || ')';
    IF (user_data_sql = 'OBJECT_CONSTRUCT_KEEP_NULL()') THEN
        user_data_sql := 'NULL';
    END IF;
    
    -- Build CUSTOM_DATA OBJECT_CONSTRUCT from column_mapping
    custom_data_sql := 'OBJECT_CONSTRUCT_KEEP_NULL(';
    
    IF (column_mapping:value IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''value'', ' || column_mapping:value::VARCHAR || '::FLOAT, ';
    END IF;
    IF (column_mapping:currency IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''currency'', ' || column_mapping:currency::VARCHAR || ', ';
    ELSIF (column_mapping:value IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''currency'', ''USD'', ';
    END IF;
    IF (column_mapping:content_ids IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''content_ids'', ARRAY_CONSTRUCT(' || column_mapping:content_ids::VARCHAR || '::VARCHAR), ';
    END IF;
    IF (column_mapping:content_type IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''content_type'', ' || column_mapping:content_type::VARCHAR || ', ';
    END IF;
    IF (column_mapping:contents IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''contents'', ' || column_mapping:contents::VARCHAR || ', ';
    END IF;
    IF (column_mapping:num_items IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''num_items'', ' || column_mapping:num_items::VARCHAR || ', ';
    END IF;
    IF (column_mapping:search_string IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''search_string'', ' || column_mapping:search_string::VARCHAR || ', ';
    END IF;
    IF (column_mapping:order_id IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''order_id'', ' || column_mapping:order_id::VARCHAR || '::VARCHAR, ';
    END IF;
    IF (column_mapping:predicted_ltv IS NOT NULL) THEN
        custom_data_sql := custom_data_sql || '''predicted_ltv'', ' || column_mapping:predicted_ltv::VARCHAR || '::FLOAT, ';
    END IF;
    
    -- Any additional custom_data fields from custom_fields mapping
    IF (column_mapping:custom_fields IS NOT NULL) THEN
        FOR rec IN (SELECT key, value FROM TABLE(FLATTEN(input => :column_mapping:custom_fields))) DO
            custom_data_sql := custom_data_sql || '''' || rec.key || ''', ' || rec.value::VARCHAR || ', ';
        END FOR;
    END IF;
    
    -- Remove trailing comma and close
    custom_data_sql := RTRIM(custom_data_sql, ', ') || ')';
    IF (custom_data_sql = 'OBJECT_CONSTRUCT_KEEP_NULL()') THEN
        custom_data_sql := 'NULL';
    END IF;
    
    -- Create stream on source table
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM META_CAPI_DB.PIPELINE.' || stream_name || 
        ' ON TABLE ' || source_database || '.' || source_schema || '.' || source_table ||
        ' APPEND_ONLY = TRUE';
    
    -- Create mapping view
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE VIEW META_CAPI_DB.PIPELINE.' || view_name || ' AS
    SELECT 
        ' || CASE WHEN column_mapping:event_id IS NOT NULL 
            THEN 'COALESCE(' || column_mapping:event_id::VARCHAR || '::VARCHAR, ''evt_'' || UUID_STRING())'
            ELSE '''evt_'' || UUID_STRING()' END || ' AS EVENT_ID,
        ''' || event_type || ''' AS EVENT_NAME,
        ' || COALESCE(column_mapping:event_time::VARCHAR, 'CURRENT_TIMESTAMP()') || '::TIMESTAMP_NTZ AS EVENT_TIME,
        ''' || action_source || ''' AS ACTION_SOURCE,
        ' || COALESCE(column_mapping:event_source_url::VARCHAR, 'NULL') || ' AS EVENT_SOURCE_URL,
        ' || user_data_sql || ' AS USER_DATA,
        ' || custom_data_sql || ' AS CUSTOM_DATA
    FROM META_CAPI_DB.PIPELINE.' || stream_name;
    
    -- Create task to load from view into META_CAPI_EVENTS
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
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'pipeline_name', pipe_name,
        'objects_created', OBJECT_CONSTRUCT(
            'stream', stream_name,
            'view', view_name,
            'task', task_name
        ),
        'source', CONCAT(source_database, '.', source_schema, '.', source_table),
        'message', 'Pipeline created and active. New rows in source table will automatically flow to META_CAPI_EVENTS.'
    );
END;
$$;

-- =============================================================================
-- LIST ACTIVE PIPELINES
-- Shows all discovery pipelines and their status
-- =============================================================================
CREATE OR REPLACE PROCEDURE list_capi_pipelines()
RETURNS TABLE (
    pipeline_name VARCHAR,
    stream_name VARCHAR,
    task_name VARCHAR,
    task_state VARCHAR,
    last_run TIMESTAMP_NTZ,
    rows_loaded INT
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        SELECT 
            REPLACE(t.NAME, '_TASK', '') AS pipeline_name,
            REPLACE(t.NAME, '_TASK', '_STREAM') AS stream_name,
            t.NAME AS task_name,
            t.STATE AS task_state,
            t.LAST_COMMITTED_ON AS last_run,
            0 AS rows_loaded
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 100
        )) t
        WHERE t.DATABASE_NAME = 'META_CAPI_DB'
        AND t.SCHEMA_NAME = 'PIPELINE'
        AND t.NAME LIKE '%_PIPELINE_TASK'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY t.NAME ORDER BY t.SCHEDULED_TIME DESC) = 1
    );
    RETURN TABLE(res);
END;
$$;

-- =============================================================================
-- DROP PIPELINE
-- Removes all objects associated with a pipeline
-- =============================================================================
CREATE OR REPLACE PROCEDURE drop_capi_pipeline(pipeline_name VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    EXECUTE IMMEDIATE 'ALTER TASK META_CAPI_DB.PIPELINE.' || pipeline_name || '_TASK SUSPEND';
    EXECUTE IMMEDIATE 'DROP TASK IF EXISTS META_CAPI_DB.PIPELINE.' || pipeline_name || '_TASK';
    EXECUTE IMMEDIATE 'DROP VIEW IF EXISTS META_CAPI_DB.PIPELINE.' || pipeline_name || '_VW';
    EXECUTE IMMEDIATE 'DROP STREAM IF EXISTS META_CAPI_DB.PIPELINE.' || pipeline_name || '_STREAM';
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'message', 'Pipeline ' || pipeline_name || ' and all associated objects dropped.'
    );
END;
$$;
