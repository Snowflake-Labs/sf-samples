-- =============================================================================
-- META USE CASE RECOMMENDATIONS - API Integration
-- Fetches recommendations from Meta's Use Case Recommendation API
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- TABLE: Store recommendations from Meta
-- =============================================================================
CREATE TABLE IF NOT EXISTS META_CAPI_RECOMMENDATIONS (
    RECOMMENDATION_ID VARCHAR(100) DEFAULT UUID_STRING() PRIMARY KEY,
    AD_ACCOUNT_ID VARCHAR(50),
    BUSINESS_ID VARCHAR(50),
    DATA_SOURCE_ID VARCHAR(50),
    USE_CASE_NAME VARCHAR(200),
    USE_CASE_REF VARCHAR(100),
    DESCRIPTION VARCHAR(1000),
    REQUIRED_EVENTS VARIANT,
    RECOMMENDED_EVENTS VARIANT,
    IMPLEMENTATION_STATUS VARCHAR(20) DEFAULT 'NOT_IMPLEMENTED',
    FETCHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IMPLEMENTED_AT TIMESTAMP_NTZ,
    RAW_RESPONSE VARIANT
);

-- =============================================================================
-- PYTHON UDTF: Call Meta Use Case Recommendation API
-- Endpoint: https://api.facebook.com/signals/use_case_recommendation
-- =============================================================================
CREATE OR REPLACE FUNCTION call_meta_recommendation_api(
    ad_account_id VARCHAR,
    business_id VARCHAR,
    data_source_id VARCHAR
)
RETURNS TABLE (
    use_case_name VARCHAR,
    use_case_ref VARCHAR,
    description VARCHAR,
    required_events VARIANT,
    recommended_events VARIANT,
    raw_response VARIANT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'MetaRecommendationHandler'
EXTERNAL_ACCESS_INTEGRATIONS = (meta_capi_integration)
SECRETS = ('access_token' = meta_capi_access_token)
AS $$
import _snowflake
import requests
import json

class MetaRecommendationHandler:
    def process(self, ad_account_id, business_id, data_source_id):
        token = _snowflake.get_generic_secret_string('access_token')
        
        params = {}
        if ad_account_id:
            params['ad_account_id'] = ad_account_id
        if business_id:
            params['business_id'] = business_id
        if data_source_id:
            params['data_source_id'] = data_source_id
        
        if not params:
            yield ('ERROR', None, 'At least one of ad_account_id, business_id, or data_source_id is required', None, None, None)
            return
        
        try:
            response = requests.get(
                'https://api.facebook.com/signals/use_case_recommendation',
                params=params,
                headers={'Authorization': f'Bearer {token}'},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            recommendations = data.get('data', {}).get('recommendations', [])
            
            if not recommendations:
                yield ('NO_RECOMMENDATIONS', None, 'No recommendations returned from Meta API', None, None, data)
                return
            
            for rec in recommendations:
                uc = rec.get('use_case', {})
                yield (
                    uc.get('name'),
                    uc.get('ref'),
                    uc.get('description'),
                    uc.get('requiredEvents'),
                    uc.get('recommendedEvents'),
                    rec
                )
        except requests.exceptions.RequestException as e:
            yield ('ERROR', None, f'API request failed: {str(e)}', None, None, None)
        except json.JSONDecodeError as e:
            yield ('ERROR', None, f'Invalid JSON response: {str(e)}', None, None, None)
$$;

-- =============================================================================
-- PROCEDURE: Fetch and store recommendations from Meta
-- =============================================================================
CREATE OR REPLACE PROCEDURE get_use_case_recommendations(
    ad_account_id VARCHAR DEFAULT NULL,
    business_id VARCHAR DEFAULT NULL,
    data_source_id VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    rec_count INTEGER;
    fetch_time TIMESTAMP_NTZ;
BEGIN
    fetch_time := CURRENT_TIMESTAMP();
    
    INSERT INTO META_CAPI_RECOMMENDATIONS (
        RECOMMENDATION_ID,
        AD_ACCOUNT_ID,
        BUSINESS_ID,
        DATA_SOURCE_ID,
        USE_CASE_NAME,
        USE_CASE_REF,
        DESCRIPTION,
        REQUIRED_EVENTS,
        RECOMMENDED_EVENTS,
        IMPLEMENTATION_STATUS,
        FETCHED_AT,
        RAW_RESPONSE
    )
    SELECT 
        UUID_STRING(),
        :ad_account_id,
        :business_id,
        :data_source_id,
        USE_CASE_NAME,
        USE_CASE_REF,
        DESCRIPTION,
        REQUIRED_EVENTS,
        RECOMMENDED_EVENTS,
        'NOT_IMPLEMENTED',
        :fetch_time,
        RAW_RESPONSE
    FROM TABLE(call_meta_recommendation_api(:ad_account_id, :business_id, :data_source_id))
    WHERE USE_CASE_REF IS NOT NULL;
    
    SELECT COUNT(*) INTO rec_count 
    FROM META_CAPI_RECOMMENDATIONS 
    WHERE FETCHED_AT = :fetch_time;
    
    RETURN OBJECT_CONSTRUCT(
        'status', CASE WHEN rec_count > 0 THEN 'SUCCESS' ELSE 'NO_RECOMMENDATIONS' END,
        'recommendations_fetched', rec_count,
        'ad_account_id', ad_account_id,
        'business_id', business_id,
        'data_source_id', data_source_id,
        'fetched_at', fetch_time,
        'next_step', 'CALL discover_tables_for_recommendations() to find matching tables'
    );
END;
$$;

-- =============================================================================
-- VIEW: Pending recommendations (not yet implemented)
-- =============================================================================
CREATE OR REPLACE VIEW V_PENDING_RECOMMENDATIONS AS
SELECT 
    RECOMMENDATION_ID,
    USE_CASE_NAME,
    USE_CASE_REF,
    DESCRIPTION,
    REQUIRED_EVENTS:events[0]::VARCHAR AS PRIMARY_EVENT_TYPE,
    REQUIRED_EVENTS:parameters AS REQUIRED_PARAMETERS,
    RECOMMENDED_EVENTS,
    FETCHED_AT
FROM META_CAPI_RECOMMENDATIONS 
WHERE IMPLEMENTATION_STATUS = 'NOT_IMPLEMENTED'
AND FETCHED_AT = (
    SELECT MAX(FETCHED_AT) FROM META_CAPI_RECOMMENDATIONS
);

-- =============================================================================
-- VIEW: All recommendations with status
-- =============================================================================
CREATE OR REPLACE VIEW V_RECOMMENDATIONS_STATUS AS
SELECT 
    USE_CASE_NAME,
    USE_CASE_REF,
    DESCRIPTION,
    REQUIRED_EVENTS:events[0]::VARCHAR AS EVENT_TYPE,
    ARRAY_SIZE(REQUIRED_EVENTS:parameters::ARRAY) AS REQUIRED_PARAM_COUNT,
    IMPLEMENTATION_STATUS,
    FETCHED_AT,
    IMPLEMENTED_AT
FROM META_CAPI_RECOMMENDATIONS
ORDER BY FETCHED_AT DESC, USE_CASE_NAME;

-- =============================================================================
-- PROCEDURE: List recommendations with detailed information
-- =============================================================================
CREATE OR REPLACE PROCEDURE list_recommendations()
RETURNS TABLE (
    use_case_name VARCHAR,
    use_case_ref VARCHAR,
    event_type VARCHAR,
    required_parameters VARCHAR,
    description VARCHAR,
    status VARCHAR,
    fetched_at TIMESTAMP_NTZ
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        SELECT 
            USE_CASE_NAME,
            USE_CASE_REF,
            REQUIRED_EVENTS:events[0]::VARCHAR AS EVENT_TYPE,
            (SELECT LISTAGG(p.value:key::VARCHAR, ', ') 
             FROM TABLE(FLATTEN(REQUIRED_EVENTS:parameters)) p) AS REQUIRED_PARAMETERS,
            DESCRIPTION,
            IMPLEMENTATION_STATUS AS STATUS,
            FETCHED_AT
        FROM META_CAPI_RECOMMENDATIONS
        WHERE FETCHED_AT = (SELECT MAX(FETCHED_AT) FROM META_CAPI_RECOMMENDATIONS)
        ORDER BY USE_CASE_NAME
    );
    RETURN TABLE(res);
END;
$$;

-- =============================================================================
-- PROCEDURE: Mark recommendation as implemented
-- Called internally by deploy_reco_config
-- =============================================================================
CREATE OR REPLACE PROCEDURE mark_recommendation_implemented(use_case_ref VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE META_CAPI_RECOMMENDATIONS
    SET IMPLEMENTATION_STATUS = 'IMPLEMENTED',
        IMPLEMENTED_AT = CURRENT_TIMESTAMP()
    WHERE USE_CASE_REF = :use_case_ref
    AND IMPLEMENTATION_STATUS = 'NOT_IMPLEMENTED';
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'UPDATED',
        'use_case_ref', use_case_ref,
        'implemented_at', CURRENT_TIMESTAMP()
    );
END;
$$;
