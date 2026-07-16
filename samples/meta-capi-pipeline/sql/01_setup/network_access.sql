-- =============================================================================
-- META CAPI EXTERNAL NETWORK ACCESS SETUP
-- Configures secure access to graph.facebook.com
-- =============================================================================

USE SCHEMA META_CAPI_DB.PIPELINE;

-- =============================================================================
-- NETWORK RULE - Allow egress to Meta's Graph API and Use Case Recommendation API
-- =============================================================================
CREATE OR REPLACE NETWORK RULE meta_capi_network_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('graph.facebook.com:443', 'api.facebook.com:443');

-- =============================================================================
-- SECRET - Store Meta Access Token securely
-- Replace <ACCESS_TOKEN> with actual token
-- =============================================================================
CREATE OR REPLACE SECRET meta_capi_access_token
    TYPE = GENERIC_STRING
    SECRET_STRING = '<ACCESS_TOKEN>';

-- =============================================================================
-- EXTERNAL ACCESS INTEGRATION - Combine rule and secret
-- =============================================================================
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION meta_capi_integration
    ALLOWED_NETWORK_RULES = (meta_capi_network_rule)
    ALLOWED_AUTHENTICATION_SECRETS = (meta_capi_access_token)
    ENABLED = TRUE;

-- =============================================================================
-- GRANT USAGE - Allow functions to use the integration
-- =============================================================================
GRANT USAGE ON INTEGRATION meta_capi_integration TO ROLE <ROLE_NAME>;

-- =============================================================================
-- STORE PIXEL ID IN CONFIG
-- Replace <PIXEL_ID> with actual pixel ID
-- =============================================================================
MERGE INTO META_CAPI_CONFIG t
USING (SELECT 'PIXEL_ID' AS CONFIG_KEY, '<PIXEL_ID>' AS CONFIG_VALUE) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = s.CONFIG_VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE) VALUES (s.CONFIG_KEY, s.CONFIG_VALUE);
