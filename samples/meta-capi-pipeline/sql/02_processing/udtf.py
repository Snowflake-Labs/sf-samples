"""
Meta CAPI Python UDTF
Sends events to Meta's Conversions API via graph.facebook.com

The UDTF is a pass-through: it receives pre-built event payloads (constructed
by the calling procedure from USER_DATA and CUSTOM_DATA VARIANT columns) and
posts them directly to Meta. It does NOT build or transform the payload.
"""

# =============================================================================
# UDTF DEFINITION (Run this SQL to create the function)
# =============================================================================
UDTF_SQL = """
CREATE OR REPLACE FUNCTION META_CAPI_DB.PIPELINE.send_to_meta_capi(
    pixel_id VARCHAR,
    events ARRAY,
    test_event_code VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    event_id VARCHAR,
    status VARCHAR,
    response VARIANT,
    events_received INT,
    fbtrace_id VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'MetaCAPIHandler'
EXTERNAL_ACCESS_INTEGRATIONS = (meta_capi_integration)
SECRETS = ('access_token' = meta_capi_access_token)
AS $$
import _snowflake
import requests
import json

class MetaCAPIHandler:
    def __init__(self):
        self.access_token = _snowflake.get_generic_secret_string('access_token')
        self.api_version = 'v22.0'
        self.batch_size = 1000
    
    def process(self, pixel_id, events, test_event_code=None):
        if not events:
            return
            
        url = f"https://graph.facebook.com/{self.api_version}/{pixel_id}/events"
        
        for i in range(0, len(events), self.batch_size):
            batch = events[i:i + self.batch_size]
            
            payload = {
                "data": batch,
                "access_token": self.access_token,
                "partner_agent": "snowflake-cortex-code"
            }
            
            if test_event_code:
                payload["test_event_code"] = test_event_code
            
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                
                result = response.json()
                events_received = result.get('events_received', 0)
                fbtrace_id = result.get('fbtrace_id', '')
                
                for event in batch:
                    event_id = event.get('event_id', 'unknown')
                    if response.status_code == 200:
                        yield (event_id, 'SENT', result, events_received, fbtrace_id)
                    else:
                        yield (event_id, 'FAILED', result, 0, fbtrace_id)
                        
            except requests.exceptions.Timeout:
                for event in batch:
                    yield (event.get('event_id', 'unknown'), 'FAILED', 
                           {"error": "Request timeout"}, 0, '')
            except Exception as e:
                for event in batch:
                    yield (event.get('event_id', 'unknown'), 'FAILED', 
                           {"error": str(e)}, 0, '')
$$;
"""
