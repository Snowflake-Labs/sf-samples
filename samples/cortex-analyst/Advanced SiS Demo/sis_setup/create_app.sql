-- Note: soon it will be replaced with a new syntax
CREATE OR REPLACE STREAMLIT CORTEX_ANALYST_DEMO.CA_SIS_DEMO_APP_SCHEMA.cortex_analyst_demo_app
ROOT = '@cortex_analyst_demo.ca_sis_demo_app_schema.app_code'
MAIN_FILE = 'Talk_to_your_data.py'
QUERY_WAREHOUSE = <warehouse>
TITLE = 'Cortex Analyst - extended demo';