USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS GIT_DB;
USE SCHEMA GIT_DB.PUBLIC;

CREATE WAREHOUSE IF NOT EXISTS NOTEBOOK_WH WAREHOUSE_SIZE=XSMALL;

-- use your GitHub PAT as password
CREATE OR REPLACE SECRET GITHUB_SECRET
  TYPE = PASSWORD
  USERNAME = 'XXXXXXXXXX.XXXXXXXXXX@snowflake.com'
  PASSWORD = 'XXXXXXXXXXXXXXXXXXXX';

CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION
    API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs/')
    API_PROVIDER = git_https_api
    ALLOWED_AUTHENTICATION_SECRETS = all
    ENABLED = TRUE
    ;

CREATE OR REPLACE GIT REPOSITORY SF_SAMPLES
  ORIGIN = 'https://github.com/Snowflake-Labs/sf-samples.git'
  API_INTEGRATION = github_itegration
  GIT_CREDENTIALS = github_secret;

-- set up Tasty Bytes data if it's not already in your account
EXECUTE IMMEDIATE FROM @SF_SAMPLES/branches/main/samples/tasty_bytes/FY25_Zero_To_Snowflake/tb_introduction.sql;
USE ROLE ACCOUNTADMIN;

-- create notebook schema and create notebook from stage
CREATE SCHEMA IF NOT EXISTS TB_101.CORTEX_ANALYST_DEMO;
GRANT CREATE NOTEBOOK, CREATE STREAMLIT ON SCHEMA TB_101.CORTEX_ANALYST_DEMO TO ROLE ACCOUNTADMIN;

-- create schema and stage for our semantic model file
create schema if not exists TB_101.SEMANTIC_MODELS ;
grant usage on schema TB_101.SEMANTIC_MODELS to role ACCOUNTADMIN;
create stage if not exists TB_101.SEMANTIC_MODELS.SEMANTIC_MODELS DIRECTORY = (ENABLE = TRUE);
grant read on stage TB_101.SEMANTIC_MODELS.SEMANTIC_MODELS to role ACCOUNTADMIN;

-- copy semantic model from Git stage or upload your own
COPY FILES
  INTO @TB_101.SEMANTIC_MODELS.SEMANTIC_MODELS
  FROM '@GIT_DB.PUBLIC.SF_SAMPLES/branches/main/samples/cortex-analyst/Cortex Analyst Tasty Bytes/'
  FILES = ('sales_detail_semantic_model.yaml');

--create notebook and streamlit app
CREATE OR REPLACE NOTEBOOK TB_101.CORTEX_ANALYST_DEMO."Tasty Bytes Analyst Demo"
FROM '@GIT_DB.PUBLIC.SF_SAMPLES/branches/main/samples/cortex-analyst/Cortex Analyst Tasty Bytes/Cortex Analyst Notebook/'
MAIN_FILE = 'notebook_app.ipynb'
QUERY_WAREHOUSE = NOTEBOOK_WH;

CREATE OR REPLACE STREAMLIT TB_101.CORTEX_ANALYST_DEMO."Tasty Bytes Cortex Analyst"
ROOT_LOCATION =  '@GIT_DB.PUBLIC.SF_SAMPLES/branches/main/samples/cortex-analyst/Cortex Analyst Tasty Bytes/Cortex Analyst Streamlit'
MAIN_FILE = '/streamlit_in_snowflake_app.py'
QUERY_WAREHOUSE = NOTEBOOK_WH;