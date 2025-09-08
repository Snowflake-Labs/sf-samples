-- -----------------------------------------------------------------------
-- SET session variables for lab resources
-- -----------------------------------------------------------------------
SET lab_db = 'ai_sql_team_db';
SET lab_schema = 'ai_sql_team_db.se_sample_data';
SET lab_stage = 'equitydocs';
SET lab_warehouse = 'ai_sql_team_wh';
SET lab_role = 'ai_sql_team_role';
SET lab_user = 'your_user_name';  -- replace with your user name

-- use sysadmin role to create lab resources
USE ROLE sysadmin;

-- CREATE session namespace (database.schema)
CREATE DATABASE IF NOT EXISTS IDENTIFIER($lab_db);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($lab_schema);

-- SET session namespace (database.schema)
USE DATABASE IDENTIFIER($lab_db);
USE SCHEMA IDENTIFIER($lab_schema);

-- Stage for files to be uploaded
DROP STAGE IDENTIFIER($lab_stage);

CREATE STAGE IF NOT EXISTS IDENTIFIER($lab_stage)
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

--create warehouse for lab
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($lab_warehouse)
  WAREHOUSE_TYPE = 'STANDARD'
  WAREHOUSE_SIZE = 'XSMALL'
  SCALING_POLICY = 'STANDARD'
  AUTO_RESUME = TRUE
  AUTO_SUSPEND = 300
  ENABLE_QUERY_ACCELERATION = FALSE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  COMMENT = 'Lab warehouse for AI SQL';

USE ROLE USERADMIN;

-- create role for lab
CREATE ROLE IF NOT EXISTS IDENTIFIER($lab_role);

-- Grant permissions to lab role.
GRANT USAGE ON DATABASE IDENTIFIER($lab_db) TO ROLE IDENTIFIER($lab_role);
GRANT USAGE ON ALL SCHEMAS IN DATABASE IDENTIFIER($lab_db) TO ROLE IDENTIFIER($lab_role);
GRANT USAGE ON WAREHOUSE IDENTIFIER($lab_warehouse) TO ROLE IDENTIFIER($lab_role);
GRANT USAGE ON ALL STAGES IN SCHEMA IDENTIFIER($lab_schema) TO ROLE IDENTIFIER($lab_role);
GRANT USAGE, READ ON ALL STAGES IN SCHEMA IDENTIFIER($lab_schema) TO ROLE IDENTIFIER($lab_role);
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE IDENTIFIER($lab_db) TO ROLE IDENTIFIER($lab_role);
GRANT CREATE NOTEBOOK, CREATE TABLE ON SCHEMA IDENTIFIER($lab_schema) TO ROLE IDENTIFIER($lab_role);
GRANT CREATE VIEW ON SCHEMA IDENTIFIER($lab_schema) TO ROLE IDENTIFIER($lab_role);
GRANT SELECT ON FUTURE VIEWS IN SCHEMA IDENTIFIER($lab_schema) TO ROLE IDENTIFIER($lab_role);


-- -----------------------------------------------------------------------
-- ROLE to ROLE Grants
-- -----------------------------------------------------------------------
GRANT ROLE IDENTIFIER($lab_role) TO ROLE accountadmin;

-- -----------------------------------------------------------------------
-- ROLE to USER Grants
-- -----------------------------------------------------------------------
GRANT ROLE IDENTIFIER($lab_role) TO USER IDENTIFIER($lab_user);

-- -----------------------------------------------------------------------
-- Upload the Equity research PDFs to the Snowflake Stage
-- -----------------------------------------------------------------------
-- It's easiest to do this in the Snowflake UI, if you want to do it from the command line,
-- you can use the following command (make sure to replace the path and connection name):
snow stage copy "/path/to/the/repo/sf-samples/samples/aisql/financial_service_equity_research/data/*.pdf" @ai_sql_team_db.se_sample_data.equitydocs --connection your_connection_name
