USE ROLE sysadmin;

-- Database for lab
CREATE DATABASE IF NOT EXISTS ai_sql_team_db;

-- Schema for lab
CREATE SCHEMA IF NOT EXISTS ai_sql_team_db.se_sample_data;

-- Stage for files to be uploaded
CREATE STAGE IF NOT EXISTS ai_sql_team_db.se_sample_data.equitydocs
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

--create warehouse for lab
CREATE WAREHOUSE IF NOT EXISTS ai_sql_team_wh
  WAREHOUSE_TYPE = 'STANDARD'
  WAREHOUSE_SIZE = 'XSMALL'
  SCALING_POLICY = 'STANDARD'
  AUTO_RESUME = TRUE
  AUTO_SUSPEND = 300
  ENABLE_QUERY_ACCELERATION = FALSE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  COMMENT = 'Lab warehouse for AI SQL'
