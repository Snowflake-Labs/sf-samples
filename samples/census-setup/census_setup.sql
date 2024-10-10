-- Create a role for the census user
CREATE ROLE CENSUS_ROLE;

-- Ensure the sysadmin role inherits any privileges the census role is granted. Note that this does not grant sysadmin privileges to the census role
GRANT ROLE CENSUS_ROLE TO ROLE SYSADMIN;

-- Create a warehouse for the census role, optimizing for cost over performance
CREATE WAREHOUSE CENSUS_WAREHOUSE WITH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = FALSE;
GRANT USAGE ON WAREHOUSE CENSUS_WAREHOUSE TO ROLE CENSUS_ROLE;
GRANT OPERATE ON WAREHOUSE CENSUS_WAREHOUSE TO ROLE CENSUS_ROLE;
GRANT MONITOR ON WAREHOUSE CENSUS_WAREHOUSE TO ROLE CENSUS_ROLE;

-- Create the census user
-- Do not set DEFAULT_WORKSPACE, this will impact which tables are visible to Census
CREATE USER CENSUS WITH DEFAULT_ROLE = CENSUS_ROLE DEFAULT_WAREHOUSE = CENSUS_WAREHOUSE PASSWORD = '<strong, unique password>';
GRANT ROLE CENSUS_ROLE TO USER CENSUS;

-- Let the census user read the data you want to sync
-- This database and schema must have a different name than CENSUS
GRANT USAGE ON DATABASE "<your database>" TO ROLE CENSUS_ROLE;
GRANT USAGE ON SCHEMA "<your database>"."<your schema>" TO ROLE CENSUS_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA "<your database>"."<your schema>" TO ROLE CENSUS_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA "<your database>"."<your schema>" TO ROLE CENSUS_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA "<your database>"."<your schema>" TO ROLE CENSUS_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA "<your database>"."<your schema>" TO ROLE CENSUS_ROLE;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA "<your database>"."<your schema>" TO ROLE CENSUS_ROLE;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA "<your database>"."<your schema>" TO ROLE CENSUS_ROLE;

-- Required for Advanced Sync Engine, not required for Basic Sync Engine:
--  Create a private bookkeeping database where Census can store sync state,
--  perform faster unloads, and keep Warehouse Writeback logs

CREATE DATABASE "CENSUS";
GRANT ALL PRIVILEGES ON DATABASE "CENSUS" TO ROLE CENSUS_ROLE;
-- If you want to explicitly grant the required permissions instead of using GRANT ALL you can use the following command
--GRANT USAGE, CREATE TABLE, CREATE VIEW,MODIFY, MONITOR ON DATABASE "CENSUS"."CENSUS" TO ROLE CENSUS_ROLE

CREATE SCHEMA "CENSUS"."CENSUS";
GRANT ALL PRIVILEGES ON SCHEMA "CENSUS"."CENSUS" TO ROLE CENSUS_ROLE;
-- If you want to explicitly grant the required permissions instead of using GRANT ALL you can use the following command
--GRANT CREATE TABLE, CREATE VIEW, MODIFY, MONITOR, CREATE STAGE  ON SCHEMA "CENSUS"."CENSUS" TO ROLE CENSUS_ROLE

GRANT CREATE STAGE ON SCHEMA "CENSUS"."CENSUS" TO ROLE CENSUS_ROLE;
