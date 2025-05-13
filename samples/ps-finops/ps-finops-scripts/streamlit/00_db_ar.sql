/**********************************
FinOps Account Setup for Snowflake.
Includes creating databases, schemas, roles, and warehouses.

Assumes connection is SNOW cli with appropriate user that has ACCOUNTADMIN role level access.
Requires SNOW CLI with snowflake.yml file for variables.

Roles used:
SECURITYADMIN
Custom roles defined in snowflake.yml

**********************************/
SET curr_user = (SELECT CONCAT('"', CURRENT_USER(), '"')); -- Double quotes are necessary to handle username with email address

USE ROLE SECURITYADMIN;
USE WAREHOUSE <% ctx.env.admin_wh %>;
--Create finops_db_admin_role if not using sysadmin and it does not already exist
EXECUTE IMMEDIATE $$
DECLARE
    wh_admin varchar default '<% ctx.env.finops_sis_admin %>';
BEGIN
    IF (wh_admin <> 'SYSADMIN') THEN
        EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS <% ctx.env.finops_sis_admin %>';
        EXECUTE IMMEDIATE 'GRANT ROLE <% ctx.env.finops_sis_admin %> TO ROLE SYSADMIN'; -- OPTIONAL: Give the db admin role to the SYSADMIN role
    END IF;
END;
$$;

GRANT USAGE ON WAREHOUSE <% ctx.env.admin_wh %> TO ROLE <% ctx.env.finops_sis_admin %>;

--Create data viewer role for finops and Snowflake use
CREATE ROLE IF NOT EXISTS <% ctx.env.finops_sis_user %>;

--Give the viewer role to the db admin role
GRANT ROLE <% ctx.env.finops_sis_user %> TO ROLE <% ctx.env.finops_sis_admin %>;


--Give current user ability to use these new delegated roles.
GRANT ROLE <% ctx.env.finops_sis_admin %> TO USER identifier($curr_user) ;
GRANT ROLE <% ctx.env.finops_sis_admin %> TO USER <% ctx.env.finops_admin_user %> ;
GRANT ROLE <% ctx.env.finops_sis_user %> TO USER identifier($curr_user) ;


-- Switch back to delegated admin
USE ROLE <% ctx.env.finops_db_admin_role %>;
CREATE DATABASE IF NOT EXISTS <% ctx.env.finops_sis_db %>;
USE DATABASE <% ctx.env.finops_sis_db %>;
CREATE SCHEMA IF NOT EXISTS <% ctx.env.finops_sis_db %>.<% ctx.env.finops_sis_tag_sc %> WITH MANAGED ACCESS;
CREATE SCHEMA IF NOT EXISTS <% ctx.env.finops_sis_db %>.<% ctx.env.finops_sis_usage_sc %> WITH MANAGED ACCESS;


-- TAG APP SCHEMA RBAC SETUP
SET finops_tag_schema = '<% ctx.env.finops_sis_db %>_<% ctx.env.finops_sis_tag_sc %>';
SET fqn_schema = '<% ctx.env.finops_sis_db %>.<% ctx.env.finops_sis_tag_sc %>';


-- Construct the schema Access Role names for Read, Write.
-- The role $finops_db_admin_role will own the DB and Schema.
SET sarR = 'SC_R_'||$finops_tag_schema;  -- READ access role (SC = Schema)
SET sarW = 'SC_W_'||$finops_tag_schema;  -- WRITE access role - Currently not used.
SET sarC = 'SC_C_'||$finops_tag_schema;
SET sarA = 'SC_'||$finops_tag_schema||'_APPS';  -- APP access role - Used to access SiS app in schema only.

--Read Database Role:
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarR);
GRANT MONITOR ON DATABASE <% ctx.env.finops_sis_db %>    TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE, MONITOR ON SCHEMA IDENTIFIER($fqn_schema)   TO DATABASE ROLE IDENTIFIER($sarR);

GRANT SELECT ON ALL TABLES                IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE TABLES             IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL VIEWS                 IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE VIEWS              IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE  ON ALL FUNCTIONS             IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE  ON FUTURE FUNCTIONS          IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL DYNAMIC TABLES        IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE DYNAMIC TABLES     IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL MATERIALIZED VIEWS    IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);


--Write Database Role:
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarW);

GRANT INSERT, UPDATE, DELETE, TRUNCATE  ON ALL TABLES            IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT INSERT, UPDATE, DELETE, TRUNCATE  ON FUTURE TABLES         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON ALL FUNCTIONS         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON FUTURE FUNCTIONS      IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON ALL SEQUENCES         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON FUTURE SEQUENCES      IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL TASKS             IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE TASKS          IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE, READ, WRITE                ON ALL STAGES            IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE, READ, WRITE                ON FUTURE STAGES         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT SELECT                            ON ALL STREAMS           IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT SELECT                            ON FUTURE STREAMS        IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL DYNAMIC TABLES    IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE DYNAMIC TABLES IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL ALERTS            IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE ALERTS         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);

-- Create Database Role
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarC);
GRANT CREATE TABLE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE VIEW ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE STREAM ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE PROCEDURE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE STAGE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE STREAMLIT ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);

-- App Database Role
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarA);
GRANT USAGE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarA);
GRANT USAGE ON ALL STREAMLITS IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarA);
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarA);

-- Access Database role Inheritance Hierarchy
GRANT DATABASE ROLE IDENTIFIER($sarR) TO DATABASE ROLE IDENTIFIER($sarW);  -- the Write role inherits all the privileges of the Read role, so no need to duplicate the grants
GRANT DATABASE ROLE IDENTIFIER($sarW) TO DATABASE ROLE IDENTIFIER($sarC);  -- the Create role inherits all the privileges of the Write role, so no need to duplicate the grants
GRANT DATABASE ROLE IDENTIFIER($sarA) TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT DATABASE ROLE IDENTIFIER($sarA) TO ROLE <% ctx.env.finops_sis_user %>;
GRANT DATABASE ROLE IDENTIFIER($sarC) TO ROLE <% ctx.env.finops_sis_admin %>;

-- USAGE APP SCHEMA RBAC SETUP
SET finops_usage_schema = '<% ctx.env.finops_sis_db %>_<% ctx.env.finops_sis_usage_sc %>';
SET fqn_schema = '<% ctx.env.finops_sis_db %>.<% ctx.env.finops_sis_usage_sc %>';


-- Construct the schema Access Role names for Read, Write.
-- The role $finops_db_admin_role will own the DB and Schema.
SET sarR = 'SC_R_'||$finops_usage_schema;  -- READ access role (SC = Schema)
SET sarW = 'SC_W_'||$finops_usage_schema;  -- WRITE access role - Currently not used.
SET sarC = 'SC_C_'||$finops_usage_schema;
SET sarA = 'SC_'||$finops_usage_schema||'_APPS';  -- APP access role - Used to access SiS app in schema only.

--Read Database Role:
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarR);
GRANT MONITOR ON DATABASE <% ctx.env.finops_sis_db %>      TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE, MONITOR ON SCHEMA IDENTIFIER($fqn_schema)   TO DATABASE ROLE IDENTIFIER($sarR);

GRANT SELECT ON ALL TABLES                IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE TABLES             IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL VIEWS                 IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE VIEWS              IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE  ON ALL FUNCTIONS             IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE  ON FUTURE FUNCTIONS          IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL DYNAMIC TABLES        IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE DYNAMIC TABLES     IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL MATERIALIZED VIEWS    IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarR);


--Write Database Role:
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarW);

GRANT INSERT, UPDATE, DELETE, TRUNCATE  ON ALL TABLES            IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT INSERT, UPDATE, DELETE, TRUNCATE  ON FUTURE TABLES         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON ALL FUNCTIONS         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON FUTURE FUNCTIONS      IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON ALL SEQUENCES         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON FUTURE SEQUENCES      IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL TASKS             IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE TASKS          IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE, READ, WRITE                ON ALL STAGES            IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE, READ, WRITE                ON FUTURE STAGES         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT SELECT                            ON ALL STREAMS           IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT SELECT                            ON FUTURE STREAMS        IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL DYNAMIC TABLES    IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE DYNAMIC TABLES IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL ALERTS            IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE ALERTS         IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarW);

-- Create Database Role
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarC);
GRANT CREATE TABLE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE VIEW ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE STAGE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE STAGE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);
GRANT CREATE STREAMLIT ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarC);

-- App Database Role
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarA);

GRANT USAGE ON SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarA);
GRANT USAGE ON ALL STREAMLITS IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarA);
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA IDENTIFIER($fqn_schema) TO DATABASE ROLE IDENTIFIER($sarA);

-- Access Database role Inheritance Hierarchy
GRANT DATABASE ROLE IDENTIFIER($sarR) TO DATABASE ROLE IDENTIFIER($sarW);  -- the Write role inherits all the privileges of the Read role, so no need to duplicate the grants
GRANT DATABASE ROLE IDENTIFIER($sarW) TO DATABASE ROLE IDENTIFIER($sarC);  -- the Create role inherits all the privileges of the Write role, so no need to duplicate the grants
GRANT DATABASE ROLE IDENTIFIER($sarA) TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT DATABASE ROLE IDENTIFIER($sarA) TO ROLE <% ctx.env.finops_sis_user %>;
GRANT DATABASE ROLE IDENTIFIER($sarC) TO ROLE <% ctx.env.finops_sis_admin %>;

-- Grant usage of finops data.
USE ROLE <% ctx.env.finops_db_admin_role %>;
GRANT DATABASE ROLE <% ctx.env.finops_acct_db %>.SC_R_<% ctx.env.finops_acct_db %> to role <% ctx.env.finops_sis_admin %>;
