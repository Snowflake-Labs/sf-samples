/**********************************
FinOps Account Setup for Snowflake.
Includes creating databases, schemas, roles, and warehouses.

Assumes connection is SNOW cli with appropriate user that has ACCOUNTADMIN role level access.
Requires SNOW CLI with snowflake.yml file for variables.

Roles used:
ACCOUNTADMIN
SECURITYADMIN
Custom roles defined in snowflake.yml

**********************************/
SET curr_user = (SELECT CONCAT('"', CURRENT_USER(), '"')); -- Double quotes are necessary to handle username with email address

--Create WH to use if one does not already exist
USE ROLE <% ctx.env.wh_creation_role %>;
CREATE WAREHOUSE IF NOT EXISTS <% ctx.env.admin_wh%>
    WAREHOUSE_SIZE = XSMALL,
    AUTO_SUSPEND = 30,
    AUTO_RESUME= TRUE,
    INITIALLY_SUSPENDED = TRUE;

GRANT MONITOR, USAGE ON WAREHOUSE <% ctx.env.admin_wh %> TO ROLE <% ctx.env.wh_creation_role %>;

USE WAREHOUSE <% ctx.env.admin_wh %>;

--The dynamic sql below requires a WH.
GRANT USAGE ON WAREHOUSE <% ctx.env.admin_wh %> TO ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE <% ctx.env.admin_wh %> TO ROLE ACCOUNTADMIN;


USE ROLE SECURITYADMIN;
--Create finops_db_admin_role if not using sysadmin and it does not already exist
EXECUTE IMMEDIATE $$
DECLARE
    wh_admin varchar default '<% ctx.env.finops_db_admin_role %>';
BEGIN
    IF (wh_admin <> 'SYSADMIN') THEN
        EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS <% ctx.env.finops_db_admin_role %>';
        EXECUTE IMMEDIATE 'GRANT ROLE <% ctx.env.finops_db_admin_role %> TO ROLE SYSADMIN'; -- OPTIONAL: Give the db admin role to the SYSADMIN role
    END IF;
    EXECUTE IMMEDIATE 'GRANT ROLE <% ctx.env.finops_db_admin_role %> TO USER ' ||CURRENT_USER(); -- Give the current user the db admin role
END;
$$;

-- Optionally give finops_db_admin_role ownership on the WH instead of wh_creation_role role we used above.
-- USE ROLE <% ctx.env.wh_creation_role %>;
-- --Grant OWNERSHIP of the Admin WH to the FinOps db Admin Role
-- EXECUTE IMMEDIATE $$
-- DECLARE
--     wh_admin varchar default '<% ctx.env.finops_db_admin_role %>';
-- BEGIN
--     IF (wh_admin <> 'SYSADMIN') THEN
--         EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON WAREHOUSE <% ctx.env.admin_wh %> TO ROLE <% ctx.env.finops_db_admin_role %>  REVOKE CURRENT GRANTS';
--     END IF;
-- END;
-- $$;

--Or if you don't want to give ownership, just grant the necessary rights:
GRANT MONITOR, USAGE ON WAREHOUSE <% ctx.env.admin_wh %> TO ROLE <% ctx.env.finops_db_admin_role %>;

USE ROLE SECURITYADMIN;
--Create data viewer role for finops and Snowflake use
CREATE ROLE IF NOT EXISTS <% ctx.env.finops_viewer_role %>;

GRANT MONITOR, USAGE ON WAREHOUSE <% ctx.env.admin_wh %> TO ROLE <% ctx.env.finops_viewer_role %>;

--Give the viewer role to the db admin role
GRANT ROLE <% ctx.env.finops_viewer_role %> TO ROLE <% ctx.env.finops_db_admin_role %>;


--Give current user ability to use these new delegated roles.
GRANT ROLE <% ctx.env.finops_db_admin_role %> TO USER identifier($curr_user) ;
GRANT ROLE <% ctx.env.finops_db_admin_role %> TO USER <% ctx.env.finops_admin_user %> ;
GRANT ROLE <% ctx.env.finops_viewer_role %> TO USER identifier($curr_user) ;


-- Optionally, give db admin role rights to create a database:
USE ROLE ACCOUNTADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <% ctx.env.finops_db_admin_role%>;

--Create finops_tag_admin_role if not using sysadmin and it does not already exist and parent it to SYSADMIN and give WH rights
EXECUTE IMMEDIATE $$
DECLARE
    tag_admin varchar default '<% ctx.env.finops_tag_admin_role %>';
BEGIN
    IF (tag_admin <> 'SYSADMIN') THEN
        EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS <% ctx.env.finops_tag_admin_role %>';
        EXECUTE IMMEDIATE 'GRANT ROLE <% ctx.env.finops_tag_admin_role %> TO ROLE SYSADMIN'; -- OPTIONAL: Give the tag admin role to the SYSADMIN role
        EXECUTE IMMEDIATE 'GRANT USAGE ON WAREHOUSE <% ctx.env.admin_wh%> TO ROLE <% ctx.env.finops_tag_admin_role %>';
        -- Grant this role to have global tag administration rights:
        EXECUTE IMMEDIATE 'GRANT APPLY TAG ON ACCOUNT TO ROLE <% ctx.env.finops_tag_admin_role %>';
    END IF;
END;
$$;

-- Grant ROLE to current user for tag admin
GRANT ROLE <% ctx.env.finops_tag_admin_role %> TO USER identifier($curr_user);

-- Switch back to delegated admin
USE ROLE <% ctx.env.finops_db_admin_role %>;

-- Create a db and schema to store the finops objects if one does not already exist.
CREATE DATABASE IF NOT EXISTS <% ctx.env.finops_acct_db %>;
USE DATABASE <% ctx.env.finops_acct_db %>;

-- GRANT USAGE ON DATABASE <% ctx.env.finops_acct_db %> TO ROLE <% ctx.env.finops_tag_admin_role %>;
GRANT USAGE ON DATABASE <% ctx.env.finops_acct_db %> TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT USAGE ON DATABASE <% ctx.env.finops_acct_db %> TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT CREATE DATABASE ROLE ON DATABASE <% ctx.env.finops_acct_db %> TO ROLE <% ctx.env.finops_db_admin_role %>;

CREATE SCHEMA IF NOT EXISTS <% ctx.env.finops_acct_schema %> WITH MANAGED ACCESS;

-- USE SCHEMA <% ctx.env.finops_acct_schema %>;
-- GRANT USAGE ON SCHEMA <% ctx.env.finops_acct_schema %> TO ROLE <% ctx.env.finops_tag_admin_role %>;
GRANT USAGE ON SCHEMA <% ctx.env.finops_acct_schema %> TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT USAGE ON SCHEMA <% ctx.env.finops_acct_schema %> TO ROLE <% ctx.env.finops_viewer_role %>;


USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;

SET finops_object_schema = '<% ctx.env.finops_acct_db %>.<% ctx.env.finops_acct_schema %>';


-- Construct the schema Access Role names for Read, Write.
-- The role $finops_db_admin_role will own the DB and Schema.
SET sarR = 'SC_R_<% ctx.env.finops_acct_db %>';  -- READ access role (SC = Schema)
SET sarW = 'SC_W_<% ctx.env.finops_acct_db %>';  -- WRITE access role - Currently not used.

--Read Database Role:
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarR);
GRANT USAGE, MONITOR ON DATABASE <% ctx.env.finops_acct_db %>      TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE, MONITOR ON SCHEMA IDENTIFIER($finops_object_schema)   TO DATABASE ROLE IDENTIFIER($sarR);

GRANT SELECT ON ALL TABLES                IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE TABLES             IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL VIEWS                 IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE VIEWS              IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE  ON ALL FUNCTIONS             IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE  ON FUTURE FUNCTIONS          IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL DYNAMIC TABLES        IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE DYNAMIC TABLES     IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON ALL MATERIALIZED VIEWS    IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarR);


--Write Database Role:
-- In current implementation, this isn't used, it is assumed the Admin role will be used.
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarW);

GRANT INSERT, UPDATE, DELETE, TRUNCATE  ON ALL TABLES            IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT INSERT, UPDATE, DELETE, TRUNCATE  ON FUTURE TABLES         IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON ALL FUNCTIONS         IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON FUTURE FUNCTIONS      IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON ALL SEQUENCES         IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE                             ON FUTURE SEQUENCES      IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL TASKS             IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE TASKS          IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE, READ, WRITE                ON ALL STAGES            IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT USAGE, READ, WRITE                ON FUTURE STAGES         IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT SELECT                            ON ALL STREAMS           IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT SELECT                            ON FUTURE STREAMS        IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL DYNAMIC TABLES    IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE DYNAMIC TABLES IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON ALL ALERTS            IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);
GRANT MONITOR, OPERATE                  ON FUTURE ALERTS         IN SCHEMA IDENTIFIER($finops_object_schema) TO DATABASE ROLE IDENTIFIER($sarW);


-- Access Database role Inheritance Hierarchy
GRANT DATABASE ROLE IDENTIFIER($sarR) TO DATABASE ROLE IDENTIFIER($sarW);  -- the Write role inherits all the privileges of the Read role, so no need to duplicate the grants

GRANT DATABASE ROLE IDENTIFIER($sarW) TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT DATABASE ROLE IDENTIFIER($sarR) TO ROLE <% ctx.env.finops_viewer_role %>;


-- Grant usage of Snowflake telemetry data.
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE;

GRANT DATABASE ROLE GOVERNANCE_VIEWER TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT DATABASE ROLE GOVERNANCE_VIEWER TO ROLE <% ctx.env.finops_db_admin_role%>;
GRANT DATABASE ROLE OBJECT_VIEWER TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT DATABASE ROLE OBJECT_VIEWER TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT DATABASE ROLE USAGE_VIEWER TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT DATABASE ROLE USAGE_VIEWER TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT DATABASE ROLE ORGANIZATION_BILLING_VIEWER TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT DATABASE ROLE ORGANIZATION_BILLING_VIEWER TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT DATABASE ROLE ORGANIZATION_USAGE_VIEWER TO ROLE <% ctx.env.finops_viewer_role %>;
GRANT DATABASE ROLE ORGANIZATION_USAGE_VIEWER TO ROLE <% ctx.env.finops_db_admin_role %>;

-- Optional: Account level setting to allow delegated admin ability to start any Snowflake task:
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE <% ctx.env.finops_db_admin_role %>;

-- Grant ability to create serverless tasks on the account:
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE <% ctx.env.finops_db_admin_role %>;
