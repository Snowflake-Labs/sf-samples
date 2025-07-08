-- Grant access to the service user role
GRANT USAGE ON DATABASE <% database_name %> TO ROLE <% role_name %>;
GRANT USAGE ON SCHEMA <% database_name %>.<% schema_name %> TO ROLE <% role_name %>;
GRANT READ, WRITE ON STAGE <% database_name %>.<% schema_name %>.DAG_STAGE TO ROLE <% role_name %>;
GRANT READ, WRITE ON STAGE <% database_name %>.<% schema_name %>.JOB_STAGE TO ROLE <% role_name %>;

-- Feature Store and Data privileges
GRANT CREATE DYNAMIC TABLE ON SCHEMA <% database_name %>.<% schema_name %> TO ROLE <% role_name %>;
GRANT CREATE TABLE, CREATE DATASET ON SCHEMA <% database_name %>.<% schema_name %> TO ROLE <% role_name %>;

-- Model privileges
GRANT CREATE MODEL ON SCHEMA <% database_name %>.<% schema_name %> TO ROLE <% role_name %>;
GRANT CREATE MODEL MONITOR ON SCHEMA <% database_name %>.<% schema_name %> TO ROLE <% role_name %>;  -- Unused

-- Task Graph privileges
GRANT CREATE TASK, CREATE SERVICE, CREATE PROCEDURE ON SCHEMA <% database_name %>.<% schema_name %> TO ROLE <% role_name %>;
GRANT CREATE ALERT ON SCHEMA <% database_name %>.<% schema_name %> TO ROLE <% role_name %>;  -- Unused