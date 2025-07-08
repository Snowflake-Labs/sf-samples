-- Create developer role
CREATE ROLE IF NOT EXISTS <% role_name %>;
GRANT ROLE <% role_name %> TO ROLE SYSADMIN;

-- Create user
CREATE USER IF NOT EXISTS <% user_name %>
    PASSWORD = 'temp123'
    MUST_CHANGE_PASSWORD = TRUE
    DEFAULT_ROLE = <% role_name %>
    TYPE = PERSON
;
GRANT ROLE <% role_name %> TO USER <% user_name %>;

-- Grant privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <% role_name %>;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE <% role_name %>;