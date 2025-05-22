/**********************************
FinOps Org V2 Setup for Snowflake.
Includes creating database roles and creating cost center tag.  Only run this on Organization Account.

Requires SNOW CLI with snowflake.yml file for variables.

Roles used:
Delegated Admin which can be defined as SYSADMIN 

**********************************/


------------------------------------------------------------------------
-- Create a db and schema to store the tags if one does not already exist.
-- Assuming the db admin has the rights to create a db and schema in the account and setup privileges.

USE ROLE <% ctx.env.finops_db_admin_role %>;

CREATE DATABASE IF NOT EXISTS <% ctx.env.finops_tag_db %>;
GRANT USAGE ON DATABASE <% ctx.env.finops_tag_db %> TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT USAGE ON DATABASE <% ctx.env.finops_tag_db %> TO ROLE <% ctx.env.finops_tag_admin_role %>;
GRANT USAGE ON DATABASE <% ctx.env.finops_tag_db %> TO ROLE <% ctx.env.finops_viewer_role %>;

GRANT CREATE DATABASE ROLE ON DATABASE <% ctx.env.finops_tag_db %> TO ROLE <% ctx.env.finops_tag_admin_role %>;

USE DATABASE <% ctx.env.finops_tag_db %>;
CREATE SCHEMA IF NOT EXISTS <% ctx.env.finops_tag_schema %> WITH MANAGED ACCESS;

GRANT USAGE ON SCHEMA <% ctx.env.finops_tag_schema %> TO ROLE <% ctx.env.finops_tag_admin_role %>;

USE ROLE <% ctx.env.finops_tag_admin_role %>;
SET sarR = 'SC_R_<% ctx.env.finops_tag_db %>';  -- READ access role (SC = Schema)
SET finops_tag_schema = '<% ctx.env.finops_tag_db %>.<% ctx.env.finops_tag_schema %>';

--Create Read Database Role:
CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER($sarR);
GRANT USAGE, MONITOR ON DATABASE <% ctx.env.finops_tag_db %>    TO DATABASE ROLE IDENTIFIER($sarR);
GRANT USAGE, MONITOR ON SCHEMA IDENTIFIER($finops_tag_schema)   TO DATABASE ROLE IDENTIFIER($sarR); 

GRANT CREATE TAG ON SCHEMA IDENTIFIER($finops_tag_schema)       TO ROLE <% ctx.env.finops_tag_admin_role %>;


--Create the cost center tag
SET FQ_TAGNAME = $finops_tag_schema||'.<% ctx.env.finops_tag_name %>';
CREATE TAG IF NOT EXISTS identifier($FQ_TAGNAME) COMMENT = 'For tracking cost of storage, compute, and managed services.';

GRANT READ ON TAG identifier($FQ_TAGNAME) TO DATABASE ROLE IDENTIFIER($sarR);

--Enable access for the necessary roles
GRANT DATABASE ROLE IDENTIFIER($sarR) TO ROLE <% ctx.env.finops_db_admin_role %>;
GRANT DATABASE ROLE IDENTIFIER($sarR) TO ROLE <% ctx.env.finops_viewer_role %>;
