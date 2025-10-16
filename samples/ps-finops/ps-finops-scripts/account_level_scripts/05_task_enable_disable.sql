/**********************************
FinOps Account Setup for Snowflake.
Includes creating task for daily cost per query snapshot.

Requires SNOW CLI with snowflake.yml file for variables.

Roles used:
Delegated Admin which can be defined as SYSADMIN 

**********************************/

USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;


--Enable the task:
-- ALTER TASK IF EXISTS SF_COST_PER_QUERY_ACCOUNT SUSPEND;
ALTER TASK IF EXISTS SF_COST_PER_QUERY_ACCOUNT RESUME;

-- ALTER TASK IF EXISTS TABLE_STORAGE_DETAILED_POP SUSPEND;
ALTER TASK IF EXISTS TABLE_STORAGE_DETAILED_POP RESUME;

-- ALTER TASK IF EXISTS HYBRID_STORAGE_DETAILED_POP SUSPEND;
ALTER TASK IF EXISTS HYBRID_STORAGE_DETAILED_POP RESUME;
