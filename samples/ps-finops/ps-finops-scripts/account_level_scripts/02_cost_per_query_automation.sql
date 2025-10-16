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

--Account version
CREATE OR REPLACE TASK SF_COST_PER_QUERY_ACCOUNT
  SCHEDULE = 'USING CRON 12 1 * * * America/Los_Angeles'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'SMALL'
  AS
    CALL COST_PER_QUERY_ACCOUNT();
