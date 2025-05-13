/**********************************
FinOps Org V2 Setup for Snowflake.
Includes creating task for daily cost per query snapshot.

Requires SNOW CLI with snowflake.yml file for variables.

Roles used:
Delegated Admin which can be defined as SYSADMIN 

**********************************/

USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
USE WAREHOUSE <% ctx.env.admin_wh %>;

-- Cost per query initialization and automation with Snowflake tasks.

--Org 2.0 version
 --Initialize the high water mark for the query_cost_parameters table data from query_history:
INSERT INTO SF_QUERY_COST_PARAMETERS (
    PARAMETER_NAME,
    PARAM_VALUE_TIMESTAMP
    )
SELECT 
    x.$1,
    x.$2
FROM
VALUES (
    'org_query_start_time' ,
    CURRENT_DATE () - 31 --For first time run, change to any value up to -365.
    ) X
LEFT JOIN SF_QUERY_COST_PARAMETERS P ON P.PARAMETER_NAME = X.$1
WHERE P.PARAMETER_NAME IS NULL;

CREATE TASK SF_COST_PER_QUERY_ORG
  SCHEDULE = 'USING CRON 12 1 * * * America/Los_Angeles'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'SMALL'
  AS
    CALL COST_PER_QUERY_ORG();

--Enable the task:
ALTER TASK SF_COST_PER_QUERY_ORG RESUME;

-- If delegated admin was given account level execute task privilege, use the following command to execute the task or just rely on the schedule to kick it off:
-- EXECUTE TASK SF_COST_PER_QUERY_ORG;
