--This function should be modified as customer desires to get the cost_center name output prioritized like this or in any manner.
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE FUNCTION GET_TAG_COST_CENTER (QUERY_TAG VARCHAR, USER_NAME VARCHAR, ROLE_NAME VARCHAR, SCHEMA_TAG_VALUE VARCHAR, ROLE_TAG_VALUE VARCHAR, 
USER_TAG_VALUE VARCHAR, WAREHOUSE_TAG_VALUE VARCHAR)
RETURNS VARCHAR
  AS
$$
SELECT coalesce(
    IFF(QUERY_TAG <> '',try_parse_json(QUERY_TAG):"cost_center"::varchar, NULL)
    , user_tag_value -- User/service account tagged when they can always be attributed to a cost center.
    , schema_tag_value -- Schema tag used for Snowflake Tasks with Warehouse use.
    , role_tag_value -- Role use of query.
    , warehouse_tag_value -- Fallback if the above are not available.
    , role_name || ' - ' || user_name -- Fallback if all tags missing
    , 'Unknown' -- Last resort value.
    ) as TEAM
$$
;
