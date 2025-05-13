USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORGV1_REMAINING_BALANCE_CURRENCY COMMENT = 'Title: Organizational Remaining Balance in Currency. Description: Total remaining contract balance of organization in currency.'
as
SELECT DATE,
   FREE_USAGE_BALANCE + CAPACITY_BALANCE + ON_DEMAND_CONSUMPTION_BALANCE + ROLLOVER_BALANCE + MARKETPLACE_CAPACITY_DRAWDOWN_BALANCE AS BALANCE
FROM SNOWFLAKE.ORGANIZATION_USAGE.REMAINING_BALANCE_DAILY
;
