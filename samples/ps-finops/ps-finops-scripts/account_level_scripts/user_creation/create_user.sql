CREATE USER IF NOT EXISTS <% ctx.env.finops_admin_user %>
    LOGIN_NAME=<% ctx.env.finops_admin_user %>
    DEFAULT_ROLE=<% ctx.env.finops_db_admin_role %>
    DEFAULT_WAREHOUSE=<% ctx.env.admin_wh %>
    ;
GRANT ROLE <% ctx.env.finops_db_admin_role %> TO USER <% ctx.env.finops_admin_user %>;
