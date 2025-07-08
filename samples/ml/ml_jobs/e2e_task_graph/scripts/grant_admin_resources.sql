-- Grant usage to resources created in create_admin_resources.sql
GRANT USAGE ON WAREHOUSE <% warehouse_name %> TO ROLE <% role_name %>;
GRANT USAGE ON COMPUTE POOL <% compute_pool_name %> TO ROLE <% role_name %>;
GRANT USAGE ON INTEGRATION <% notification_integration_name %> TO ROLE <% role_name %>;