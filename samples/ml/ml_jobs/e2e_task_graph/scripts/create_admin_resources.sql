-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS <% warehouse_name %>;

-- Create compute pool
CREATE COMPUTE POOL IF NOT EXISTS <% compute_pool_name %>
    MIN_NODES = 2
    MAX_NODES = 5
    INSTANCE_FAMILY = CPU_X64_M;

-- Set up notification integration
-- https://docs.snowflake.com/en/user-guide/notifications/webhook-notifications
-- (ACTION NEEDED) Create a webhook e.g. in Slack
CREATE SCHEMA IF NOT EXISTS <% database_name %>.<% schema_name %>;
CREATE SECRET IF NOT EXISTS <% database_name %>.<% schema_name %>.DEMO_SLACK_WEBHOOK_SECRET
  TYPE = GENERIC_STRING
  SECRET_STRING = 'T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX'; -- (ACTION NEEDED) Put your Slack webhook secret here
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS <% notification_integration_name %>
  TYPE=WEBHOOK
  ENABLED=TRUE
  WEBHOOK_URL='https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
  WEBHOOK_SECRET=<% database_name %>.<% schema_name %>.DEMO_SLACK_WEBHOOK_SECRET
  WEBHOOK_BODY_TEMPLATE='{"text": "SNOWFLAKE_WEBHOOK_MESSAGE"}'
  WEBHOOK_HEADERS=('Content-Type'='application/json');