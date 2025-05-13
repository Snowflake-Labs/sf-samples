USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW SNOWPIPE_COSTS_CC_CREDITS_DAY COMMENT = 'Title: Snowpipe Credit Usage by Cost Center per day. Description: Analyze Snowpipe credit costs by pipe schema and cost center per day.'
AS
SELECT
   DATE_TRUNC('DAY', START_TIME) AS DAY,
   COALESCE(TS.TAG_VALUE, PIPE_SCHEMA) AS COST_CENTER,
   P.PIPE_CATALOG || '.' || P.PIPE_SCHEMA AS PIPE_SCHEMA,
   SUM(CREDITS_USED) AS CREDITS_USED   -- Total credits used for Snowpipes for this day.
-- , sum(files_inserted) as total_files_ingested
-- , case when total_files_ingested > 0 then (sum(bytes_inserted) / total_files_ingested ) / power(1024, 2) else 0 end as avg_mb_per_file
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY h
INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.PIPES p ON h.pipe_id = p.pipe_id
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ts ON ts.object_database = p.pipe_catalog
   AND ts.object_name = p.pipe_schema
   AND ts.TAG_NAME = '<% ctx.env.finops_tag_name %>'
   AND ts.DOMAIN = 'SCHEMA'
GROUP BY ALL;
