USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW CORTEX_SEARCH_CC_CREDITS_DAY COMMENT = 'Title: Cortex Search Credits by Cost Center, Consumption type, model_name, and tokens used per day. Description: Analyze cortex search use costs by Cost Center, Consumption type, model_name, and tokens used per day.'
AS

SELECT
   DATE_TRUNC('DAY', SF.ADJUSTED_START_TIME) AS DAY,
   COALESCE(TS.TAG_VALUE, SERVICE_NAME) AS COST_CENTER,
   ZEROIFNULL(SUM(CREDITS)) AS CREDITS,  -- Total CREDITS for this day.
   CONSUMPTION_TYPE, --The category of consumption incurred. One of “SERVING”, “EMBED_TEXT_TOKENS”.
   MODEL_NAME, --For CONSUMPTION_TYPE = “EMBED_TEXT_TOKENS”, the name of the embedding model used to generate vector embeddings (nullable).
   TOKENS, --For CONSUMPTION_TYPE = “EMBED_TEXT_TOKENS”, the number of input tokens consumed (nullable).
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_QUERY_USAGE_HISTORY SFQ
--Schema
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ts ON ts.object_database = sf.database_name
  AND ts.object_name = sf.schema_name
  AND ts.TAG_NAME = '<% ctx.env.finops_tag_name %>'
  AND ts.DOMAIN = 'SCHEMA'
GROUP BY ALL
;
