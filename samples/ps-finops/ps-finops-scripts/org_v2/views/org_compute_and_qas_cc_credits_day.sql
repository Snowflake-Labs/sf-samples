-- Note: Using organization_usage schema for ORG 2.0 Views
USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW ORG_COMPUTE_AND_QAS_CC_CREDITS_DAY COMMENT = 'Title: Compute & Query Acceleration Service (QAS) credits Usage by Cost Center per Day. Description: Analyze the major costs incurred in Snowflake Compute use. Does not account for extra cloud services fees above the 10% threshold.'
AS

SELECT
   DATE_TRUNC('DAY', SF.ADJUSTED_START_TIME) AS DAY,
   sf.ACCOUNT_NAME,
   CONCAT_WS('-', REGION, sf.ACCOUNT_NAME) AS REGION_ACCOUNT_NAME
   , GET_TAG_COST_CENTER(SF.QUERY_TAG, SF.USER_NAME, TU.TAG_VALUE, SF.ROLE_NAME, TR.TAG_VALUE, TS.TAG_VALUE, TW.TAG_VALUE) AS COST_CENTER
   , ROUND(SUM(ALLOCATED_CREDITS_USED_COMPUTE + ZEROIFNULL(ALLOCATED_CREDITS_USED_QAS)), 2) AS COMPUTE_CREDITS  -- TOTAL WAREHOUSE COST IN RESPECTIVE CREDITS FOR THIS DAY.
FROM SF_CREDITS_BY_QUERY_ORG SF
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TR
  ON TR.OBJECT_NAME = SF.ROLE_NAME
    AND TR.ACCOUNT_LOCATOR = SF.ACCOUNT_LOCATOR
    AND TR.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TR.DOMAIN = 'ROLE'
    AND SF.USER_NAME <> 'SYSTEM' -- Exclude Tasks that will be attributed to schema instead of Access Roles that are not tagged.
--Schema Snowflake Tasks
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TS
  ON TS.OBJECT_DATABASE = SF.DATABASE_NAME
    AND TS.ACCOUNT_LOCATOR = SF.ACCOUNT_LOCATOR
    AND TS.OBJECT_NAME = SF.SCHEMA_NAME
    AND TS.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TS.DOMAIN = 'SCHEMA'
    AND SF.USER_NAME = 'SYSTEM'
--User overrides
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TU
  ON TU.OBJECT_NAME = SF.USER_NAME
    AND TU.ACCOUNT_LOCATOR = SF.ACCOUNT_LOCATOR
    AND TU.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TU.DOMAIN = 'USER'
--Warehouse fallback if nothing else tagged.
LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.TAG_REFERENCES TW
  ON tw.object_id = sf.warehouse_id
    AND TW.ACCOUNT_LOCATOR = SF.ACCOUNT_LOCATOR
    AND TW.TAG_NAME = '<% ctx.env.finops_tag_name %>'
    AND TW.DOMAIN = 'WAREHOUSE'
GROUP BY ALL
HAVING COMPUTE_CREDITS > 0;
