USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW TABLE_STORAGE_PASSIVE_ACTIVE_RATIO_TOP50 COMMENT = 'Title: TOP 50 tables with passive storage greater than active storage. Description: Analyze the ratio of passive (Time Travel + Failsafe) to active storage for the top 50 worst tables.'
AS
SELECT  
    table_catalog||'.'||table_schema as SCHEMA_NAME
    , table_catalog||'.'||table_schema ||'.'||table_name AS FULL_TABLE_NAME
    -- , REGEXP_REPLACE(table_name, '[0-9]', '') table_name_sans_num
    , COUNT(DISTINCT TABLE_ID) AS Table_Count
    , IFF(is_transient = 'NO', 1, 0) as FailSafe_In_Use
    , ROUND(SUM(active_bytes) / POWER(1024,3),1) as Active_GB
    , ROUND(SUM(time_travel_bytes) / POWER(1024,3),2) AS TT_GB
    , ROUND(SUM(failsafe_bytes) / POWER(1024,3),2) AS FS_GB
    , ROUND(SUM(time_travel_bytes + failsafe_bytes) / POWER(1024,3),1) AS TTFS_GB
    , MAX(LEAST(TABLE_RENTENTION_DAYS + iff(is_transient = 'NO', 7, 0), table_active_lifespan_days)) AS TTFS_DAYS
    , ROUND(div0(TTFS_GB, Active_GB),1) as Passive_size_ratio -- How many times Passive storage is greater than active.
FROM table_storage_detailed t
WHERE DATE_COLLECTED > (select max(date(date_collected)) from table_storage_detailed)
GROUP BY ALL
HAVING Passive_size_ratio > 1 -- Can be even more aggressive.
    -- AND active_gb > 1 --adjust this to remove small tables
ORDER BY Passive_size_ratio DESC 
LIMIT 50
;
