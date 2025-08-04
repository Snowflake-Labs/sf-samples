USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;

CREATE OR REPLACE PROCEDURE COST_PER_QUERY_ORG()
 RETURNS VARCHAR
 LANGUAGE SQL
 COMMENT = 'Procedure to allocate warehouse costs to individual queries'
 EXECUTE AS OWNER
AS
$$
/*****
Purpose: Provide *APPROXIMATE* per query cost for you to join with team information (you provide) to inform teams on their spend with Warehouse Compute and Query Acceleration Service costs.

What is provided: This procedure stores per query cost allocation since its last run for compute and includes Query Acceleration Service costs in its own column.
This procedure was created before Snowflake had a per query cost allocation feature. The new QUERY_ATTRIBUTION_HISTORY view does provide a cost per query, but leaves out 
warehouse idle time as an exercise for you. 
How:
It does an incremental load of query history and warehouse metering history and allocates the credits for that hour and warehouse across
the queries using warehouse compute based on query execution time. It evenly distributes the cost across all execution time for that hour using the metering history
dollar amount. Basically, it linearly adds up all the execution time, figures out each queries total percent of the hour's total time * WH server count * Query Load Percentage
using native Snowflake function, ratio_to_report. Similar process for QAS time.

Limitations: Since we don't have per cluster cost metrics, in multi-cluster implementations (MCW), we are averaging across all the warehouses, which could not be fair for small time frames like a day, but hopefully with the law of averages, over say a week's period in well-used environments, this should be fine. Also, in highly concurrent environments with short and long running jobs, the concurrent queries get a larger percentage of the cost. So, if you need higher precision use QUERY_ATTRIBUTION_HISTORY and figure out a way to charge for idle time.

Revision:
2024/08/12 CL Update for ORG 2.0 preview. QAS data is at daily grain, so removed it for now.
2025/04/03 CL Added filter to exclude queries on Snowflake Compute Service.
2025/05/20 CL Added CALL statements and incorporated query_load_percent into warehouse_weight and table output
******/

DECLARE 
    START_TIME_RANGE TIMESTAMP_LTZ;
    END_TIME_RANGE TIMESTAMP_LTZ;
    MINIMUM_LAG NUMBER DEFAULT 4; --end_time should be more than 4 hours past because of latency in organization_usage.warehouse_metering_history. 
    LOOKBACK_HOURS NUMBER DEFAULT 24 ;  --Change to your longest running query time to reprocess data to capture long running queries that would have been missed.
    ROWCOUNT NUMBER;

BEGIN

SELECT DATE_TRUNC('HOUR',PARAM_VALUE_TIMESTAMP) AS START_TIME,
        DATE_TRUNC('HOUR',DATEADD('HOUR',-:MINIMUM_LAG,CURRENT_TIMESTAMP()))::TIMESTAMP_LTZ AS END_TIME
INTO   :START_TIME_RANGE, :END_TIME_RANGE
FROM SF_QUERY_COST_PARAMETERS
WHERE PARAMETER_NAME = 'org_query_start_time';

--decrease start_time range by lookback hours so that we reprocess the lookback time range
START_TIME_RANGE :=  DATEADD('HOURS',-LOOKBACK_HOURS,START_TIME_RANGE);

DELETE FROM SF_CREDITS_BY_QUERY_ORG
WHERE START_SLICE >= :START_TIME_RANGE;

ROWCOUNT := (SELECT DATEDIFF('HOUR', :START_TIME_RANGE, :END_TIME_RANGE));


CREATE OR REPLACE TEMPORARY TABLE TEMP_TIME_SLICE_BY_INTERVAL
AS 
SELECT
    SEQ4()::NUMBER AS SEQ_NUM,
    ROW_NUMBER() OVER (ORDER BY SEQ_NUM)::NUMBER AS INDEX,
    :ROWCOUNT + 1 - INDEX ROW_SEQ_NUM,
    DATEADD(
        'HOUR',
        -1 * INDEX,
        :END_TIME_RANGE)::TIMESTAMP_LTZ AS START_SLICE,
    DATEADD('HOUR', 1, START_SLICE)::TIMESTAMP_LTZ AS END_SLICE
FROM
    TABLE(GENERATOR(ROWCOUNT =>(:ROWCOUNT)));

    
CREATE OR REPLACE TEMPORARY TABLE TEMP_SF_QUERY_HISTORY AS
SELECT
    Q.ACCOUNT_NAME,
    Q.ACCOUNT_LOCATOR,
    Q.REGION,
    Q.QUERY_ID,
    Q.START_TIME,
    Q.END_TIME,
    Q.QUERY_TYPE,
    '' AS QUERY_TEXT, --Add if appropriate instead of making blank.
    Q.QUERY_TAG,
    Q.QUERY_PARAMETERIZED_HASH AS SQL_HASH,
    Q.DATABASE_ID,
    Q.DATABASE_NAME,
    Q.SCHEMA_NAME,
    Q.SESSION_ID,
    Q.USER_NAME,
    Q.ROLE_NAME,
    Q.WAREHOUSE_NAME,
    Q.WAREHOUSE_ID,
    Q.WAREHOUSE_SIZE,
    Q.WAREHOUSE_TYPE,
    CASE WHEN Q.QUERY_TYPE = 'CALL' THEN 1/8::NUMBER(32,4) ELSE NVL(QUERY_LOAD_PERCENT,100)/100 * W.WAREHOUSE_WEIGHT END AS WAREHOUSE_WEIGHT, --1 cpu for CALL queries and factor WH node count into weight value.
    Q.QUERY_LOAD_PERCENT,
    Q.CLUSTER_NUMBER,
    Q.QUERY_ACCELERATION_BYTES_SCANNED AS QAS_BYTES_USED,
    Q.TOTAL_ELAPSED_TIME,
    --Q.CREDITS_USED_CLOUD_SERVICES::NUMBER(38,12) AS CREDITS_USED_CLOUD_SERVICES_BY_QUERY,
    Q.QUEUED_PROVISIONING_TIME + Q.QUEUED_REPAIR_TIME + Q.QUEUED_OVERLOAD_TIME AS TOTAL_QUEUE_TIME,
    Q.TRANSACTION_BLOCKED_TIME,
    DATEADD(
        'MILLISECOND',(TOTAL_QUEUE_TIME+TRANSACTION_BLOCKED_TIME+Q.COMPILATION_TIME+Q.LIST_EXTERNAL_FILES_TIME),
        Q.START_TIME )::TIMESTAMP_LTZ AS ADJUSTED_START_TIME,
    DATE_TRUNC('HOUR', ADJUSTED_START_TIME)::TIMESTAMP_LTZ AS ADJUSTED_START_INTERVAL
FROM
    SNOWFLAKE.ORGANIZATION_USAGE.QUERY_HISTORY Q
JOIN SF_WAREHOUSE_WEIGHTS W ON Q.WAREHOUSE_SIZE = W.WAREHOUSE_SIZE -- FORCE SIZE VALUE TO EXIST.
        AND Q.WAREHOUSE_TYPE = W.WAREHOUSE_TYPE
WHERE
    START_TIME < :END_TIME_RANGE
    AND END_TIME >= :START_TIME_RANGE
    AND WAREHOUSE_NAME NOT LIKE 'COMPUTE_SERVICE%' --Snowflake compute service use that is visible, but already billed by services it supports.
    AND NOT (QUERY_TYPE = 'SELECT' AND BYTES_SCANNED = 0) --info schema queries we can ignore as mostly CS time 
    AND Q.WAREHOUSE_SIZE IS NOT NULL --Exclude queries that fail compilation or are just cloud services use.
    ;


INSERT INTO SF_CREDITS_BY_QUERY_ORG (
    ACCOUNT_NAME,
    ACCOUNT_LOCATOR,
    REGION,
    QUERY_ID,
    START_SLICE,
    ADJUSTED_START_TIME,
    START_TIME,
    END_TIME,
    QUERY_TYPE,
    QUERY_TEXT,
    QUERY_TAG,
    SQL_HASH,
    DATABASE_ID,
    DATABASE_NAME,
    SCHEMA_NAME,
    SESSION_ID,
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_NAME,
    WAREHOUSE_ID,
    WAREHOUSE_SIZE,
    WAREHOUSE_TYPE,
    WAREHOUSE_WEIGHT,
    QUERY_LOAD_PERCENT,
    CLUSTER_NUMBER,
    QAS_BYTES_USED,
    TOTAL_ELAPSED_TIME,
    TOTAL_QUEUE_TIME,
    TRANSACTION_BLOCKED_TIME,
    DERIVED_ELAPSED_TIME_MS,
    ELAPSED_TIME_RATIO,
    ALLOCATED_CREDITS_USED,
    ALLOCATED_CREDITS_USED_COMPUTE,
    ALLOCATED_CREDITS_USED_CLOUD_SERVICES,
    -- , ALLOCATED_CREDITS_USED_QAS
    )
WITH SF_WAREHOUSE_METERING_HISTORY AS(
    SELECT
        WMH.ACCOUNT_NAME,
        WMH.ACCOUNT_LOCATOR,
        WMH.REGION,
        WMH.START_TIME,
        WMH.WAREHOUSE_ID,
        WMH.WAREHOUSE_NAME,
        WMH.CREDITS_USED,
        WMH.CREDITS_USED_COMPUTE,
        WMH.CREDITS_USED_CLOUD_SERVICES,
        -- Q.CREDITS_USED CREDITS_USED_QAS
    FROM
        SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY WMH
        -- LEFT JOIN SNOWFLAKE.ORGANIZATION_USAGE.QUERY_ACCELERATION_HISTORY Q ON WMH.START_TIME = Q.START_TIME --NOTE: QAS DATA IS AT DAILY GRAIN, SO REMOVED FOR NOW.
        --     AND WMH.WAREHOUSE_ID = Q.WAREHOUSE_ID
        --     AND Q.ACCOUNT_LOCATOR = WMH.ACCOUNT_LOCATOR
    WHERE
        WMH.START_TIME >= :START_TIME_RANGE
        AND WMH.START_TIME < :END_TIME_RANGE
        AND WMH.WAREHOUSE_NAME IS NOT NULL --Exclude cloud services WH entries
   )
   , QUERIES_BY_TIME_SLICE AS (
    SELECT
        QH.ACCOUNT_NAME,
        QH.ACCOUNT_LOCATOR,
        QH.REGION,
        QH.QUERY_ID,
        QH.QUERY_TYPE,
        QH.QUERY_TEXT,
        QH.QUERY_TAG,
        QH.SQL_HASH,
        QH.DATABASE_ID,
        QH.DATABASE_NAME,
        QH.SCHEMA_NAME,
        QH.SESSION_ID,
        QH.USER_NAME,
        QH.ROLE_NAME,
        QH.WAREHOUSE_NAME,
        QH.WAREHOUSE_ID,
        QH.WAREHOUSE_SIZE,
        QH.WAREHOUSE_TYPE,
        QH.WAREHOUSE_WEIGHT,
        QH.QUERY_LOAD_PERCENT,
        QH.CLUSTER_NUMBER,
        QH.QAS_BYTES_USED,
        QH.START_TIME,
        QH.END_TIME,
        QH.TOTAL_ELAPSED_TIME,
        QH.TOTAL_QUEUE_TIME,
        QH.TRANSACTION_BLOCKED_TIME,
        QH.ADJUSTED_START_TIME,
        DD.START_SLICE,     
        DATEDIFF(
            'MILLISECOND',
            GREATEST(DD.START_SLICE, QH.ADJUSTED_START_TIME),
            LEAST(DD.END_SLICE, QH.END_TIME)
        ) AS DERIVED_ELAPSED_TIME_MS,
        --Add WH weight to help with same hour size changes
        RATIO_TO_REPORT(DERIVED_ELAPSED_TIME_MS::NUMBER(38, 0)*QH.WAREHOUSE_WEIGHT) OVER (PARTITION BY QH.ACCOUNT_LOCATOR, QH.REGION, QH.WAREHOUSE_ID, DD.START_SLICE) AS ELAPSED_TIME_RATIO,
		RATIO_TO_REPORT(QAS_BYTES_USED) OVER (PARTITION BY QH.ACCOUNT_LOCATOR, QH.REGION, QH.WAREHOUSE_ID, DD.START_SLICE) AS QAS_RATIO
    FROM
        TEMP_SF_QUERY_HISTORY QH
    JOIN TEMP_TIME_SLICE_BY_INTERVAL DD              
        ON QH.ADJUSTED_START_TIME < DD.END_SLICE
            AND DD.START_SLICE < QH.END_TIME
    WHERE
        DD.START_SLICE >= :START_TIME_RANGE
        AND DD.START_SLICE < :END_TIME_RANGE
)
SELECT
    WMH.ACCOUNT_NAME,
    WMH.ACCOUNT_LOCATOR,
    WMH.REGION,
    Q.QUERY_ID,
    COALESCE(Q.START_SLICE,WMH.START_TIME) AS START_SLICE, 
    Q.ADJUSTED_START_TIME,
    Q.START_TIME,
    Q.END_TIME,
    Q.QUERY_TYPE,
    Q.QUERY_TEXT,
    Q.QUERY_TAG,
    Q.SQL_HASH,
    Q.DATABASE_ID,
    Q.DATABASE_NAME,
    Q.SCHEMA_NAME,
    Q.SESSION_ID,
    Q.USER_NAME,
    Q.ROLE_NAME,
    COALESCE(Q.WAREHOUSE_NAME, WMH.WAREHOUSE_NAME) WAREHOUSE_NAME,
    COALESCE(Q.WAREHOUSE_ID, WMH.WAREHOUSE_ID) WAREHOUSE_ID,
    Q.WAREHOUSE_SIZE,
    Q.WAREHOUSE_TYPE,
    Q.WAREHOUSE_WEIGHT,
    Q.QUERY_LOAD_PERCENT,
    Q.CLUSTER_NUMBER,
    Q.QAS_BYTES_USED,
    Q.TOTAL_ELAPSED_TIME,
    Q.TOTAL_QUEUE_TIME,
    Q.TRANSACTION_BLOCKED_TIME,
    Q.DERIVED_ELAPSED_TIME_MS,
    Q.ELAPSED_TIME_RATIO,
    NVL(ELAPSED_TIME_RATIO, 1) * WMH.CREDITS_USED AS ALLOCATED_CREDITS_USED,
    NVL(ELAPSED_TIME_RATIO, 1) * WMH.CREDITS_USED_COMPUTE AS ALLOCATED_CREDITS_USED_COMPUTE,
    NVL(ELAPSED_TIME_RATIO, 1) * WMH.CREDITS_USED_CLOUD_SERVICES AS ALLOCATED_CREDITS_USED_CLOUD_SERVICES,
    -- ZEROIFNULL(QAS_RATIO * WMH.CREDITS_USED_QAS) AS ALLOCATED_CREDITS_USED_QAS
FROM
    SF_WAREHOUSE_METERING_HISTORY WMH
LEFT JOIN QUERIES_BY_TIME_SLICE Q ON WMH.WAREHOUSE_ID = Q.WAREHOUSE_ID 
    AND WMH.ACCOUNT_LOCATOR = Q.ACCOUNT_LOCATOR 
    AND WMH.REGION = Q.REGION
    AND Q.START_SLICE >= WMH.START_TIME 
    AND Q.START_SLICE < DATEADD('HOUR',1,WMH.START_TIME)
ORDER BY WMH.START_TIME
;

--UPDATE CONTROL TABLE TO VALUE TO START FROM ON NEXT ITERATION/CALL
UPDATE SF_QUERY_COST_PARAMETERS 
SET PARAM_VALUE_TIMESTAMP =:END_TIME_RANGE::TIMESTAMP_LTZ
WHERE PARAMETER_NAME = 'org_query_start_time';

--All done and successful if here
RETURN ('Hours Processed:'||rowcount|| ' start_time:'||start_time_range||' end_time:'||end_time_range);

--HANDLE ERRORS
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                            'SQLCODE', SQLCODE,
                            'SQLERRM' , SQLERRM,
                            'SQLSTATE', SQLSTATE);
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('ERROR TYPE', 'OTHER ERROR',
                            'SQLCODE', SQLCODE,
                            'SQLERRM', SQLERRM,
                            'SQLSTATE', SQLSTATE);

END --END OF PROC
$$
; 