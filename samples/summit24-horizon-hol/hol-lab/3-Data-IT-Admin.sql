
/***************************************************************************************************
| H | O | R | I | Z | O | N |   | L | A | B | S | 

Demo:         Horizon Lab (Data / IT Admin Persona)
Version:      HLab v1
Create Date:  Apr 17, 2024
Author:       Ravi Kumar
Reviewers:    Ben Weiss, Susan Devitt
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************

****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
Apr 17, 2024        Ravi Kumar           Initial Lab
***************************************************************************************************/
/*************************************************/
/*************************************************/
/* D A T A      I T   A D M I N      R O L E */
/*************************************************/
/*************************************************/



/*----------------------------------------------------------------------------------
Step - Access History (Read and Writes)

 Access History provides insights into user queries encompassing what data was 
 read and when, as well as what statements have performed a write operations.

 Access History is particularly important for Compliance, Auditing,
 and Governance.

 Within this step, we will walk through leveraging Access History to find when the
 last time our Raw data was read from and written to.

 Note: Access History latency is up to 3 hours.  So, some of the queries below may not have results.
---------------------------------------------------------------------------------*/

 USE ROLE HRZN_IT_ADMIN;
 USE DATABASE HRZN_DB;
 USE SCHEMA HRZN_SCH;


--> how many queries have accessed each of our Raw layer tables directly?
SELECT 
    value:"objectName"::STRING AS object_name,
    COUNT(DISTINCT query_id) AS number_of_queries
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => direct_objects_accessed)
WHERE object_name ILIKE 'HRZN%'
GROUP BY object_name
ORDER BY number_of_queries DESC;


--> what is the breakdown between Read and Write queries and when did they last occur?
SELECT 
    value:"objectName"::STRING AS object_name,
    CASE 
        WHEN object_modified_by_ddl IS NOT NULL THEN 'write'
        ELSE 'read'
    END AS query_type,
    COUNT(DISTINCT query_id) AS number_of_queries,
    MAX(query_start_time) AS last_query_start_time
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => direct_objects_accessed)
WHERE object_name ILIKE 'HRZN%'
GROUP BY object_name, query_type
ORDER BY object_name, number_of_queries DESC;

--> last few "read" queries
SELECT
    qh.user_name,    
    qh.query_text,
    value:objectName::string as "TABLE"
FROM snowflake.account_usage.query_history AS qh
JOIN snowflake.account_usage.access_history AS ah
ON qh.query_id = ah.query_id,
    LATERAL FLATTEN(input => ah.base_objects_accessed)
WHERE query_type = 'SELECT' AND
    value:objectName = 'HRZN_DB.HRZN_SCH.CUSTOMER' AND
    start_time > dateadd(day, -90, current_date());

--> last few "write" queries
SELECT
    qh.user_name,    
    qh.query_text,
    value:objectName::string as "TABLE"
FROM snowflake.account_usage.query_history AS qh
JOIN snowflake.account_usage.access_history AS ah
ON qh.query_id = ah.query_id,
    LATERAL FLATTEN(input => ah.base_objects_accessed)
WHERE query_type != 'SELECT' AND
    value:objectName = 'HRZN_DB.HRZN_SCH.CUSTOMER' AND
    start_time > dateadd(day, -90, current_date());


--> Find longest running queries

SELECT
query_text,
user_name,
role_name,
database_name,
warehouse_name,
warehouse_size,
execution_status,
round(total_elapsed_time/1000,3) elapsed_sec
FROM snowflake.account_usage.query_history
ORDER BY total_elapsed_time desc
LIMIT 10;

--> Find queries that have been executed against sensitive tables
SELECT
  q.USER_NAME,
  q.QUERY_TEXT,
  q.START_TIME,
  q.END_TIME
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
WHERE
  q.QUERY_TEXT ILIKE '%HRZN_DB.HRZN_SCH.CUSTOMER%'
ORDER BY
  q.START_TIME DESC;


--SHOW THE FLOW OF SENSITIVE DATA
SELECT
    *
FROM
(
    select
      directSources.value: "objectId"::varchar as source_object_id,
      directSources.value: "objectName"::varchar as source_object_name,
      directSources.value: "columnName"::varchar as source_column_name,
      'DIRECT' as source_column_type,
      om.value: "objectName"::varchar as target_object_name,
      columns_modified.value: "columnName"::varchar as target_column_name
    from
      (
        select
          *
        from
          snowflake.account_usage.access_history
      ) t,
      lateral flatten(input => t.OBJECTS_MODIFIED) om,
      lateral flatten(input => om.value: "columns", outer => true) columns_modified,
      lateral flatten(
        input => columns_modified.value: "directSources",
        outer => true
      ) directSources
    union
// 2
    select
      baseSources.value: "objectId" as source_object_id,
      baseSources.value: "objectName"::varchar as source_object_name,
      baseSources.value: "columnName"::varchar as source_column_name,
      'BASE' as source_column_type,
      om.value: "objectName"::varchar as target_object_name,
      columns_modified.value: "columnName"::varchar as target_column_name
    from
      (
        select
          *
        from
          snowflake.account_usage.access_history
      ) t,
      lateral flatten(input => t.OBJECTS_MODIFIED) om,
      lateral flatten(input => om.value: "columns", outer => true) columns_modified,
      lateral flatten(
        input => columns_modified.value: "baseSources",
        outer => true
      ) baseSources
) col_lin
   WHERE
       (SOURCE_OBJECT_NAME = 'HRZN_DB.HRZN_SCH.CUSTOMER' OR TARGET_OBJECT_NAME='HRZN_DB.HRZN_SCH.CUSTOMER')
    AND
        (SOURCE_COLUMN_NAME IN (
                SELECT
                    COLUMN_NAME
                FROM
                (
                    SELECT
                        *
                    FROM TABLE(
                      HRZN_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                        'HRZN_DB.HRZN_SCH.CUSTOMER',
                        'table'
                      )
                    )
                )
                WHERE TAG_NAME IN ('CONFIDENTIAL','PII_COL','PII_TYPE') 
            )
            OR
            TARGET_COLUMN_NAME IN (
                SELECT
                    COLUMN_NAME
                FROM
                (
                    SELECT
                        *
                    FROM TABLE(
                      HRZN_DB.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                        'HRZN_DB.HRZN_SCH.CUSTOMER',
                        'table'
                      )
                    )
                )
                WHERE TAG_NAME IN ('CONFIDENTIAL','PII_COL','PII_TYPE') --Enter the relevant tag(s) to check against.
            )
            );

    
--> how many queries have accessed each of our tables indirectly?
SELECT 
    base.value:"objectName"::STRING AS object_name,
    COUNT(DISTINCT query_id) AS number_of_queries
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => base_objects_accessed) base,
LATERAL FLATTEN (input => direct_objects_accessed) direct,
WHERE 1=1
    AND object_name ILIKE 'HRZN%'
    AND object_name <> direct.value:"objectName"::STRING -- base object is not direct object
GROUP BY object_name
ORDER BY number_of_queries DESC;



    
/**
  Direct Objects Accessed: Data objects directly named in the query explicitly.
  Base Objects Accessed: Base data objects required to execute a query.
**/ 
