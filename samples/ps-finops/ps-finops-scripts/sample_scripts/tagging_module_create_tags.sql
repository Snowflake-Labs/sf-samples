/*
Detailed cost attribution in FinOps requires the use of object and query tagging. 

Objects we consume for tagging:
 - Query_tag - if you can get your application (dbt, Tableau, Streamlit, etc) to provide cost_center tag and value, this is best solution.
 - User tag - Tag service accounts with cost_center tag. Tag human user's if they use cortex analyst or they are Snowflake admin's and you want one tag for all they do. If not, just leave untagged.
 - Database Schema tag - Schema's inherit Database tags, so tag all databases and override schemas with different tag if different attribution. It is assumed that a Schema is attributable to one cost center, if not then not currently supported. 
 - Role tag - Tag Functional Roles if attributable to one cost center. If not one cost center, rely on other tags. This assumes Access Roles are not given to User's, hence skipping tagging them. If you do do this, rely on user tag or just tag the Access Role until you can parent it to a Functional Role.
 - Warehouse tag - Tag a warehouse is last as to reduce credit consumption, as shared warehouses are paramount in cost reduction. Tag if one cost center can be attributed, else don't tag.

Note on Team vs Cost Center vs Cost_Center verbiage: Most HR systems think in terms of Teams, but accountants and finance think in terms of Cost Centers. Cost Center is the more generic term that could include Project names, or Business Units. Cost_Center is the codified name we typically use.

For Query and Warehouse attribution costs, a helper function was created such that all queries can use the same logic and one update changes them all.
The function is GET_TAG_COST_CENTER and the priority defaults to this - of which you many want to customize to your specific needs:

CREATE OR REPLACE FUNCTION SNOWFLAKE_FINOPS.FINOPS_ACCOUNT.GET_TAG_COST_CENTER("QUERY_TAG" VARCHAR, "USER_NAME" VARCHAR, "ROLE_NAME" VARCHAR, "SCHEMA_TAG_VALUE" VARCHAR, "ROLE_TAG_VALUE" VARCHAR, "USER_TAG_VALUE" VARCHAR, "WAREHOUSE_TAG_VALUE" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS '
SELECT coalesce(
    IFF(QUERY_TAG <> '''',try_parse_json(QUERY_TAG):"COST_CENTER"::varchar, NULL)
    , user_tag_value -- User/service account tagged when they can always be attributed to a cost center.
    , schema_tag_value -- Schema tag used for Snowflake Tasks with Warehouse use.
    , role_tag_value -- Role use of query.
    , warehouse_tag_value -- Fallback if the above are not available.
    , role_name || '' - '' || user_name -- Fallback if all tags missing
    , ''Unknown'' -- Last resort value.
    ) as TEAM
';

show tags in account;

If using Snowsight Dashboards, setup a custom cost center Snowsight managed filter and optionally delegate others to create them: https://docs.snowflake.com/en/user-guide/ui-snowsight-filters#grant-permission-to-create-custom-filters

Use Display of `Cost Center (SF)` and name of costcenter. It is case sensitive in Snowsight and have a daily update of this code to populate the costcenter tag data:
Include the ALL option - Any Value type.
Enable multiple values to be selected.

--Snowsight Managed Filter costcenter code:
SELECT DISTINCT TAG_VALUE
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TS
WHERE ts.TAG_NAME = 'COST_CENTER'
AND TS.DOMAIN IN ('DATABASE','SCHEMA','ROLE','USER','REPLICATION GROUP','COMPUTE POOL')
UNION ALL
SELECT 'Unknown'
ORDER BY 1;

-- Example use:
SELECT * FROM COMPUTE_AND_QAS_CC_WH_CURRENCY_DAY
WHERE DAY = :daterange
   AND COST_CENTER = :costcenter
;

--Some example tag application sql:
USE ROLE SYSADMIN;
ALTER DATABASE SNOWFLAKE_FINOPS                 SET TAG SNOWFLAKE_FINOPS.TAG.COST_CENTER = 'Snowflake Admin';
--All schemas will inherit this tag.

----------------
--General Notes:
--Snowflake APPLICATIONS are not tagged in this solution. They are not part of the cost attribution model yet.

--In this solution, we use telemetry delayed data for performance & cost reasons. If you need immediate results, you can use the SYSTEM$GET_TAG function to see the immediate results.:
SELECT SYSTEM$GET_TAG('SNOWFLAKE_FINOPS.TAG.COST_CENTER', 'SNOWFLAKE_FINOPS', 'DATABASE');

SELECT * FROM TABLE(SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES_WITH_LINEAGE('SNOWFLAKE_FINOPS.TAG.COST_CENTER')) WHERE DOMAIN = 'DATABASE';

--Show tags in account that you have access to:
show tags in account;

-- shows all tags
select * from snowflake.account_usage.tags 
where deleted is null
;

select * from snowflake.account_usage.tag_references 
where tag_name = 'COST_CENTER'
;
*/

/****   Tag setup     ****/
-- set up tag_admin access role - we assume SYSADMIN is the main admin role, change as needed.
SET finops_tag_admin_role = 'FINOPS_TAG_ADMIN'; -- TO SET - Tag delegated admin role. Will create if not exists.
SET tag_db = 'FINOPS'; -- TO SET - Tags are objects that need a schema to live in. Change as desired.
SET tag_schema = 'TAG'; -- TO SET - Tags are objects that need a schema to live in. Change as desired.
SET tag_db_tag_schema = $tag_db||'.'||$tag_schema;
SET admin_wh = 'ADMIN_XSMALL'; -- TO SET - warehouse name for creating and applying tags

use role identifier($finops_tag_admin_role); -- TO SET - Need a role with access to Snowflake DB (we do recommend using built-in database roles for privileges to Snowflake DB). Can use finops_tag_admin_role if it was created.

-- Use Tags schema to store tables used for tagging. Update code as necessary if this is not desired.
USE SCHEMA IDENTIFIER($tag_db_tag_schema);

--------------- Create a User To Team Mapping View/Table -------------
-- Get a list of users and their department/team from HR system or other source like below Workday example to help you map users to teams/cost centers.
-- Assumptions:
-- You are good with sql and can code sql alternatives to our examples as needed.
-- Below code assumes human users are owned by 'RSA_IMG_SCIM_PROVISIONER' and service accounts are owned by 'SERVICE_ACCOUNT_PROVISIONER'. Adjust as needed.
-- Functional Roles are named with a <Role Name> naming convention and Access Roles are _<Role_name> to differentiate them. Adjust as needed.

/*
-- Create a base view or import table from HR on user and team information, like this Workday example that joins Snowflake user data:

-- Example of a view that joins Snowflake user data with Workday team data:
DESC VIEW USER_TEAM_MAPPING; 

SF_NAME VARCHAR(255)
SF_LOGIN_NAME   VARCHAR(255)
EMAIL   VARCHAR(255)
SF_DISABLED VARIANT
USER_ID VARCHAR(150)
WKD_ID  VARCHAR(20)
WKD_STATUS  VARCHAR(50)
WKD_ACTIVE  BOOLEAN
BUSINESS_TITLE  VARCHAR(128)
POSITION_TITLE  VARCHAR(128)
TEAM_NAME   VARCHAR(255)
SUPERIOR_TEAM_NAME  VARCHAR(50)


CREATE OR REPLACE VIEW USER_TEAM_MAPPING(
    SF_NAME,
    SF_LOGIN_NAME,
    EMAIL,
    SF_DISABLED,
    USER_ID,
    WKD_ID,
    WKD_STATUS,
    WKD_ACTIVE,
    BUSINESS_TITLE,
    POSITION_TITLE,
    TEAM_NAME,
    SUPERIOR_TEAM_NAME
) as (
    WITH TEAM AS (
        SELECT
            WKD_ID AS TEAM_WKD_ID
            ,REPLACE(TEAM_NAME, ' (Sub Team)','') as TEAM_NAME
            ,TEAM_CODE
            --,WKD_LAST_UPDATED_DT AS TEAM_WKD_LAST_UPDATED_DT
            --,WKD_ACTIVE_FLG AS TEAM_WKD_ACTIVE_FLG
            ,SUPERIOR_TEAM_ID
            ,SUPERIOR_TEAM_NAME
        FROM HR.HR_WORKDAY.WORKDAY_TEAM_ORGANIZATION_D_VW
        WHERE WKD_ACTIVE_FLG = TRUE
    )
    ,SNOWFLAKE_USERS AS (
        SELECT
            NAME AS SF_NAME
            ,UPPER(LOGIN_NAME) AS SF_LOGIN_NAME
            --,UPPER(DISPLAY_NAME) AS SF_DISPLAY_NAME
            ,UPPER(EMAIL) AS EMAIL
            ,DISABLED AS SF_DISABLED
        FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
        WHERE DELETED_ON IS NULL
--        AND DISABLED = FALSE
        AND OWNER = 'RSA_IMG_SCIM_PROVISIONER'
    )
    ,EMP AS (
        SELECT
             UPPER(USER_ID) AS USER_ID
            ,UPPER(WORK_EMAIL_ADDRESS) AS WORK_EMAIL_ADDRESS
            ,WKD_ID
            ,SUB_TEAM_ID
            ,STATUS AS WKD_STATUS
            ,ACTIVE_FLG AS WKD_ACTIVE
            ,BUSINESS_TITLE
            ,POSITION_TITLE
            --,WKD_LAST_UPDATED_DT
            --,CONTRACT_END_DATE
        FROM HR.HR_WORKDAY.WORKDAY_EMPLOYEES_F_VW
        WHERE WKD_LAST_UPDATED_DT IS NOT NULL
        AND STATUS IN ('ACTIVE', 'ON LEAVE')
        QUALIFY RANK() OVER(PARTITION BY WORK_EMAIL_ADDRESS ORDER BY WKD_LAST_UPDATED_DT DESC, WKD_ID ASC) = 1
    )
    SELECT
        * EXCLUDE (WORK_EMAIL_ADDRESS, TEAM_WKD_ID, TEAM_CODE, SUPERIOR_TEAM_ID, SUB_TEAM_ID)
    FROM SNOWFLAKE_USERS S
    LEFT JOIN EMP E
        ON S.EMAIL = E.WORK_EMAIL_ADDRESS
    LEFT JOIN TEAM T
        ON E.SUB_TEAM_ID = T.TEAM_WKD_ID
)
;
*/


--Assumming workday table doesn't exist, we just need simple table of user's email to team they are on. Populate by any means.
USE SCHEMA FINOPS_ACCOUNT;
CREATE TRANSIENT TABLE USER_TEAM_MAPPING (EMAIL VARCHAR(300), TEAM_NAME VARCHAR(300));


select * from snowflake.account_usage.users
where deleted_on is null
;

--If Workday isn't used, just need a mapping table to map the user to snowflake.account_usage.users table to get their team name to use as cost center value.

select $tag_db_tag_schema;

--Export all your service accounts and manually tag them:
SELECT
    u.NAME AS SF_USER_NAME
    ,UPPER(u.LOGIN_NAME) AS SF_LOGIN_NAME
    ,UPPER(u.DISPLAY_NAME) AS SF_DISPLAY_NAME
    ,UPPER(u.EMAIL) AS SF_EMAIL
    ,u.DEFAULT_ROLE
    -- ,CASE WHEN u.OWNER like '%SCIM_PROVISIONER' THEN 0 ELSE 1 END AS IS_SERVICE_ACCOUNT
    ,CASE WHEN u.name  like '%@%' THEN 0 ELSE 1 END AS IS_SERVICE_ACCOUNT
    ,u.DISABLED
    -- ,'ALTER USER '||u.name||' SET TAG '||$tag_db_tag_schema||'.COST_CENTER = ''REPLACE_ME'';' as "Code to Run"
    --If you have a nice naming convention to start with for good candidate team names:
    ,'ALTER USER '||u.name||' SET TAG '||$tag_db_tag_schema||'.COST_CENTER = '''||split_part(sf_user_name, '_', 1) as Code_part_1
    , split_part(sf_user_name, '_', 2) pos2 
    , split_part(sf_user_name, '_', 3) pos3 
    , ''';' as code_part2
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS u
WHERE TRUE
AND u.DELETED_ON IS NULL
-- AND u.DISABLED = FALSE
AND u.name not like '%@%' --This should be all your service accounts, adjust as needed.
-- AND u.OWNER like '%SCIM_PROVISIONER' --Alternative: This should be all your service accounts, adjust as needed.
;


--Look at all the Team's a Role's user members have given convenient count (TEAM_COUNT) and chose a winner to tag role to.
--Assumes you have populated the USER_TEAM_MAPPING table.
SELECT GU.ROLE, U.NAME, U.EMAIL, TM.TEAM_NAME
    , COUNT(TM.TEAM_NAME) OVER (PARTITION BY GU.ROLE, TM.TEAM_NAME ORDER BY NULL) AS TEAM_COUNT
    , 'ALTER ROLE '||GU.ROLE||' SET TAG '||$tag_db_tag_schema||' = '''||coalesce(TEAM_NAME,'UNKNOWN')||''';' as code
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS GU
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS U ON U.NAME = GU.GRANTEE_NAME
-- LEFT JOIN USER_TEAM_MAPPING TM ON TM.SF_NAME = U.NAME --workday table
LEFT JOIN USER_TEAM_MAPPING TM ON TM.EMAIL = U.EMAIL
WHERE TRUE
AND GU.DELETED_ON IS NULL
AND U.DELETED_ON IS NULL
-- AND U.OWNER like '%SCIM_PROVISIONER' --If you just want SCIM onboarded users
AND GU.ROLE NOT IN ('ACCOUNTADMIN', 'SYSADMIN','SECURITYADMIN','USERADMIN', 'ORGADMIN') --Exclude as considered snowflake admin.
-- AND TM.SF_DISABLED = FALSE -- Add if you want to exclude disabled users.
AND U.NAME NOT IN ('WORKSHEETS4_RL', 'SNOWFLAKE') --SNOWFLAKE SAAS USER'S YOU CAN IGNORE.
AND GU.ROLE NOT IN (SELECT OBJECT_NAME
                    FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
                    WHERE TRUE
                    AND TAG_NAME = 'COST_CENTER'
                    AND DOMAIN = 'ROLE')
ORDER BY GU.ROLE, TEAM_COUNT DESC
;


--Find missing roles without a tag that are not access roles.
--WARNING: Tag tables are 1 hour delayed, so may not show recent tag work.
SELECT DISTINCT R.NAME, GU.ROLE, U.NAME, U.EMAIL, U.DISABLED, GU.DELETED_ON, TM.TEAM_NAME
    , COUNT(TM.TEAM_NAME) OVER (PARTITION BY GU.ROLE, TM.TEAM_NAME ORDER BY NULL) AS TEAM_COUNT
    , 'ALTER ROLE '||GU.ROLE||' SET TAG '||$tag_db_tag_schema||' = '''|| COALESCE(TEAM_NAME, 'PLACEHOLDER_ROLE')||''';'
FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES R
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS GU ON GU.ROLE = R.NAME AND GU.DELETED_ON IS NULL
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS U ON U.NAME = GU.GRANTEE_NAME AND U.DELETED_ON IS NULL AND U.DISABLED = FALSE
LEFT JOIN USER_TEAM_MAPPING TM ON TM.SF_NAME = U.NAME AND TM.SF_DISABLED = FALSE
    AND TM.TEAM_NAME IS NOT NULL --DEAD USERS, BUT NOT YET DEAD IN SNOWFLAKE!
WHERE TRUE
AND GU.ROLE NOT IN ('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN', 'USERADMIN', 'ORGADMIN', 'WORKSHEETS4_RL')
--AND TM.SF_NAME IS NULL --Find missing team mapping users.
-- AND R.NAME NOT LIKE '\\_%' ESCAPE '\\' -- If missing the first _ in the name, ignore. Add exclusions as needed.
AND R.DELETED_ON IS NULL
AND NOT EXISTS (SELECT *
    FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES R
    WHERE true
    AND R.OBJECT_NAME = R.NAME
    AND TAG_NAME = 'COST_CENTER'
    AND DOMAIN = 'ROLE'
)
ORDER BY GU.ROLE, TEAM_COUNT DESC
;

------------ Database Tag Setup ----------------
--Only tag databases that should have a fall back to a cost center if schema is not tagged yet.
SELECT Database_name, 'ALTER DATABASE '||Database_name||' SET TAG '||$tag_db_tag_schema ||'.COST_CENTER = '''||REPLACE(REPLACE(REPLACE(Database_name,'DEV',''),'PRD',''),'QA','') ||''';' as "--code to modify"
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES d
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr on TAG_NAME = 'COST_CENTER' AND DOMAIN = 'DATABASE' AND tr.object_name = D.DATABASE_NAME
WHERE DELETED IS NULL
AND d.TYPE in ('STANDARD')
and tr.object_name is null
and d.database_name not in('USERDB', 'WORKSHEETS4')
;
--For Schema tagging, use the missing tag script below to find missing tags and apply them to the schema.


--Will have delayed data in account usage...remember
CREATE OR REPLACE TEMPORARY TABLE ROLE_COST_CENTER AS
SELECT OBJECT_NAME AS ROLE_NAME, TAG_VALUE AS COST_CENTER
FROM TABLE(SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES_WITH_LINEAGE($tag_db_tag_schema))
WHERE TRUE
AND LEVEL = 'ROLE'
AND TAG_NAME = 'COST_CENTER'
;

--Code for granting COST_CENTER tag to databases and schema. THEN APPLY IT TO APPROPRIATE GROUP AS A MANUAL EFFORT EVERY TIME new stuff comes onboard.
SELECT
S.CATALOG_NAME
, S.SCHEMA_NAME
, S.IS_MANAGED_ACCESS --Hopefully every schema is true here
--, S.IS_TRANSIENT
, GR.PRIVILEGE
, GR.GRANTEE_NAME
, RCC.COST_CENTER as Role_Cost_center
, COUNT( GR.GRANTEE_NAME) OVER (PARTITION BY CATALOG_NAME, GRANTEE_NAME) AS GRANTEE_COUNT_DB
, COUNT( GR.GRANTEE_NAME) OVER (PARTITION BY CATALOG_NAME, SCHEMA_NAME, GRANTEE_NAME) AS GRANTEE_COUNT_SCHEMA
, TAGR.TAG_VALUE AS SCHEMA_EXISTING_TAG
, TAGRDB.TAG_VALUE AS DB_TAG
, IFF(TAGR.TAG_VALUE IS NULL, 'ALTER SCHEMA '||S.CATALOG_NAME||'.'||S.SCHEMA_NAME||' SET TAG '||$tag_db_tag_schema||' = '''|| COALESCE(RCC.COST_CENTER, tagrdb.tag_value, 'PLACEHOLDER_ROLE')||''';','') AS SCHEMA_TAG
FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA S
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES GR ON S.CATALOG_NAME = GR.TABLE_CATALOG AND S.SCHEMA_NAME = GR.NAME
    --Ignore Access Roles:
    --AND GR.GRANTEE_NAME NOT LIKE '\\_%' ESCAPE '\\' -- If Access roles ar _Name then uncomment, else do something like:
    AND GR.GRANTEE_NAME NOT LIKE '%RO' 
    AND GR.GRANTEE_NAME NOT LIKE '%RW'
    --Ignore purged items:
    AND GR.DELETED_ON IS NULL
    AND GR.GRANTED_ON = 'SCHEMA'
    AND GR.PRIVILEGE = 'USAGE'
    AND GR.GRANTEE_NAME NOT IN ('SECURITYADMIN','SYSADMIN','ACCOUNTADMIN','SERVICE_ACCOUNT_PROVISIONER')
LEFT JOIN ROLE_COST_CENTER RCC ON RCC.ROLE_NAME = GR.GRANTEE_NAME
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TAGR ON S.CATALOG_NAME = TAGR.OBJECT_DATABASE AND S.SCHEMA_NAME = TAGR.OBJECT_NAME AND TAGR.TAG_NAME = 'COST_CENTER' AND TAGR.DOMAIN = 'SCHEMA'
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TAGRDB ON S.CATALOG_NAME = TAGRDB.OBJECT_NAME AND TAGRDB.TAG_NAME = 'COST_CENTER' AND TAGRDB.DOMAIN = 'DATABASE'
WHERE TRUE
--Missing Tagged Schemas:
AND (TAGR.OBJECT_NAME IS NULL OR tagr.tag_value <> RCC.COST_CENTER)
AND S.DELETED IS NULL
--AND S.IS_TRANSIENT <> 'NO'
-- AND S.SCHEMA_NAME NOT IN ('PUBLIC') -- Optionally tag if used, but you shouldn't use.
AND NOT (S.SCHEMA_NAME = 'LOCAL' AND S.CATALOG_NAME = 'SNOWFLAKE')
AND S.CATALOG_NAME NOT IN ('SNOWFLAKE', 'WORKSHEETS4')
AND S.CATALOG_NAME NOT IN (
                SELECT DATABASE_NAME from SNOWFLAKE.ACCOUNT_USAGE.DATABASES
                WHERE DELETED IS NULL
                AND TYPE = 'IMPORTED DATABASE') -- Can't tag read-only databases.
AND NOT EXISTS (SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr1_schema_gr
                WHERE tr1_schema_gr.TAG_NAME = 'COST_CENTER' AND tr1_schema_gr.DOMAIN = 'SCHEMA'
                AND tr1_schema_gr.object_name = s.schema_name AND tr1_schema_gr.object_database = s.catalog_name
                AND object_deleted IS NULL)
ORDER BY 1,2
;


/*
--------   simple taggable objects list     ----------
-- Dump this list to a Google Sheet for manual tagging if above is too complex.

--Get current list of warehouses to be used in script below.
show warehouses;

--Make sure to run the above show warehouse command to get the list of warehouses to be used in the script below.
SELECT CURRENT_ACCOUNT_ALIAS(),CURRENT_ACCOUNT()
    , 'USER' AS OBJECT_TYPE
    , NAME AS OBJECT_NAME
    , '' AS COST_CENTER
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE DISABLED = FALSE
AND DELETED_ON IS NULL
and owner <> 'OKTA_PROVISIONER' --TO SET - Whatever SQL/Owner is for human users, do the opposite for service accounts to get all non humans. This could be redundant to filters below.
and (name in ('USER1','USER2') --Add users that should be overridden to a cost center 100% of the time, like Snowflake Admins.
    or name like '%SVC%') --Add a pattern to match service accounts.
and name NOT IN ('SNOWFLAKE') --TO SET - Exclude Snowflake's service account and any other service accounts that should not be tagged that are shared.
UNION ALL
SELECT CURRENT_ACCOUNT_ALIAS(),CURRENT_ACCOUNT()
    , 'DATABASE_ROLE' AS OBJECT_TYPE
    , ROLE_DATABASE_NAME || '.' || NAME
    , '' AS COST_CENTER
FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
WHERE DELETED_ON IS NULL
AND ROLE_TYPE = 'DATABASE_ROLE'

UNION ALL
SELECT CURRENT_ACCOUNT_ALIAS(),CURRENT_ACCOUNT()
    , 'ROLE' AS OBJECT_TYPE
    , NAME
    , '' AS COST_CENTER
FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
WHERE DELETED_ON IS NULL
AND ROLE_TYPE = 'ROLE'

UNION ALL
SELECT CURRENT_ACCOUNT_ALIAS(),CURRENT_ACCOUNT()
    , 'WAREHOUSE' AS OBJECT_TYPE
    , "name" AS OBJECT_NAME
    , '' AS COST_CENTER
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" NOT LIKE 'SYSTEM%'

UNION ALL
SELECT DISTINCT CURRENT_ACCOUNT_ALIAS(),CURRENT_ACCOUNT()
    , 'REPLICATION_GROUPS' AS OBJECT_TYPE
    ,  REPLICATION_GROUP_NAME AS OBJECT_NAME
    , '' AS COST_CENTER
FROM   SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY UH
WHERE START_TIME >= CURRENT_DATE()-3

UNION ALL
SELECT DISTINCT CURRENT_ACCOUNT_ALIAS(),CURRENT_ACCOUNT()
    , 'COMPUTE_POOLS' AS OBJECT_TYPE
    ,  COMPUTE_POOL_NAME AS OBJECT_NAME
    , '' AS COST_CENTER
FROM   SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY SH
WHERE START_TIME >= CURRENT_DATE()-3
ORDER BY OBJECT_TYPE, OBJECT_NAME
;
*/

