/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Governance with Snowflake Horizon
Version:      v2
Script:       tb_fy25_governance_snowflake_horizon.sql         
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
 Governance with Snowflake Horizon
  Protect Your Data
    1 - System Defined Roles and Privileges
    2 - Role Based Access Control
    3 - Tag-Based Masking
    4 - Row-Access Policies
    5 - Aggregation Policies
    6 - Projection Policies

  Know Your Data
    7 – Sensitive Data Classification
    8 – Sensitive Custom Classification
    9 – Access History (Read and Writes)

 Discovery with Snowflake Horizon
    10 - Universal Search
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Before we begin, the Snowflake Access Control Framework is based on:
  • Role-based Access Control (RBAC): Access privileges are assigned to roles, which 
    are in turn assigned to users.
  • Discretionary Access Control (DAC): Each object has an owner, who can in turn 
    grant access to that object.

The key concepts to understanding access control in Snowflake are:
  • Securable Object: An entity to which access can be granted. Unless allowed by a 
    grant, access is denied. Securable Objects are owned by a Role (as opposed to a User)
      • Examples: Database, Schema, Table, View, Warehouse, Function, etc
  • Role: An entity to which privileges can be granted. Roles are in turn assigned 
    to users. Note that roles can also be assigned to other roles, creating a role 
    hierarchy.
  • Privilege: A defined level of access to an object. Multiple distinct privileges 
    may be used to control the granularity of access granted.
  • User: A user identity recognized by Snowflake, whether associated with a person 
    or program.

In Summary:
  • In Snowflake, a Role is a container for Privileges to a Securable Object.
  • Privileges can be granted Roles
  • Roles can be granted to Users
  • Roles can be granted to other Roles (which inherit that Roles Privileges)
  • When Users choose a Role, they inherit all the Privileges of the Roles in the 
    hierarchy.
----------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------
Step 1 - System Defined Roles and Privileges

 Before beginning to deploy Role Based Access Control (RBAC) for Tasty Bytes,
 let's first take a look at the Snowflake System Defined Roles and their privileges.
----------------------------------------------------------------------------------*/

-- let's start by assuming the Accountadmin role and our Snowflake Development Warehouse (synonymous with compute)
USE ROLE accountadmin;
USE WAREHOUSE tb_dev_wh;


-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit","name":"tb_zts,"version":{"major":1, "minor":1},"attributes":{"medium":"quickstart", "source":"tastybytes", "vignette": "governance_with_horizon"}}';


-- to follow best practices we will begin to investigate and deploy RBAC (Role-Based Access Control)
-- first, let's take a look at the Roles currently in our account
SHOW ROLES;


-- this next query, will turn the output of our last SHOW command and allow us to filter on the Snowflake System Roles that
-- are provided as default in all Snowflake Accounts
  --> Note: Depending on your permissions you may not see a result for every Role in the Where clause below.
SELECT
    "name",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('ORGADMIN','ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN','PUBLIC');

    /**
      Snowflake System Defined Role Definitions:
       1 - ORGADMIN: Role that manages operations at the organization level.
       2 - ACCOUNTADMIN: Role that encapsulates the SYSADMIN and SECURITYADMIN system-defined roles.
            It is the top-level role in the system and should be granted only to a limited/controlled number of users
            in your account.
       3 - SECURITYADMIN: Role that can manage any object grant globally, as well as create, monitor,
          and manage users and roles.
       4 - USERADMIN: Role that is dedicated to user and role management only.
       5 - SYSADMIN: Role that has privileges to create warehouses and databases in an account.
          If, as recommended, you create a role hierarchy that ultimately assigns all custom roles to the SYSADMIN role, this role also has
          the ability to grant privileges on warehouses, databases, and other objects to other roles.
       6 - PUBLIC: Pseudo-role that is automatically granted to every user and every role in your account. The PUBLIC role can own securable
          objects, just like any other role; however, the objects owned by the role are available to every other
          user and role in your account.

                                +---------------+
                                | ACCOUNTADMIN  |
                                +---------------+
                                  ^    ^     ^
                                  |    |     |
                    +-------------+-+  |    ++-------------+
                    | SECURITYADMIN |  |    |   SYSADMIN   |<------------+
                    +---------------+  |    +--------------+             |
                            ^          |     ^        ^                  |
                            |          |     |        |                  |
                    +-------+-------+  |     |  +-----+-------+  +-------+-----+
                    |   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
                    +---------------+  |     |  +-------------+  +-------------+
                            ^          |     |      ^              ^      ^
                            |          |     |      |              |      |
                            |          |     |      |              |    +-+-----------+
                            |          |     |      |              |    | CUSTOM ROLE |
                            |          |     |      |              |    +-------------+
                            |          |     |      |              |           ^
                            |          |     |      |              |           |
                            +----------+-----+---+--+--------------+-----------+
                                                 |
                                            +----+-----+
                                            |  PUBLIC  |
                                            +----------+
    **/

/*----------------------------------------------------------------------------------
Step 2 - Role Creation, GRANTS and SQL Variables

 Now that we understand System Defined Roles, let's begin leveraging them to create
 a Test Role and provide it access to the Customer Loyalty data we will deploy our
 initial Snowflake Horizon Governance features against.
----------------------------------------------------------------------------------*/

-- let's use the Useradmin Role to create a Test Role
USE ROLE useradmin;

CREATE OR REPLACE ROLE tb_test_role
    COMMENT = 'Test role for Tasty Bytes';


-- now we will switch to Securityadmin to handle our privilege GRANTS
USE ROLE securityadmin;


-- first we will grant ALL privileges on the Development Warehouse to our Sysadmin
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE sysadmin;


-- next we will grant only OPERATE and USAGE privileges to our Test Role
GRANT OPERATE, USAGE ON WAREHOUSE tb_dev_wh TO ROLE tb_test_role;

    /**
     Snowflake Warehouse Privilege Grants
      1 - MODIFY: Enables altering any properties of a warehouse, including changing its size.
      2 - MONITOR: Enables viewing current and past queries executed on a warehouse as well as usage
           statistics on that warehouse.
      3 - OPERATE: Enables changing the state of a warehouse (stop, start, suspend, resume). In addition,
           enables viewing current and past queries executed on a warehouse and aborting any executing queries.
      4 - USAGE: Enables using a virtual warehouse and, as a result, executing queries on the warehouse.
           If the warehouse is configured to auto-resume when a SQL statement is submitted to it, the warehouse
           resumes automatically and executes the statement.
      5 - ALL: Grants all privileges, except OWNERSHIP, on the warehouse.
    **/

-- now we will grant USAGE on our Database and all Schemas within it
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_test_role;

GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_test_role;

    /**
     Snowflake Database and Schema Grants
      1 - MODIFY: Enables altering any settings of a database.
      2 - MONITOR: Enables performing the DESCRIBE command on the database.
      3 - USAGE: Enables using a database, including returning the database details in the
           SHOW DATABASES command output. Additional privileges are required to view or take
           actions on objects in a database.
      4 - ALL: Grants all privileges, except OWNERSHIP, on a database.
    **/

-- we are going to test Data Governance features as our Test Role, so let's ensure it can run SELECT statements against our Data Model
GRANT SELECT ON ALL TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_test_role;

GRANT SELECT ON ALL TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_test_role;

GRANT SELECT ON ALL VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_test_role;

    /**
     Snowflake View and Table Privilege Grants
      1 - SELECT: Enables executing a SELECT statement on a table/view.
      2 - INSERT: Enables executing an INSERT command on a table. 
      3 - UPDATE: Enables executing an UPDATE command on a table.
      4 - TRUNCATE: Enables executing a TRUNCATE TABLE command on a table.
      5 - DELETE: Enables executing a DELETE command on a table.
    **/

-- before we proceed, let's SET a SQL Variable to equal our CURRENT_USER()
SET my_user_var  = CURRENT_USER();


-- now we can GRANT our Role to the User we are currently logged in as
GRANT ROLE tb_test_role TO USER identifier($my_user_var);


/*----------------------------------------------------------------------------------
Step 3 - Column-Level Security and Tagging = Tag-Based Masking

  The first Governance feature set we want to deploy and test will be Snowflake Tag
  Based Dynamic Data Masking. This will allow us to mask PII data in columns from
  our Test Role but not from more privileged Roles.
----------------------------------------------------------------------------------*/

-- we can now USE the Test Role,  Development Warehouse and Database
USE ROLE tb_test_role;
USE WAREHOUSE tb_dev_wh;
USE DATABASE tb_101;


-- to begin we will look at the Customer Loyalty table in the Raw layer
-- which contains raw data ingested from the Customer Loyalty program
SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.city,
    cl.country,
    cl.sign_up_date,
    cl.birthday_date
FROM raw_customer.customer_loyalty cl 
SAMPLE (1000 ROWS);


-- woah! there is a lot of PII we need to take care of before our users can touch this data.
-- luckily we can use Snowflakes native Tag-Based Masking to do just this

    /**
     A tag-based masking policy combines the object tagging and masking policy features
     to allow a masking policy to be set on a tag using an ALTER TAG command. When the data type in
     the masking policy signature and the data type of the column match, the tagged column is
     automatically protected by the conditions in the masking policy.
    **/

-- first let's create Tags and Governance Schemas to keep ourselves organized and follow best practices
USE ROLE accountadmin;


-- create a Tag Schema to contain our Object Tags
CREATE OR REPLACE SCHEMA tags
    COMMENT = 'Schema containing Object Tags';


-- we want everyone with access to this table to be able to view the tags 
GRANT USAGE ON SCHEMA tags TO ROLE public;


-- now we will create a Governance Schema to contain our Security Policies
CREATE OR REPLACE SCHEMA governance
    COMMENT = 'Schema containing Security Policies';

GRANT ALL ON SCHEMA governance TO ROLE sysadmin;


-- next we will create one Tag for PII that allows these values: NAME, PHONE_NUMBER, EMAIL, BIRTHDAY
-- not only will this prevent free text values, but will also add the selection menu to Snowsight
CREATE OR REPLACE TAG tags.tasty_pii
    ALLOWED_VALUES 'NAME', 'PHONE_NUMBER', 'EMAIL', 'BIRTHDAY'
    COMMENT = 'Tag for PII, allowed values are: NAME, PHONE_NUMBER, EMAIL, BIRTHDAY';


-- with the Tags created, let's assign them to the relevant columns in our Customer Loyalty table
ALTER TABLE raw_customer.customer_loyalty
    MODIFY COLUMN 
    first_name SET TAG tags.tasty_pii = 'NAME',
    last_name SET TAG tags.tasty_pii = 'NAME',
    phone_number SET TAG tags.tasty_pii = 'PHONE_NUMBER',
    e_mail SET TAG tags.tasty_pii = 'EMAIL',
    birthday_date SET TAG tags.tasty_pii = 'BIRTHDAY';


-- now we can use the TAG_REFERENCE_ALL_COLUMNS function to return the Tags associated with our Customer Loyalty table
SELECT
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value
FROM TABLE(information_schema.tag_references_all_columns
    ('tb_101.raw_customer.customer_loyalty','table'));

    /**
     With our Tags in place we can now create our Masking Policies that will mask data for all but privileged Roles.

     We need to create 1 policy for every data type where the return data type can be implicitly cast
     into the column datatype. We can only assign 1 policy per datatype to an individual Tag.
    **/

-- create our String Datatype Masking Policy
  --> Note: a Masking Policy is made of standard conditional logic, such as a CASE statement
CREATE OR REPLACE MASKING POLICY governance.tasty_pii_string_mask AS (val STRING) RETURNS STRING ->
    CASE
        -- these active roles have access to unmasked values 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
            THEN val 
        -- if a column is tagged with TASTY_PII : PHONE_NUMBER 
        -- then mask everything but the first 3 digits   
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'PHONE_NUMBER'
            THEN CONCAT(LEFT(val,3), '-***-****')
        -- if a column is tagged with TASTY_PII : EMAIL  
        -- then mask everything before the @ sign  
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'EMAIL'
            THEN CONCAT('**~MASKED~**','@', SPLIT_PART(val, '@', -1))
        -- all other conditions should be fully masked   
    ELSE '**~MASKED~**' 
END;

    /**
     The combination of an individuals City, first 3 Phone Number digits, and Birthday
     to re-identify them. Let's play it safe and also truncate Birthdays into 5 year buckets
     which will fit the use case of our Analyst
    **/

-- create our Date Masking Policy to return the modified Birthday
CREATE OR REPLACE MASKING POLICY governance.tasty_pii_date_mask AS (val DATE) RETURNS DATE ->
    CASE
        -- these active roles have access to unmasked values 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
            THEN val
        -- if a column is tagged with TASTY_PII : BIRTHDAY  
        -- then truncate to 5 year buckets 
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'BIRTHDAY'
            THEN DATE_FROM_PARTS(YEAR(val) - (YEAR(val) % 5),1,1)
        -- if a Date column is not tagged with BIRTHDAY, return NULL
    ELSE NULL 
END;


-- now we are able to use an ALTER TAG statement to set the Masking Policies on the PII tagged columns
ALTER TAG tags.tasty_pii SET
    MASKING POLICY governance.tasty_pii_string_mask,
    MASKING POLICY governance.tasty_pii_date_mask;


-- with Tag Based Masking in-place, let's give our work a test using our Test Role and Development Warehouse
USE ROLE tb_test_role;
USE WAREHOUSE tb_dev_wh;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    cl.birthday_date,
    cl.city,
    cl.country
FROM raw_customer.customer_loyalty cl
WHERE cl.country IN ('United States','Canada','Brazil');


-- the masking is working! let's also check the downstream Analytic layer View that leverages this table
SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd;


/*----------------------------------------------------------------------------------
Step 4 - Row-Access Policies

 Happy with our Tag Based Dynamic Masking controlling masking at the column level,
 we will now look to restrict access at the row level for our test role.

 Within our Customer Loyalty table, our role should only see Customers who are
 based in Tokyo.

 Thankfully, Snowflake Horizon has another powerful native Governance feature that can
 handle this at scale called Row Access Policies. For our use case, we will leverage
 the mapping table approach.
----------------------------------------------------------------------------------*/

 -- to start, our Accountadmin will create our mapping table including Role and City Permissions columns
 -- we will create this in the Governance Sschema, as we don't want this table to be visible to others.
USE ROLE accountadmin;

CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, city_permissions STRING);


-- with the table in place, we will now INSERT the relevant Role to City Permissions mapping to ensure
-- our Test only can see Tokyo customers
INSERT INTO governance.row_policy_map
    VALUES ('TB_TEST_ROLE','Tokyo'); 


-- now that we have our mapping table in place, let's create our Row Access Policy

    /**
     Snowflake supports row-level security through the use of Row Access Policies to
     determine which rows to return in the query result. The row access policy can be relatively
     simple to allow one particular role to view rows, or be more complex to include a mapping
     table in the policy definition to determine access to rows in the query result.
    **/

CREATE OR REPLACE ROW ACCESS POLICY governance.customer_city_row_policy
    AS (city STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN') -- list of roles that will not be subject to the policy
        OR EXISTS -- this clause references our mapping table from above to handle the row level filtering
            (
            SELECT rp.role
                FROM governance.row_policy_map rp
            WHERE 1=1
                AND rp.role = CURRENT_ROLE()
                AND rp.city_permissions = city
            )
COMMENT = 'Policy to limit rows returned based on mapping table of ROLE and CITY: governance.row_policy_map';


 -- let's now apply the Row Access Policy to our City column in the Customer Loyalty table
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_city_row_policy ON (city);


-- with the policy successfully applied, let's test it using the Test Role
USE ROLE tb_test_role;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM raw_customer.customer_loyalty cl SAMPLE (10000 ROWS)
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;


 -- as we did for our masking, let's double check our Row Level Security is flowing into downstream Analytic Views
SELECT
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;


/*----------------------------------------------------------------------------------
Step 5 - Aggregation Policies

 Outside of the Data Access Policies (Masking and Row Access) we have covered,
 Snowflake Horizon also provides Privacy Policies. In this section we will cover
 the ability to set Aggregation Policies on Database Objects which can restrict
 certain roles to only aggregate data by only allowing for queries that aggregate
 data into groups of a minimum size versus retrieving individual roles.

 For Tasty Bytes and the Test role we have created, let's test an Aggregation Policy
 out against our Raw Order Header table.
----------------------------------------------------------------------------------*/

    /**
     An Aggregation Policy is a schema-level object that controls what type of
     query can access data from a table or view. When an aggregation policy is applied to a table,
     queries against that table must aggregate data into groups of a minimum size in order to return results,
     thereby preventing a query from returning information from an individual record.
    **/

-- to begin, let's once again assume our Accountadmin role
USE ROLE accountadmin;


-- for our use case, we will create a Conditional Aggregation Policy in our Governance
-- Schema that will only allow queries from non-admin users to return results for queries 
-- that aggregate more than 1000 rows
CREATE OR REPLACE AGGREGATION POLICY governance.tasty_order_test_aggregation_policy
  AS () RETURNS AGGREGATION_CONSTRAINT ->
    CASE
      WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
      THEN NO_AGGREGATION_CONSTRAINT ()  
      ELSE AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 1000) -- atleast 1000 rows in aggregate
    END;


-- with the Aggregation Policy created, let's apply it to our Order Header table
ALTER TABLE raw_pos.order_header
    SET AGGREGATION POLICY governance.tasty_order_test_aggregation_policy;


-- now let's test our work by assuming our Test Role and executing a few queries
USE ROLE tb_test_role;


-- can we run a simple SELECT *?
SELECT TOP 10 * FROM raw_pos.order_header;


-- what if we include over 1000 rows?
SELECT TOP 5000 * FROM raw_pos.order_header;


    /**
     Bringing in the Customer Loyalty table that we have previously:
        1) Deployed Masking against PII columns
        2) Deployed Row Level Security to restrict our Test Role to only Tokyo results

     Let's answer a few aggregate business questions.
    **/

-- what are the total order amounts by gender?
SELECT 
    cl.gender,
    cl.city,
    COUNT(oh.order_id) AS count_order,
    SUM(oh.order_amount) AS order_total
FROM raw_pos.order_header oh
JOIN raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id
GROUP BY ALL
ORDER BY order_total DESC;

-- what are the total order amounts by postal code?
    --> Note: If the query returns a group that contains fewer records than the minimum group size of the policy,
    --> then Snowflake combines those groups into a remainder group.
SELECT 
    cl.postal_code,
    cl.city,
    COUNT(oh.order_id) AS count_order,
    SUM(oh.order_amount) AS order_total
FROM raw_pos.order_header oh
JOIN raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id
GROUP BY ALL
ORDER BY order_total DESC;


/*----------------------------------------------------------------------------------
Step 6 - Projection Policies

 Within this step, we will cover another Privacy Policy framework provided by Snowflake
 Horizon this time diving into Projection Policies which in short will prevent queries
 from using a SELECT statement to project values from a column.
----------------------------------------------------------------------------------*/

    /**
      A projection policy is a first-class, schema-level object that defines
      whether a column can be projected in the output of a SQL query result. A column
      with a projection policy assigned to it is said to be projection constrained.
    **/

-- assume our Accountadmin role
USE ROLE accountadmin;

-- for our use case, we will create a Conditional Projection Policy in our Governance Schema
-- that will only allow our Admin Roles to project the columns we will assign it to
CREATE OR REPLACE PROJECTION POLICY governance.tasty_customer_test_projection_policy
  AS () RETURNS PROJECTION_CONSTRAINT -> 
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
    THEN PROJECTION_CONSTRAINT(ALLOW => true)
    ELSE PROJECTION_CONSTRAINT(ALLOW => false)
  END;


-- with the Projection Policy in place, let's assign it to our Postal Code column
ALTER TABLE raw_customer.customer_loyalty
 MODIFY COLUMN postal_code
 SET PROJECTION POLICY governance.tasty_customer_test_projection_policy;


-- let's assume our Test Role and begin testing
USE ROLE tb_test_role;


-- what does a SELECT * against the table yield?
SELECT TOP 100 * FROM raw_customer.customer_loyalty;


-- what if we EXCLUDE the postal_code column?
SELECT TOP 100 * EXCLUDE postal_code FROM raw_customer.customer_loyalty;


/*----------------------------------------------------------------------------------
Step 7 - Sensitive Data Classification

 In some cases, you may not know if there is sensitive data in a table.
 Snowflake Horizon provides the capability to attempt to automatically detect
 sensitive information and apply relevant Snowflake system defined privacy tags. 

 Classification is a multi-step process that associates Snowflake-defined system
 tags to columns by analyzing the fields and metadata for personal data. Data 
 Classification can be done via SQL or the Snowsight interface.

 Within this step we will be using SQL to classify a single table as well as all
 tables within a schema.

 To learn how to complete Data Classification within the Snowsight interface,
 please see the following documentation: 

 Using Snowsight to classify tables in a schema 
  • https://docs.snowflake.com/en/user-guide/governance-classify-using#using-sf-web-interface-to-classify-tables-in-a-schema
----------------------------------------------------------------------------------*/

-- to begin we will assume our Accountadmin Role
USE ROLE accountadmin;


-- as our Raw Customer Schema only includes one table, let's use SYSTEM$CLASSIFY against it
CALL SYSTEM$CLASSIFY('raw_customer.customer_loyalty', {'auto_tag': true});


-- now let's view the new Tags Snowflake applied automatically via Data Classification
SELECT * FROM TABLE(information_schema.tag_references_all_columns('raw_customer.customer_loyalty','table'));


-- as our Raw Point-of-Sale Schema includes numerous tables, let's use SYSTEM$CLASSIFY_SCHEMA against it
CALL SYSTEM$CLASSIFY_SCHEMA('raw_pos', {'auto_tag': true});


-- once again, let's view the Tags applied using the Franchise table within the Schema
SELECT * FROM TABLE(information_schema.tag_references_all_columns('raw_pos.franchise','table'));


/*----------------------------------------------------------------------------------
Step 8 - Sensitive Custom Classification

 Snowflake provides the CUSTOM_CLASSIFIER class in the SNOWFLAKE.DATA_PRIVACY schema
 to enable Data Engineers to extend their Data Classification capabilities based
 on their own knowledge of their data.

 In this step, we will cover creating and deploying a Customer Classifier to identify
 Placekey location identifiers across our data.
---------------------------------------------------------------------------------*/

-- to begin, let's take a look at our Location table where we know Placekey is present
SELECT 
    TOP 10 *
FROM raw_pos.location
WHERE city = 'London';


-- let's now create a Classifier Schema

CREATE OR REPLACE SCHEMA classifiers
    COMMENT = 'Schema containing Custom Classifiers';


-- now we can create our Placekey Custom Classifier in the Classifier schema
CREATE OR REPLACE snowflake.data_privacy.custom_classifier classifiers.placekey();


-- next let's test the Regular Expression (Regex) that our Data Engineer has created to locate the Placekey value
SELECT 
    placekey
FROM raw_pos.location
WHERE placekey REGEXP('^[a-zA-Z0-9\d]{3}-[a-zA-Z0-9\d]{3,4}@[a-zA-Z0-9\d]{3}-[a-zA-Z0-9\d]{3}-.*$');


-- it looks good! let's now use the ADD_REGEX method to assign this to our Placekey Classifier
CALL placekey!ADD_REGEX(
  'PLACEKEY', -- semantic category
  'IDENTIFIER', -- privacy category
  '^[a-zA-Z0-9\d]{3}-[a-zA-Z0-9\d]{3,4}@[a-zA-Z0-9\d]{3}-[a-zA-Z0-9\d]{3}-.*$', -- regex expression
  'PLACEKEY*', --column name regex
  'Add a regex to identify Placekey' -- description
);


-- with the details in place, we can now use the LIST method to validate our work
SELECT placekey!LIST();


-- let's now use SYSTEM$CLASSIFY and our Classifier against the Location table
CALL SYSTEM$CLASSIFY('raw_pos.location', {'custom_classifiers': ['placekey'], 'auto_tag':true});


-- to finish, let's confirm our Placekey column was successfully tagged
SELECT 
    tag_name,
    level, 
    tag_value,
    column_name
FROM TABLE(information_schema.tag_references_all_columns('raw_pos.location','table'))
WHERE tag_value = 'PLACEKEY';

    /**
     Moving forward as Schemas or Tables are created and updated we can use this exact
     process of Automatic and Custom Classification to maintain a strong governance posture 
     and build rich semantic-layer metadata.
    **/


/*----------------------------------------------------------------------------------
Step 9 - Access History (Read and Writes)

 Access History provides insights into user queries encompassing what data was 
 read and when, as well as what statements have performed a write operations.
 
 For Tasty Bytes, Access History is particularly important for Compliance, Auditing,
 and Governance.

 Within this step, we will walk through leveraging Access History to find when the
 last time our Raw data was read from and written to.

 Note: Access History latency is up to 3 hours. If you have just recently setup
 the Tasty Bytes environment, some of the queries below may not have results.
---------------------------------------------------------------------------------*/


--> 1) how many queries have accessed each of our Raw layer tables directly?
SELECT 
    value:"objectName"::STRING AS object_name,
    COUNT(DISTINCT query_id) AS number_of_queries
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => direct_objects_accessed)
WHERE object_name ILIKE 'tb_101.raw_%'
GROUP BY object_name
ORDER BY number_of_queries DESC;


--> 2) what is the breakdown between Read and Write queries and when did they last occur?
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
WHERE object_name ILIKE 'tb_101.raw_%'
GROUP BY object_name, query_type
ORDER BY object_name, number_of_queries DESC;


--> 3) how many queries have accessed each of our Raw layer tables indirectly?
SELECT 
    base.value:"objectName"::STRING AS object_name,
    COUNT(DISTINCT query_id) AS number_of_queries
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => base_objects_accessed) base,
LATERAL FLATTEN (input => direct_objects_accessed) direct,
WHERE 1=1
    AND object_name ILIKE 'tb_101.raw_%'
    AND object_name <> direct.value:"objectName"::STRING -- base object is not direct object
GROUP BY object_name
ORDER BY number_of_queries DESC;

    /**
      Direct Objects Accessed: Data objects directly named in the query explicitly.
      Base Objects Accessed: Base data objects required to execute a query.
    **/ 


/*----------------------------------------------------------------------------------
Step 10 - Discovery with Snowflake Horizon - Universal Search

 Having explored a wide variety of Governance functionality available in Snowflake,
 it is time to put it all together with Universal Search.
 
 Universal Search enables Tasty Bytes to easily find Account objects,
 Snowflake Marketplace listings, relevant Snowflake Documentation and Snowflake
 Community Knowledge Base articles.

 Universal Search understands your query and information about your database
 objects and can find objects with names that differ from your search terms.
 
 Even if you misspell or type only part of your search term, you can still see
 useful results.
----------------------------------------------------------------------------------*/

/**
 To leverage Universal Search in Snowsight:
  -> uUse the Left Navigation Menu
  -> Select "Search" (Magnifying Glass)
  -> Enter Search criteria such as:
    - Tasty Bytes
    - Snowflake Best Practices
    - How to use Snowflake Column Masking
**/


/*----------------------------------------------------------------------------------
 Reset Scripts 
 
  Run the scripts below to reset your account to the state required to re-run
  this vignette.
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- drop Test Role
DROP ROLE IF EXISTS tb_test_role;

-- unset our Masking Policies
ALTER TAG tags.tasty_pii UNSET 
    MASKING POLICY governance.tasty_pii_string_mask,
    MASKING POLICY governance.tasty_pii_date_mask;

-- drop our Row Access Policy
ALTER TABLE raw_customer.customer_loyalty
DROP ROW ACCESS POLICY governance.customer_city_row_policy;

-- unset our Aggregation Policy
ALTER TABLE raw_pos.order_header UNSET AGGREGATION POLICY;

-- remove our Projection Policy
ALTER TABLE raw_customer.customer_loyalty MODIFY COLUMN postal_code UNSET PROJECTION POLICY;

-- unset the System Tags
--> customer_loyalty
ALTER TABLE raw_customer.customer_loyalty MODIFY
    COLUMN first_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN last_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN e_mail UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN gender UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN marital_status UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN birthday_date UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN phone_number UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN postal_code UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;

--> franchise
ALTER TABLE raw_pos.franchise MODIFY
    COLUMN first_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN last_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN e_mail UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN phone_number UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;

--> menu
ALTER TABLE raw_pos.menu MODIFY
    COLUMN menu_item_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;

--> location
ALTER TABLE raw_pos.location MODIFY
    COLUMN placekey UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN iso_country_code UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;    

--> truck
ALTER TABLE raw_pos.truck MODIFY
    COLUMN primary_city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN iso_country_code UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;   

--> country
ALTER TABLE raw_pos.country MODIFY
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN iso_country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;  

-- drop Custom Placekey Classifier
DROP snowflake.data_privacy.custom_classifier classifiers.placekey;

-- drop Tags, Governance and Classifiers Schemas (including everything within)
DROP SCHEMA IF EXISTS tags;
DROP SCHEMA IF EXISTS governance;
DROP SCHEMA IF EXISTS classifiers;

-- remove test Insert records
DELETE FROM raw_customer.customer_loyalty WHERE customer_id IN (000001, 000002, 000003, 000004, 000005, 000006);

-- unset Query Tag
ALTER SESSION UNSET query_tag;
