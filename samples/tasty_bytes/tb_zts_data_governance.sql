/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Data Governance
Version:      v1
Script:       tb_zts_data_governance.sql         
Create Date:  2023-03-17
Author:       Jacob Kranzler
Copyright(c): 2023 Snowflake Inc. All rights reserved.
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2023-03-17          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Quickstart Section 3 - Exploring Available Roles
 Our Tasty Bytes Adminstrator has been tasked with learning the process of deploying
 Role Based Access Control (RBAC) and proper Data Governance across our Snowflake
 Account. 

 To begin, let's first dive into the Snowflake System Defined Roles provided by
 default in all accounts and learn a bit more on their privileges.
----------------------------------------------------------------------------------*/

-- Section 3: Step 1 - Setting our Context
USE ROLE accountadmin;
USE WAREHOUSE tasty_dev_wh;


-- Section 3: Step 2 - Exploring All Roles in our Account
SHOW ROLES;


-- Section 3: Step 3 - Using Result Scan to Filter our Result
SELECT 
    "name",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('ORGADMIN','ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN','PUBLIC');


/*----------------------------------------------------------------------------------
Quickstart Section 4 - Creating a Role and Granting Privileges

 Now that we understand these System Defined roles, let's begin leveraging them to
 create a test role and grant it access to the Customer Loyalty data we will deploy
 our initial Data Governance features against and our tasty_dev_wh Warehouse
----------------------------------------------------------------------------------*/

-- Section 4: Step 1 - Using the Useradmin Role to Create our Test Role
USE ROLE useradmin;

CREATE OR REPLACE ROLE tasty_test_role
    COMMENT = 'test role for tasty bytes';

-- Section 4: Step 2 - Using the Securityadmin Role to Grant Warehouse Privileges
USE ROLE securityadmin;
GRANT OPERATE, USAGE ON WAREHOUSE tasty_dev_wh TO ROLE tasty_test_role;


-- Section 4: Step 3 - Using the Securityadmin Role to Grant Database and Schema Privileges
GRANT USAGE ON DATABASE frostbyte_tasty_bytes TO ROLE tasty_test_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes TO ROLE tasty_test_role;


-- Section 4: Step 4 - Using the Securityadmin Role to Grant Table and View Privileges
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_customer TO ROLE tasty_test_role;
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_pos TO ROLE tasty_test_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes.analytics TO ROLE tasty_test_role;


-- Section 4: Step 5 - Using the Securityadmin Role to our Role to our User
SET my_user_var  = CURRENT_USER();
GRANT ROLE tasty_test_role TO USER identifier($my_user_var);


/*----------------------------------------------------------------------------------
Quickstart Section 4 - Creating and Attaching Tags to our PII Columns

 The first Data Governance feature set we want to deploy and test will be Snowflake
 Tag Based Dynamic Data Masking. This feature will allow us to mask PII data in 
 columns at query run time from our test role but leave it exposed to more 
 privileged roles.

 Before we can begin masking data, let's first explore what PII exists in our
 Customer Loyalty data.
----------------------------------------------------------------------------------*/

-- Section 4: Step 1 - Finding our PII Columns
USE ROLE tasty_test_role;
USE WAREHOUSE tasty_dev_wh;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.city,
    cl.country
FROM frostbyte_tasty_bytes.raw_customer.customer_loyalty cl;


-- Section 4: Step 2 - Creating Tags
USE ROLE accountadmin;

CREATE OR REPLACE TAG frostbyte_tasty_bytes.raw_customer.pii_name_tag
    COMMENT = 'PII Tag for Name Columns';
    
CREATE OR REPLACE TAG frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag
    COMMENT = 'PII Tag for Phone Number Columns';
    
CREATE OR REPLACE TAG frostbyte_tasty_bytes.raw_customer.pii_email_tag
    COMMENT = 'PII Tag for E-mail Columns';


-- Section 4 - Step 3 - Applying Tags
ALTER TABLE frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN first_name 
        SET TAG frostbyte_tasty_bytes.raw_customer.pii_name_tag = 'First Name';

ALTER TABLE frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN last_name 
        SET TAG frostbyte_tasty_bytes.raw_customer.pii_name_tag = 'Last Name';

ALTER TABLE frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN phone_number 
        SET TAG frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag = 'Phone Number';

ALTER TABLE frostbyte_tasty_bytes.raw_customer.customer_loyalty 
    MODIFY COLUMN e_mail
        SET TAG frostbyte_tasty_bytes.raw_customer.pii_email_tag = 'E-mail Address';


-- Section 4: Step 4 - Exploring Tags on a Table
SELECT 
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value 
FROM TABLE(frostbyte_tasty_bytes.information_schema.tag_references_all_columns
    ('frostbyte_tasty_bytes.raw_customer.customer_loyalty','table'));


/*----------------------------------------------------------------------------------
Quickstart Section 5 - Creating and Applying Tag Based Masking Policies

 With our Tag foundation in place, we can now begin to develop Dynamic Masking 
 Policies to support different masking requirements for our name, phone number
 and e-mail columns.
----------------------------------------------------------------------------------*/

-- Section 5: Step 1 - Creating Masking Policies
USE ROLE sysadmin;

CREATE OR REPLACE MASKING POLICY frostbyte_tasty_bytes.raw_customer.name_mask AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE '**~MASKED~**'
END;

CREATE OR REPLACE MASKING POLICY frostbyte_tasty_bytes.raw_customer.phone_mask AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT(LEFT(val,3), '-***-****')
END;

CREATE OR REPLACE MASKING POLICY frostbyte_tasty_bytes.raw_customer.email_mask AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT('**~MASKED~**','@', SPLIT_PART(val, '@', -1))
END;
            

-- Section 5: Step 2 - Applying Masking Policies Tags
USE ROLE accountadmin;

ALTER TAG frostbyte_tasty_bytes.raw_customer.pii_name_tag 
    SET MASKING POLICY frostbyte_tasty_bytes.raw_customer.name_mask;
    
ALTER TAG frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag
    SET MASKING POLICY frostbyte_tasty_bytes.raw_customer.phone_mask;
    
ALTER TAG frostbyte_tasty_bytes.raw_customer.pii_email_tag
    SET MASKING POLICY frostbyte_tasty_bytes.raw_customer.email_mask;


/*----------------------------------------------------------------------------------
Quickstart Section 6 - Testing our Tag Based Masking Policies

 With deployment of our Tag Based Masking Policies in place let's validate what we
 have conducted so far to confirm we were successful in meeting Tasty Bytes Customer
 Loyalty PII Data Masking requirements.
----------------------------------------------------------------------------------*/

-- Section 6: Step 1 - Testing our Masking Policy in a non-Admin Role
USE ROLE tasty_test_role;
USE WAREHOUSE tasty_dev_wh;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    cl.city,
    cl.country
FROM frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
WHERE cl.country IN ('United States','Canada','Brazil');


-- Section 6: Step 2 - Testing our Masking Policy Downstream
SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd DESC;


-- Section 6: Step 3 - Testing our Masking Policy in an Admin Role
USE ROLE accountadmin;

SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
WHERE 1=1
    AND clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd DESC;


/*----------------------------------------------------------------------------------
Quickstart Section 7 - Deploying and Testing Row Level Security

Happy with our Tag Based Dynamic Masking controlling masking at the column level,
we will now look to restrict access at the row level for our test role. 

Within our Customer Loyalty table, our role should only see Customers who are
based in Tokyo. Thankfully, Snowflake has another powerful native Data Governance
feature that can handle this at scale called Row Access Policies. 

For our use case, we will leverage the mapping table approach.
----------------------------------------------------------------------------------*/

-- Section 7: Step 1 - Creating a Mapping Table
USE ROLE sysadmin;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes.public.row_policy_map
    (role STRING, city_permissions STRING);

    
-- Section 7: Step 2 - Inserting Mapping Records
INSERT INTO frostbyte_tasty_bytes.public.row_policy_map
    VALUES ('TASTY_TEST_ROLE','Tokyo');
    

-- Section 7: Step 3 - Creating a Row Access Policy
CREATE OR REPLACE ROW ACCESS POLICY frostbyte_tasty_bytes.public.customer_city_row_policy
    AS (city STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN 
       (
           'ACCOUNTADMIN','SYSADMIN', 'TASTY_ADMIN', 'TASTY_DATA_ENGINEER', 
           'TASTY_DATA_APP','TASTY_BI','TASTY_DATA_SCIENTIST','TASTY_DEV'
       ) 
        OR EXISTS 
            (
            SELECT rp.role 
                FROM frostbyte_tasty_bytes.public.row_policy_map rp
            WHERE 1=1
                AND rp.role = CURRENT_ROLE()
                AND rp.city_permissions = city
            );

            
-- Section 7: Step 4 - Applying a Row Access Policy to a Table
ALTER TABLE frostbyte_tasty_bytes.raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY frostbyte_tasty_bytes.public.customer_city_row_policy ON (city);

    
-- Section 7: Step 5 - Testing our Row Access Policy in a Non-Privileged Role
USE ROLE tasty_test_role;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;


-- Section 7: Step 6 - Testing our Row Access Policy Downstream
SELECT 
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM frostbyte_tasty_bytes.analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;


-- Section 7: Step 7 - Testing our Row Access Policy in a Privileged Role
USE ROLE sysadmin;

SELECT 
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM frostbyte_tasty_bytes.raw_customer.customer_loyalty cl
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;



/**********************************************************************/
/*------               Quickstart Reset Scripts                 ------*/
/*------   These can be ran to reset your account to a state    ------*/
/*----- that will allow you to run through this Quickstart again -----*/
/**********************************************************************/

USE ROLE accountadmin;

DROP ROLE IF EXISTS tasty_test_role;

ALTER TAG frostbyte_tasty_bytes.raw_customer.pii_name_tag UNSET MASKING POLICY frostbyte_tasty_bytes.raw_customer.name_mask;
ALTER TAG frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag UNSET MASKING POLICY frostbyte_tasty_bytes.raw_customer.phone_mask;
ALTER TAG frostbyte_tasty_bytes.raw_customer.pii_email_tag UNSET MASKING POLICY frostbyte_tasty_bytes.raw_customer.email_mask;

DROP TAG IF EXISTS frostbyte_tasty_bytes.raw_customer.pii_name_tag;
DROP TAG IF EXISTS frostbyte_tasty_bytes.raw_customer.pii_phone_number_tag;
DROP TAG IF EXISTS frostbyte_tasty_bytes.raw_customer.pii_email_tag;

ALTER TABLE frostbyte_tasty_bytes.raw_customer.customer_loyalty
DROP ROW ACCESS POLICY frostbyte_tasty_bytes.public.customer_city_row_policy;

DROP TABLE IF EXISTS frostbyte_tasty_bytes.public.row_policy_map;
