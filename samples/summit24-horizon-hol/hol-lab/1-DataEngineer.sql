/***************************************************************************************************
| H | O | R | I | Z | O | N |   | L | A | B | S | 

Demo:         Horizon Lab (Data Engineer Persona)
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

/*----------------------------------------------------------------------------------
Snowflake’s approach to access control combines aspects from both of the following models:
  • Discretionary Access Control (DAC): Each object has an owner, who can in turn grant access to that object.
  • Role-based Access Control (RBAC): Access privileges are assigned to roles, which are in turn assigned to users.

The key concepts to understanding access control in Snowflake are:
  • Securable object: An entity to which access can be granted. Unless allowed by a grant, access is denied.
  • Role: An entity to which privileges can be granted. Roles are in turn assigned to users. Note that roles can also be assigned to other roles, creating a role hierarchy.
  • Privilege: A defined level of access to an object. Multiple distinct privileges may be used to control the granularity of access granted.
  • User: A user identity recognized by Snowflake, whether associated with a person or program.

  
In Summary:
  • In Snowflake, a Role is a container for Privileges to a Securable Object.
  • Privileges can be granted Roles
  • Roles can be granted to Users
  • Roles can be granted to other Roles (which inherit that Roles Privileges)
  • When Users choose a Role, they inherit all the Privileges of the Roles in the 
    hierarchy.
----------------------------------------------------------------------------------*/




/*************************************************/
/*************************************************/
/*           R B A C    &   D A C                */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step - System Defined Roles and Privileges

Let's first take a look at the Snowflake System Defined Roles and their privileges.
----------------------------------------------------------------------------------*/

USE ROLE HRZN_DATA_ENGINEER;
USE WAREHOUSE HRZN_WH;
USE DATABASE HRZN_DB;
USE SCHEMA HRZN_SCH;


--Let's take a look at the Roles currently in our account
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
Step - Role Creation, GRANTS and SQL Variables

 Now that we understand System Defined Roles, let's begin leveraging them to create
 a Test Role to provide access to the customer table.
----------------------------------------------------------------------------------*/

-- let's use the Useradmin Role to create a Data Analyst Role
USE ROLE USERADMIN;

CREATE OR REPLACE ROLE HRZN_DATA_ANALYST
    COMMENT = 'Analyst Role';


-- now we will switch to Securityadmin to handle our privilege GRANTS
USE ROLE SECURITYADMIN;

-- first we will grant ALL privileges on the Development Warehouse to our Data Analyst Role
GRANT ALL ON WAREHOUSE HRZN_WH TO ROLE HRZN_DATA_ANALYST;

-- next we will grant only OPERATE and USAGE privileges to our Test Role
GRANT OPERATE, USAGE ON WAREHOUSE HRZN_WH TO ROLE HRZN_DATA_ANALYST;

-- before we proceed, let's SET a SQL Variable to equal our CURRENT_USER()
SET MY_USER_ID  = CURRENT_USER();

-- now we can GRANT our Role to the User we are currently logged in as
GRANT ROLE HRZN_DATA_ANALYST TO USER identifier($MY_USER_ID);

--Lets try and access the CUSTOMER TABLE.
SELECT * FROM HRZN_DB.HRZN_SCH.CUSTOMER;

--The previous query fails as the role hasn't been provided access to query the database, schema or the table CUSTOMER.

-- now we will grant USAGE on our Database and all Schemas within it
GRANT USAGE ON DATABASE HRZN_DB TO ROLE HRZN_DATA_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE HRZN_DB TO ROLE HRZN_DATA_ANALYST;

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
GRANT SELECT ON ALL TABLES IN SCHEMA HRZN_DB.HRZN_SCH TO ROLE HRZN_DATA_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA HRZN_DB.HRZN_SCH TO ROLE HRZN_DATA_ANALYST;

    /**
     Snowflake View and Table Privilege Grants
      1 - SELECT: Enables executing a SELECT statement on a table/view.
      2 - INSERT: Enables executing an INSERT command on a table. 
      3 - UPDATE: Enables executing an UPDATE command on a table.
      4 - TRUNCATE: Enables executing a TRUNCATE TABLE command on a table.
      5 - DELETE: Enables executing a DELETE command on a table.
    **/


USE ROLE HRZN_DATA_ANALYST;

--Lets query the table again
SELECT * FROM HRZN_DB.HRZN_SCH.CUSTOMER;












/*************************************************/
/*************************************************/
/* D A T A      E N G I N E E R      R O L E */
/*************************************************/
/*************************************************/

USE ROLE HRZN_DATA_ENGINEER;


/*----------------------------------------------------------------------------------
Step  - Load the Data into the table

  The first Governance feature set we want to deploy and test will be Snowflake Tag
  Based Dynamic Data Masking. This will allow us to mask PII data in columns from
  our Test Role but not from more privileged Roles.
----------------------------------------------------------------------------------*/
--Load the file CustomerDataRaw.csv into the HRZN_DB.HRZN_SCH.CUSTOMER table via the snowsight UI
--Load the file CustomerOrders.csv into the HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS table via the snowsight UI
--Menu: Data -> Databases -> HRZN_DB -> HRZN_SCH -> Tables -> CUSTOMER



/*----------------------------------------------------------------------------------
Step  - Data Quality Monitoring 

 Within Snowflake, you can measure the quality of your data by using Data Metric
 Functions. Using these, we want to ensure that there are not duplicate or invalid
 Customer Email Addresses present in our system. While our team works to resolve any
 existing bad records, we will work to monitor these occuring moving forward.

 Within this step, we will walk through adding Data Metric Functions to our Customer
 Table to capture Duplicate and Invalid Email Address counts every 5 minutes.
----------------------------------------------------------------------------------*/

/*  S Y S T E M   D M F */
--System DMF
-- Set the schedule on the table
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER
SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

--Accuracy
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT on (EMAIL);

--Uniqueness
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT on (EMAIL);
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT on (EMAIL);;

--Volume
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT on ();


--Review Counts
SELECT SNOWFLAKE.CORE.NULL_COUNT(SELECT EMAIL FROM HRZN_DB.HRZN_SCH.CUSTOMER);
SELECT SNOWFLAKE.CORE.UNIQUE_COUNT(SELECT EMAIL FROM HRZN_DB.HRZN_SCH.CUSTOMER);
SELECT SNOWFLAKE.CORE.DUPLICATE_COUNT (SELECT EMAIL FROM HRZN_DB.HRZN_SCH.CUSTOMER) AS duplicate_count;

-- before moving on, let's validate that our schedule is in place
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE HRZN_DB.HRZN_SCH.CUSTOMER;



/*  C U S T O M   D M F */

-- to accompany the Duplicate Count DMF, let's also create a Custom Data Metric Function
-- that uses Regular Expression (RegEx) to Count Invalid Email Addresses
CREATE DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.INVALID_EMAIL_COUNT(IN_TABLE TABLE(IN_COL STRING))
RETURNS NUMBER 
AS
'SELECT COUNT_IF(FALSE = (IN_COL regexp ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'')) FROM IN_TABLE';


-- for demo purposes, let's grant this to everyone
GRANT ALL ON FUNCTION HRZN_DB.HRZN_SCH.INVALID_EMAIL_COUNT(TABLE(STRING)) TO ROLE PUBLIC;


-- as we did above, let's see how many Invalid Email Addresses currently exist
SELECT HRZN_DB.HRZN_SCH.INVALID_EMAIL_COUNT(SELECT EMAIL FROM HRZN_DB.HRZN_SCH.CUSTOMER) AS INVALID_EMAIL_COUNT;


-- before we can apply our DMF's to the table, we must first set the Data Metric Schedule. for our
-- testing we will Trigger this to run every 5 minutes 

ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER SET DATA_METRIC_SCHEDULE = '5 minute'; -- for demo purpose, use 5 minute

/**
Data Metric Schedule specifies the schedule for running Data Metric Functions
for tables and can leverage MINUTE, USING CRON or TRIGGER_ON_CHANGES
**/

-- add our Invalid Email Count Data Metric Function (DMF)
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER 
    ADD DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.INVALID_EMAIL_COUNT ON (EMAIL);


-- before moving on, let's ensure that the Schedule is in place
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE HRZN_DB.HRZN_SCH.CUSTOMER;


--Review the schedule
select metric_name, ref_entity_name, schedule, schedule_status 
from table(information_schema.data_metric_function_references(
    ref_entity_name => 'HRZN_DB.HRZN_SCH.CUSTOMER', 
    ref_entity_domain => 'TABLE'));


-- the results from our Data Metric Functions can be access through the DATA_QUALITY_MONITORING_RESULTS view
SELECT 
    change_commit_time,
    measurement_time,
    table_schema,
    table_name,
    metric_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'HRZN_DB'
ORDER BY change_commit_time DESC;


/**

 In a production scenario a logical next step would be to configure alerts to notify you
 when changes to data quality occur. By combining the DMF and alert functionality, you can
 have consistent threshold notifications for data quality on the tables that you measure. 
**/
