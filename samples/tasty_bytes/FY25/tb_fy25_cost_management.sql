/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Cost Management
Version:      v2
Script:       tb_fy25_collaboration.sql         
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
Cost Management
    a) Cost Optimization
        1 - Virtual Warehouses and Settings
    b) Cost Control
        2 - Resuming, Suspending and Scaling a Warehouse
        3 - Setting up Session Timeout Parameters 
        4 - Setting up Account Timeout Parameters
        5 - Setting up a Resource Monitors 
        6 - Setting up an Account Level Budget
    c) Cost Visibility
        7 - Tagging Objects to Attribute Spend
        8 - Exploring Cost with SQL
        9 - Exploring Cost with Snowsight
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-31          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Virtual Warehouses and Settings

 As a Tasty Bytes Snowflake Administrator we have been tasked with gaining an
 understanding of the features Snowflake provides to help ensure proper
 Cost Management is in place before we begin extracting value from our data.

 Within this step, we will create our first Snowflake Warehouse, which can be 
 thought of as virtual compute. 
 
 Snowflake recommends starting with the smallest sized Warehouse possible for the
 assigned workload, so for our Test Warehouse we will create it as an Extra Small.
----------------------------------------------------------------------------------*/

-- before we begin, let's set our Role, Warehouse and Database context
USE ROLE tb_admin;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;


-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit","name":"tb_zts,"version":{"major":1, "minor":1},"attributes":{"medium":"quickstart", "source":"tastybytes", "vignette": "cost_management"}}';


-- now let's look at all of the Warehouses available in our account by running a SHOW command
  --> NOTE: to execute a query use the top-right ▶️ button or COMMAND/CONTROL + ENTER
SHOW WAREHOUSES LIKE '%tb%';


-- having seen the configurable settings for a Snowflake Warehouse , let's create our own
-- Test Warehouse and reference the section below to understand each parameter is handling
CREATE OR REPLACE WAREHOUSE tb_test_wh WITH
COMMENT = 'test warehouse for tasty bytes'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true -- turn on 
    INITIALLY_SUSPENDED = true;

    /**
     1) Warehouse Type: Warehouses are required for queries, as well as all DML operations, including
         loading data into tables. Snowflake supports Standard (most-common) or Snowpark-optimized
          Warehouse Types. Snowpark-optimized warehouses should be considered for memory-intensive
          workloads.

     2) Warehouse Size: Size specifies the amount of compute resources available per cluster in a warehouse.
         Snowflake supports X-Small through 6X-Large sizes.

     3) Max Cluster Count: With multi-cluster warehouses, Snowflake supports allocating, either statically
         or dynamically, additional clusters to make a larger pool of compute resources available.
         A multi-cluster warehouse is defined by specifying the following properties:
            - Min Cluster Count: Minimum number of clusters, equal to or less than the maximum (up to 10).
            - Max Cluster Count: Maximum number of clusters, greater than 1 (up to 10).

     4) Scaling Policy: Specifies the policy for automatically starting and shutting down clusters in a
         multi-cluster warehouse running in Auto-scale mode.

     5) Auto Suspend: By default, Auto-Suspend is enabled. Snowflake automatically suspends the warehouse
         if it is inactive for the specified period of time, in our case 60 seconds.

     6) Auto Resume: By default, auto-resume is enabled. Snowflake automatically resumes the warehouse
         when any statement that requires a warehouse is submitted and the warehouse is the
         current warehouse for the session.

     7) Initially Suspended: Specifies whether the warehouse is created initially in the ‘Suspended’ state.
    **/


/*----------------------------------------------------------------------------------
Step 2 - Resuming, Suspending and Scaling a Warehouse

 With a Warehouse created, let's now use it to answer a few questions from the 
 business. While doing so we will learn how to resume, suspend and elastically
 scale the Warehouse.
----------------------------------------------------------------------------------*/

-- let's first set our Admin Role and Test Warehouse context
USE ROLE tb_admin;
USE WAREHOUSE tb_test_wh;


-- what menu items do we serve at our Plant Palace branded trucks?
    --> NOTE: Snowflake automatically resumes the warehouse when any statement that requires a warehouse is submitted
SELECT
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_id,
    m.menu_item_name
FROM raw_pos.menu m
WHERE truck_brand_name = 'Plant Palace';


-- to showcase Snowflakes elastic scalability let's scale our Warehouse up and run a few larger, aggregation queries
ALTER WAREHOUSE tb_test_wh SET warehouse_size = 'XLarge';


-- using a Show Warehouses command, we can confirm the current size of our Test Warehouse
SHOW WAREHOUSES LIKE 'tb_test_wh';


-- what are the total orders and total sales volumes for our truck brands? 
SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM analytics.orders_v o
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC;


-- what are the total orders and total sales volumes for our top customer loyalty members? 
SELECT
    o.customer_id,
    CONCAT(clm.first_name, ' ', clm.last_name) AS name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM analytics.orders_v o
JOIN analytics.customer_loyalty_metrics_v clm
    ON o.customer_id = clm.customer_id
GROUP BY o.customer_id, name
ORDER BY order_count DESC;


-- let's now scale our Test Warehouse back down
ALTER WAREHOUSE tb_test_wh SET warehouse_size = 'XSmall';


-- and now manually Suspend it
    --> NOTE: if you recieve "Invalid state. Warehouse cannot be suspended." the auto_suspend we configured earlier has already occured
ALTER WAREHOUSE tb_test_wh SUSPEND;


/*----------------------------------------------------------------------------------
Step 3 - Controlling Cost with Session Timeout Parameters 

 Within this step, let's now make sure we are protecting ourselves from bad,
 long running queries.
 
 To do this we will adjust two Statement Timeout Parameters on our Test Warehouse.
----------------------------------------------------------------------------------*/

-- to begin, let's look at the Statement Parameters for our Test Warehouse
SHOW PARAMETERS LIKE 'STATEMENT%' IN WAREHOUSE tb_test_wh;


-- let's start by adjusting the 2 Statement Parameters related to Query Timeouts
--> 1) adjust Statement Timeout on the Test Warehouse to 30 minutes
ALTER WAREHOUSE tb_test_wh
    SET statement_timeout_in_seconds = 1800; -- 1800 seconds = 30 minutes


--> 2) adjust Statement Queued Timeout on the Test Warehouse to 10 minutes
ALTER WAREHOUSE tb_test_wh
    SET statement_queued_timeout_in_seconds = 600; -- 600 seconds = 10 minutes

    /**
     Statement Timeout in Seconds: Timeout in seconds for statements: statements are automatically canceled if they
      run for longer; if set to zero, max value (604800) is  enforced.

     Statement Queued in Second: Timeout in seconds for queued statements: statements will automatically be
      canceled if they are queued on a warehouse for longer than this  amount of time; disabled if set to zero.
    **/

/*----------------------------------------------------------------------------------
Step 4 - Controlling Cost with Account Timeout Parameters 

 The Timeout Parameters we set on our Test Warehouse are also available at the
 Account, User and Session level. Within this step, we will adjust these at the
 Account level.

 Moving forward we will plan to monitor these as our Snowflake Workloads and Usage
 grow to ensure they are continuing to protect our account from unneccesary consumption
 but allowing for expected longer jobs to complete.
----------------------------------------------------------------------------------*/

-- to begin we will assume the role of Accountadmin
USE ROLE accountadmin;


--> 1) adjust Statement Timeout on the the Account to 5 hours
ALTER ACCOUNT 
    SET statement_timeout_in_seconds = 18000; -- 18000 seconds = 5 hours


--> 2) adjust Statement Queued Timeout on the Account to 1 hour
ALTER ACCOUNT
    SET statement_queued_timeout_in_seconds = 3600; -- 3600 seconds = 1 hour;


/*----------------------------------------------------------------------------------
Step 5 - Monitoring Cost with Resource Monitors

 With a Test Warehouse in place, let's now leverage Snowflakes Resource Monitors
 to ensure the Warehouse has a monthly quota. This will also allow Admins to monitor
 credit consumption and trigger Warehouse suspension if the quota is surpassed.

 Within this step we will create our Resource Monitor using SQL but these can also
 be deployed and monitored in Snowsight by navigating to Admin -> Cost Management.
----------------------------------------------------------------------------------*/

   /**
     Resource Monitor: A resource monitor can be used to monitor credit usage by virtual warehouses
      and the cloud services needed to support those warehouses. If desired, the warehouse can be
      suspended when it reaches a credit limit.
    **/

-- create our Resource Monitor
CREATE OR REPLACE RESOURCE MONITOR tb_test_rm
WITH
    CREDIT_QUOTA = 100 -- set the quota to 100 credits
    FREQUENCY = monthly -- reset the monitor monthly
    START_TIMESTAMP = immediately -- begin tracking immediately
    TRIGGERS
        ON 75 PERCENT DO NOTIFY -- notify accountadmins at 75%
        ON 100 PERCENT DO SUSPEND -- suspend warehouse at 100 percent, let queries finish
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- suspend warehouse and cancel all queries at 110 percent


-- with the Resource Monitor created, apply it to our Test Warehouse
ALTER WAREHOUSE tb_test_wh 
    SET RESOURCE_MONITOR = tb_test_rm;


/*----------------------------------------------------------------------------------
Step 6 - Monitoring Cost with Budgets

 In the previous step we configured a Resource Monitor that allows for monitoring
 costs for Warehouses and Cloud Services. 
 
 Now to help Tasty Bytes monitor cost across the entire account we will deploy a
 Budget using SQL however these can also be deployed and monitored in Snowsight
 by navigating to Admin -> Cost Management.
----------------------------------------------------------------------------------*/

    /**
     Budget: A Budget enables account-level monitoring and notification of Snowflake
      credit usage for a group of specific Snowflake objects. A budget sends a daily
      alert notification when current spending is on track to exceed the spending
      limit based on time-series forecasting. 
    **/

-- remaining in our Accountadmin role, let's activate our account level budget 
    --> if you recieve BUDGET_ALREADY_ACTIVATED message then this budget is already enabled on the account
CALL snowflake.local.account_root_budget!ACTIVATE();


-- now let's set a 1000 credit monthly spending limit for our account  
CALL snowflake.local.account_root_budget!SET_SPENDING_LIMIT(1000);


-- with our 1000 credit monthly Account Level Budget in place, we can now monitor how we are tracking in a few ways:
    -- option 1: view current, forecasted and historical budget spending using the Budgets page in Snowsight.
        --> path to Admin -> Cost Management -> Budgets

    -- option 2: recieve daily Email Alert Notifications when current spend is on track to exceed the defined spending limit.
        --> NOTE: the below SQL is for demonstration purposes only and only needs to be filled out
        --> and executed if you would like to set up the daily alerts.

        --> create a Budget Notification Integration including the desired email recipients
            --> NOTE: These Emails must be verified within the Snowflake Account
            CREATE NOTIFICATION INTEGRATION budgets_notification_integration
            TYPE= email
            ENABLED = true
            ALLOWED_RECIPIENTS = ('costadmin@example.com','budgetadmin@example.com');

        --> grant Usage to leverage the Budget Notification Integration
            GRANT USAGE ON INTEGRATION budgets_notification_integration TO APPLICATION snowflake;
        
        --> turn on daily Email notifications for our Account Level Budget
            CALL snowflake.local.account_root_budget!SET_EMAIL_NOTIFICATIONS(
            'budgets_notification_integration', 'costadmin@example.com, budgetadmin@example.com');


/*----------------------------------------------------------------------------------
Step 7 - Tag Objects to Attribute Spend

 Within this step, we will help our Finance department attribute consumption costs
 for the Test Warehouse to our Development Team. 
 
 We will create a Tag object for associating Cost Centers to Database
 Objects and Warehouses and leverage it to assign the Development Team Cost Center
 to our Test Warehouse.
----------------------------------------------------------------------------------*/

    /**
     Tag: A tag is a schema-level object that can be assigned to another Snowflake object.
      A tag can be assigned an arbitrary string value upon assigning the tag to a Snowflake object.
      Snowflake stores the tag and its string value as a key-value pair.
    **/
    
-- first, we will create our Cost Center Tag
CREATE OR REPLACE TAG cost_center;


-- now we use the Tag to attach the Development Team Cost Center to the Test Warehouse
ALTER WAREHOUSE tb_test_wh SET TAG cost_center = 'DEVELOPMENT_TEAM';


-- using our information_schema, let's confirm our tag is in place
SELECT 
    tag_name,
    tag_value,
    level,
    object_name
FROM TABLE(information_schema.tag_references('tb_test_wh', 'warehouse'));


/*----------------------------------------------------------------------------------
Step 8 - Exploring Cost with SQL

 Snowflake provides a system-defined, read-only shared database named Snowflake
 that contains metadata and historical usage data about the objects in your
 organization and accounts. 

 In this step, we will explore Warehouse and Cloud Services Costs using the 
 Warehouse Metering History View from within.
----------------------------------------------------------------------------------*/

-- what is the total daily warehouse consumption for the last month?
SELECT 
    TO_DATE(start_time) AS start_date,
    SUM(credits_used) AS credits_used
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_date > DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY start_date
ORDER BY start_date DESC;


-- what is our total daily consumption by warehouse for the last month?
SELECT 
    TO_DATE(start_time) AS start_date,
    warehouse_name,
    SUM(credits_used) AS credits_used
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_date > DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY start_date, warehouse_name
ORDER BY start_date DESC;


/*----------------------------------------------------------------------------------
Step 9 - Exploring Cost with Snowsight

Snowflake also provides many ways to visually inspect Cost data within Snowsight.
In this step, we will walk through the click path to access a few of these pages.

To access an overview of incurred costs within Snowsight:
    1. Select Admin » Cost Management.
    2. Select a warehouse to use to view the usage data.
        • Snowflake recommends using an X-Small warehouse for this purpose.
    3. Select Account Overview.

To access and drill down into overall cost within Snowsight: 
    1. Select Admin » Cost Management.
    2. Select a warehouse to use to view the usage data.
        • Snowflake recommends using an X-Small warehouse for this purpose.
    3. Select Consumption.
    4. Select All Usage Types from the drop-down list.
----------------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------------
 Reset Scripts 
 
  Run the scripts below to reset your account to the state required to re-run
  this vignette.
----------------------------------------------------------------------------------*/           
USE ROLE accountadmin;

-- drop Test Warehouse
DROP WAREHOUSE IF EXISTS tb_test_wh;

-- drop Cost Center Tag
DROP TAG IF EXISTS cost_center;

-- drop Resource Monitor
DROP RESOURCE MONITOR IF EXISTS tb_test_rm;

-- drop Notification Integration
DROP NOTIFICATION INTEGRATION IF EXISTS budgets_notification_integration;

-- reset Account Timeout Parameters
ALTER ACCOUNT SET statement_timeout_in_seconds = default;
ALTER ACCOUNT SET statement_queued_timeout_in_seconds = default;

-- unset Query Tag
ALTER SESSION UNSET query_tag;

-- deactivate account budget
USE WAREHOUSE tb_de_wh;
CALL snowflake.local.account_root_budget!DEACTIVATE();
