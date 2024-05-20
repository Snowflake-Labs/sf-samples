/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Transformation
Version:      v2
Script:       tb_fy25_transformation.sql
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
Transformation
    1 - Zero Copy Cloning
    2 - Using Persisted Query Results
    3 - Adding and Updating a Column in a Table
    4 - Time-Travel for Table Restore
    5 - Table Swap, Drop and Undrop
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Zero Copy Cloning

 As part of Tasty Bytes Fleet Analysis, our Developer has been tasked with creating
 and updating a new Truck Type column within the Raw layer Truck table that combines
 the Year, Make and Model together.

 Within this step, we will first walk through standing up a Development environment
 using Snowflake Zero Copy Cloning for this development to be completed and tested
 within before rolling into production.
----------------------------------------------------------------------------------*/

-- before we begin, let's set our Role and Warehouse context
USE ROLE tb_dev;
USE DATABASE tb_101;


-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit","name":"tb_zts,"version":{"major":1, "minor":1},"attributes":{"medium":"quickstart", "source":"tastybytes", "vignette": "transformation"}}';


-- to ensure our new Column development does not impact production,
-- let's first create a snapshot copy of the Truck table using Clone 
CREATE OR REPLACE TABLE raw_pos.truck_dev CLONE raw_pos.truck;

      /**
        Zero Copy Cloning: Creates a copy of a database, schema or table. A snapshot of data present in
         the source object is taken when the clone is created and is made available to the cloned object.

         The cloned object is writable and is independent of the clone source. That is, changes made to
         either the source object or the clone object are not part of the other. Cloning a database will
         clone all the schemas and tables within that database. Cloning a schema will clone all the
         tables in that schema.
      **/

-- before we query our clone, let's now set our Warehouse context
    --> NOTE: a Warehouse isn't required in a Clone statement as it is handled via Snowwflake Cloud Services
USE WAREHOUSE tb_dev_wh;


-- with our Zero Copy Clone created, let's query for what we will need to combine for our new Truck Type column
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM raw_pos.truck_dev t
ORDER BY t.truck_id;


/*----------------------------------------------------------------------------------
Step 2 - Using Persisted Query Results

 If a user repeats a query that has already been run in the last 24 hours, and the 
 data in the table hasn’t changed since the last time that the query was run, 
 then the result of the query is the same. 
 
 Instead of running the query again, Snowflake simply returns the same result that
 it returned previously from the result set cache. Within this step, we will
 test this functionality.
----------------------------------------------------------------------------------*/

-- to test Snowflake's Result Cache, let's first suspend our Warehouse
    --> NOTE: if you recieve "..cannot be suspended" then the Warehouse Auto Suspend timer has already elapsed
ALTER WAREHOUSE tb_dev_wh SUSPEND;


-- with our compute suspended, let's re-run our query from above
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model, --> Snowflake supports Trailing Comma's in SELECT clauses
FROM raw_pos.truck_dev t
ORDER BY t.truck_id;


-- a few things we should look at after executing the query:

    -- did the Warehouse turn on?
        --> In the Top Right Context window, explore the Warehouse status via Show Warehouse details

    -- what does the Query Profile show?
        --> In the Query Details pane next to Result, Click on the Query ID to open up the Query Profile

    -- we also need to fix the typo from Ford_ to Ford in the Make column
        --> We will do this in the next Step

  
/*----------------------------------------------------------------------------------
Step 3 - Adding and Updating a Column in a Table

 Within this step, we will now will Add and Update a Truck Type column
 to the Development Truck Table we created previously while also addressing
 the typo in the Make field.
----------------------------------------------------------------------------------*/

-- to start, let's correct the typo we noticed in the Make column
UPDATE raw_pos.truck_dev 
    SET make = 'Ford' WHERE make = 'Ford_';


-- now, let's build the query to concatenate columns together that will make up our Truck Type
SELECT
    truck_id,
    year,
    make,
    model,
    CONCAT(year,' ',make,' ',REPLACE(model,' ','_')) AS truck_type
FROM raw_pos.truck_dev;


-- let's now Add the Truck Type Column to the table
ALTER TABLE raw_pos.truck_dev 
    ADD COLUMN truck_type VARCHAR(100);


-- with our empty column in place, we can now run the Update statement to populate each row
UPDATE raw_pos.truck_dev
    SET truck_type =  CONCAT(year,make,' ',REPLACE(model,' ','_'));


--with 450 rows successfully updated, let's validate our work
SELECT
    truck_id,
    year,
    truck_type
FROM raw_pos.truck_dev
ORDER BY truck_id;


/*----------------------------------------------------------------------------------
Step 3 - Time-Travel for Table Restore

 Oh no! We made a mistake on the Update statement earlier and missed adding a space 
 between Year and Make. Thankfully, we can use Time Travel to revert our table back
 to the state it was after we fixed the misspelling so we can correct our work.

 Time-Travel enables accessing data that has been changed or deleted at any point
 up to 90 days. It serves as a powerful tool for performing the following tasks:
   - Restoring data objects that have been incorrectly changed or deleted.
   - Duplicating and backing up data from key points in the past.
   - Analyzing data usage/manipulation over specified periods of time.
----------------------------------------------------------------------------------*/

-- first, let's look at all Update statements to our Development Table using the Query History function
SELECT
    query_id,
    query_text,
    user_name,
    query_type,
    start_time
FROM TABLE(information_schema.query_history())
WHERE 1=1
    AND query_type = 'UPDATE'
    AND query_text LIKE '%raw_pos.truck_dev%'
ORDER BY start_time DESC;


-- for future use, let's create a SQL variable and store the Update statement's Query ID in it
SET query_id =
    (
    SELECT TOP 1
        query_id
    FROM TABLE(information_schema.query_history())
    WHERE 1=1
        AND query_type = 'UPDATE'
        AND query_text LIKE '%SET truck_type =%'
    ORDER BY start_time DESC
    );

    /**
    Time-Travel provides many different statement options including:
        • At, Before, Timestamp, Offset and Statement

    For our demonstration we will use Statement since we have the Query ID from our bad
    Update statement that we want to revert our table back to the state it was before execution.
    **/

-- now we can leverage Time Travel and our Variable to look at the Development Table state we will be reverting to
SELECT 
    truck_id,
    make,
    truck_type
FROM raw_pos.truck_dev
BEFORE(STATEMENT => $query_id)
ORDER BY truck_id;


-- using Time Travel and Create or Replace Table, let's restore our Development Table
CREATE OR REPLACE TABLE raw_pos.truck_dev
    AS
SELECT * FROM raw_pos.truck_dev
BEFORE(STATEMENT => $query_id); -- revert to before a specified Query ID ran


--to conclude, let's run the correct Update statement 
UPDATE raw_pos.truck_dev t
    SET truck_type = CONCAT(t.year,' ',t.make,' ',REPLACE(t.model,' ','_'));


/*----------------------------------------------------------------------------------
Step 4 - Table Swap, Drop and Undrop

 Based on our previous efforts, we have addressed the requirements we were given and
 to complete our task need to push our Development into Production.

 Within this step, we will swap our Development Truck table with what is currently
 available in Production.
----------------------------------------------------------------------------------*/

-- our Accountadmin role will now Swap our Development Table with the Production
USE ROLE accountadmin;

ALTER TABLE raw_pos.truck_dev 
    SWAP WITH raw_pos.truck;


-- let's confirm the production Truck table has the new column in place
SELECT
    t.truck_id,
    t.truck_type
FROM raw_pos.truck t
WHERE t.make = 'Ford';


-- looks great, let's now drop the Development Table
DROP TABLE raw_pos.truck;


-- we have made a mistake! that was the production version of the table!
-- let's quickly use another Time Travel reliant feature and Undrop it
UNDROP TABLE raw_pos.truck;


-- with the Production table restored we can now correctly drop the Development Table
DROP TABLE raw_pos.truck_dev;


/*----------------------------------------------------------------------------------
 Reset Scripts 
 
  Run the scripts below to reset your account to the state required to re-run
  this vignette.
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- revert Ford to Ford_
UPDATE tb_101.raw_pos.truck SET make = 'Ford_' WHERE make = 'Ford';

-- remove Truck Type column
ALTER TABLE tb_101.raw_pos.truck DROP COLUMN IF EXISTS truck_type;

-- unset SQL Variable
UNSET query_id;

-- unset Query Tag
ALTER SESSION UNSET query_tag;