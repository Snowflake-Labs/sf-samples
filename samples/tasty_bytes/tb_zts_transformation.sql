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
Version:      v1
Script:       tb_zts_transformation.sql         
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
Quickstart Section Step 3 - Instantly Clone Production Table for Development

 As part of Tasty Bytes truck fleet analysis, our developer has been tasked with 
 adding a calculated truck age column to our Truck table. 
 
 Being a great developer, we know we cannot develop against a Production table, so
 we first need to create a Development environment that mimics Production. 
----------------------------------------------------------------------------------*/

-- Section 3: Step 1 - Create a Clone of Production
USE ROLE tasty_dev;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes.raw_pos.truck_dev 
    CLONE frostbyte_tasty_bytes.raw_pos.truck;

      
/*----------------------------------------------------------------------------------
Quickstart Section 4: Testing Snowflakes Query Result Set Cache 

 With our Zero Copy Clone, instantly available we can now begin to develop against 
 it without any fear of impacting production. However, before we make any changes
 let's first run some simple queries against it and test out Snowflakes
 Result Set Cache.
----------------------------------------------------------------------------------*/

-- Section 4: Step 1 - Querying our Cloned Table
USE WAREHOUSE tasty_dev_wh;

SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;


-- Section 4: Step 2 - Re-Running our Query
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM frostbyte_tasty_bytes.raw_pos.truck_dev t
ORDER BY t.truck_id;

    
/*----------------------------------------------------------------------------------
Quickstart Section 5: Updating Data and Calculating Food Truck Ages

 Based on our output above we first need to address the typo in those Ford_ records
 we saw in our `make` column. From there, we can begin to work on our calculation
 that will provide us with the age of each truck.
----------------------------------------------------------------------------------*/

-- Section 5: Step 1 - Updating Incorrect Values in a Column
UPDATE frostbyte_tasty_bytes.raw_pos.truck_dev 
SET make = 'Ford' 
WHERE make = 'Ford_';


-- Section 5: Step 2 - Building an Age Calculation
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model,
    (YEAR(CURRENT_DATE()) - t.year) AS truck_age_year
FROM frostbyte_tasty_bytes.raw_pos.truck_dev t;


/*----------------------------------------------------------------------------------
Quickstart Section 6: Adding a Column and Updating it

 With our Truck Age in Years calculation done and dusted, let's now add a new column
 to our cloned table to support it and finish things off by updating the column to
 reflect the calculated values.
----------------------------------------------------------------------------------*/

-- Section 6: Step 1 - Adding a Column to a Table
ALTER TABLE frostbyte_tasty_bytes.raw_pos.truck_dev
    ADD COLUMN truck_age NUMBER(4);


-- Section 6: Step 2 - Adding Calculate Values to our Column
UPDATE frostbyte_tasty_bytes.raw_pos.truck_dev t
    SET truck_age = (YEAR(CURRENT_DATE()) / t.year);


-- Section 6: Step 3 - Querying our new Column
SELECT
    t.truck_id,
    t.year,
    t.truck_age
FROM frostbyte_tasty_bytes.raw_pos.truck_dev t;


/*----------------------------------------------------------------------------------
Quickstart Section 7: Utilizing Time Travel for Data Disaster Recovery

 Although we made an mistake, Snowflake has many features that can help get us out
 of trouble here. The process we will take will leverage Query History, SQL Variables
 and Time Travel to revert our `truck_dev` table back to what it looked like prior
 to that incorrect update statement.
----------------------------------------------------------------------------------*/

-- Section 7: Step 1 - Leveraging Query History
SELECT 
    query_id,
    query_text,
    user_name,
    query_type,
    start_time
FROM TABLE(frostbyte_tasty_bytes.information_schema.query_history())
WHERE 1=1
    AND query_type = 'UPDATE'
    AND query_text LIKE '%frostbyte_tasty_bytes.raw_pos.truck_dev%'
ORDER BY start_time DESC;


-- Section 7: Step 2 - Setting a SQL Variable
SET query_id = 
(
    SELECT TOP 1 query_id
    FROM TABLE(frostbyte_tasty_bytes.information_schema.query_history())
    WHERE 1=1
        AND query_type = 'UPDATE'
        AND query_text LIKE '%SET truck_age = (YEAR(CURRENT_DATE()) / t.year);'
    ORDER BY start_time DESC
);


-- Section 7: Step 3 - Leveraging Time-Travel to Revert our Table
CREATE OR REPLACE TABLE frostbyte_tasty_bytes.raw_pos.truck_dev
    AS 
SELECT * FROM frostbyte_tasty_bytes.raw_pos.truck_dev
BEFORE(STATEMENT => $query_id); 


/*----------------------------------------------------------------------------------
Quickstart Section 8: Utilizing Time Travel for Data Disaster Recovery

 Although we made an mistake, Snowflake has many features that can help get us out
 of trouble here. The process we will take will leverage Query History, SQL Variables
 and Time Travel to revert our `truck_dev` table back to what it looked like prior
 to that incorrect update statement.
----------------------------------------------------------------------------------*/


-- Section 8: Step 1 - Adding Correctly Calculated Values to our Column
UPDATE frostbyte_tasty_bytes.raw_pos.truck_dev t
SET truck_age = (YEAR(CURRENT_DATE()) - t.year);


-- Section 8: Step 2 - Swapping our Development Table with Production
USE ROLE sysadmin;

ALTER TABLE frostbyte_tasty_bytes.raw_pos.truck_dev 
    SWAP WITH frostbyte_tasty_bytes.raw_pos.truck;


-- Section 8: Step 3 - Validate Production
SELECT
    t.truck_id,
    t.year,
    t.truck_age
FROM frostbyte_tasty_bytes.raw_pos.truck t
WHERE t.make = 'Ford';


/*----------------------------------------------------------------------------------
Quickstart Section 9: Dropping and Undropping Tables

 We can officially say our developer has completed their assigned task successfully.
 With the truck_age column in place and correctly calulated, our sysadmin can 
 clean up the left over tables to finish things off.
----------------------------------------------------------------------------------*/

-- Section 9: Step 1 - Dropping a Table
DROP TABLE frostbyte_tasty_bytes.raw_pos.truck;


-- Section 9: Step 2 - Undropping a Table
UNDROP TABLE frostbyte_tasty_bytes.raw_pos.truck;


-- Section 9: Step 3 - Dropping the Correct Table
DROP TABLE frostbyte_tasty_bytes.raw_pos.truck_dev;




/**********************************************************************/
/*------               Quickstart Reset Scripts                 ------*/
/*------   These can be ran to reset your account to a state    ------*/
/*----- that will allow you to run through this Quickstart again -----*/
/**********************************************************************/
USE ROLE accountadmin;
UPDATE frostbyte_tasty_bytes.raw_pos.truck SET make = 'Ford_' WHERE make = 'Ford';
ALTER TABLE frostbyte_tasty_bytes.raw_pos.truck DROP COLUMN truck_age;
UNSET query_id;
 