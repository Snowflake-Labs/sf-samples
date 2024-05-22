/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Semi-Structured Data
Version:      v2
Script:       tb_fy25_semi_structured_data.sql         
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
Semi-Structured Data
    1 - Semi-Structured Data and the Variant Data Type
    2 - Querying Semi-Structured Data via Dot and Bracket Notation + Flatten
    3 - Providing Flattened Data to Business Users 
    4 - Analyzing Processed Semi-Structured Data in Snowsight
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Semi-Structured Data and the Variant Data Type

 As a Tasty Bytes Data Engineer, we have been tasked with profiling our Menu data and
 developing an Analytics layer View that exposes Dietary and Ingredient data to our
 downstream business users.
----------------------------------------------------------------------------------*/

-- first we must set our Role, Warehouse and Database context
USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;


-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit","name":"tb_zts,"version":{"major":1, "minor":1},"attributes":{"medium":"quickstart", "source":"tastybytes", "vignette": "semi_structured"}}';


-- let's take a look at a few columns in our Raw Menu table we are receiving from our
-- Point of Sales (POS) system so we can see where our Dietary and Ingredient data is stored
SELECT TOP 10
    truck_brand_name,
    menu_type,
    menu_item_name,
    menu_item_health_metrics_obj
FROM raw_pos.menu;


-- based on the results above, the data we need to provide downstream is stored in the
-- Menu Item Health Metrics Object column. let's now use a SHOW COLUMNS command to
-- investigate what Data Type this column is.
SHOW COLUMNS IN raw_pos.menu;

    /**
     Variant: Snowflake can convert data from JSON, Avro, ORC, or Parquet format to an internal hierarchy of ARRAY,
      OBJECT, and VARIANT data and store that hierarchical data directly into a VARIANT column.
    **/


/*----------------------------------------------------------------------------------
Step 2 - Querying Semi-Structured Data

 The data stored within our Variant, Menu Item Health Metrics Object, column is JSON.
 
 Within this step, we will leverage Snowflake's Native Semi-Structured Support,
 to query and flatten this column so that we can prepare to provide our downstream
 users with their requested data in an easy to understand, tabular format.
----------------------------------------------------------------------------------*/

-- to extract first-level elements from Variant columns, we can insert a Colon ":" between the Variant Column
-- name and first-level identifier. let's use this to extract Menu Item Id, and Menu Item Health Metrics Object
SELECT
    menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    menu_item_health_metrics_obj:menu_item_health_metrics AS menu_item_health_metrics
FROM raw_pos.menu;


/*--
 To convert Semi-Structured data to a relational representation we can use Flatten
 and to access elements from within a JSON object, we can use Dot or Bracket Notation.

 Let's now leverage both of these to extract our Ingredients into an Array Column.
--*/

--> Dot Notation and Lateral Flatten
SELECT
    m.menu_item_name,
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    obj.value:"ingredients"::ARRAY AS ingredients
FROM raw_pos.menu m, 
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
ORDER BY menu_item_id;


--> Bracket Notation and Lateral Flatten
SELECT
    m.menu_item_name,
    m.menu_item_health_metrics_obj['menu_item_id'] AS menu_item_id,
    obj.value['ingredients']::ARRAY AS ingredients
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
ORDER BY menu_item_id;

    /**
     Array: A Snowflake ARRAY is similar to an array in many other programming languages.
      An ARRAY contains 0 or more pieces of data. Each element is accessed by specifying
      its position in the array.
    **/
    
/*--
 To complete our Semi-Structured processing, let's extract the remaining Dietary Columns
 using both Dot and Bracket Notation alongside the Ingredients Array.
--*/

--> Dot Notation and Lateral Flatten
SELECT
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
--> Bracket Notation and Lateral Flatten
SELECT
    m.menu_item_health_metrics_obj['menu_item_id'] AS menu_item_id,
    m.menu_item_name,
    obj.value['ingredients']::VARIANT AS ingredients,
    obj.value['is_healthy_flag']::VARCHAR(1) AS is_healthy_flag,
    obj.value['is_gluten_free_flag']::VARCHAR(1) AS is_gluten_free_flag,
    obj.value['is_dairy_free_flag']::VARCHAR(1) AS is_dairy_free_flag,
    obj.value['is_nut_free_flag']::VARCHAR(1) AS is_nut_free_flag
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;


/*----------------------------------------------------------------------------------
Step 3 - Providing Flattened Data to Business Users 

 With all of the required data, extracted, flattened and available in tabular form,
 we will now work to provide access to our Business Users.

 Within this step, we will promote a full output of the Menu table with the flattened
 columns to Views in our Harmonized and Analytics layers.

 If you are more familiar with Medallion Architectures, we can think of the Harmonized
 layer as Silver and the Analytics layer as Gold.
----------------------------------------------------------------------------------*/

-- to begin, let's add columns to our previous Dot Notation query and leverage it within a new Menu View in our Harmonized layer
CREATE OR REPLACE VIEW harmonized.menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including Cost, Price, Ingredients and Dietary Restrictions'
    AS
SELECT
    m.menu_id,
    m.menu_type_id,
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    m.item_category,
    m.item_subcategory,
    m.cost_of_goods_usd,
    m.sale_price_usd,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- with the Harmonized View containing the flattening logic in place, let's now promote the data to the
-- Analytics Schema where our various Business Users will be able to access it
CREATE OR REPLACE VIEW analytics.menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including Cost, Price, Ingredients and Dietary Restrictions'
    AS
SELECT
    *
    EXCLUDE (menu_type_id) --exclude MENU_TYPE_ID
    RENAME (truck_brand_name AS brand_name) -- rename TRUCK_BRAND_NAME to BRAND_NAME
FROM harmonized.menu_v;

    /**
     Exclude: specifies the columns that should be excluded from the results of a SELECT * statement.
     Rename: specifies the column aliases that should be used in the results of a SELECT * statement.
    **/

-- before moving on, let's use our view to take a look at the results for our Better Off Bread brand
SELECT 
    brand_name,
    menu_item_name,
    sale_price_usd,
    ingredients,
    is_healthy_flag,
    is_gluten_free_flag,
    is_dairy_free_flag,
    is_nut_free_flag
FROM analytics.menu_v
WHERE brand_name = 'Better Off Bread';


-- the results look great, let's now grant our Developer the ability to query this View
GRANT SELECT ON analytics.menu_v TO ROLE tb_dev;


/*----------------------------------------------------------------------------------
Step 4 - Leveraging Array Functions

 With our Menu View available in our Analytics layer, let's now jump into the
 life of a Tasty Bytes Developer. Within this step, we will address questions from 
 the the Tasty Bytes Leadership Team related to our Food Truck Menu's.
 
 Along the way we will see how Snowflake can provide a relational query experience
 over Semi-Structured data without having to make additional copies or conduct any
 complex data transformations.
----------------------------------------------------------------------------------*/

-- to start this step, let's assume our Developer Role and use the Developer Warehouse
USE ROLE tb_dev;
USE WAREHOUSE tb_dev_wh;


-- with recent Lettuce recalls in the news, which of our Menu Items include this as an Ingredient?
SELECT
    m.menu_item_id,
    m.menu_item_name,
    m.ingredients
FROM analytics.menu_v m
WHERE ARRAY_CONTAINS('Lettuce'::VARIANT, m.ingredients);

    /**
     Array_contains: The function returns TRUE if the specified value is present in the array.
    **/

-- which Menu Items across Menu Types contain overlapping Ingredients and are those Ingredients?.
SELECT
    m1.brand_name,
    m1.menu_item_name,
    m2.brand_name AS overlap_brand,
    m2.menu_item_name AS overlap_menu_item_name,
    ARRAY_INTERSECTION(m1.ingredients, m2.ingredients) AS overlapping_ingredients
FROM analytics.menu_v m1
JOIN analytics.menu_v m2
    ON m1.menu_item_id <> m2.menu_item_id -- avoid joining the same menu item to itself
    AND m1.menu_type <> m2.menu_type
WHERE 1=1
    AND m1.item_category  <> 'Beverage' -- remove beverages
    AND ARRAYS_OVERLAP(m1.ingredients, m2.ingredients) -- return only those that overlap
ORDER BY ARRAY_SIZE(overlapping_ingredients) DESC; -- order by largest number of overlapping ingredients

    /**
     Array_intersection: Returns an array that contains the matching elements in the two input arrays.
     Arrays_overlap: Compares whether two arrays have at least one element in common
     Array_size: Returns the size of the input array
    **/

-- how many total Menu Items do we have and how many address Dietary Restrictions?
SELECT
    COUNT(DISTINCT menu_item_id) AS total_menu_items,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM analytics.menu_v m;


-- how do the Plant Palace, Peking Truck and Better Off Bread Brands compare to each other?
    --> Snowsight Chart Type: Bar | Orientation: 1st Option | Grouping: 1st Option
        --> Y-Axis: BRAND_NAME | Bars: GLUTEN_FREE_ITEM_COUNT, DAIRY_FREE_ITEM_COUNT, NUT_FREE_ITEM_COUNT
SELECT
    m.brand_name,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM analytics.menu_v m
WHERE m.brand_name IN ('Plant Palace', 'Peking Truck','Revenge of the Curds')
GROUP BY m.brand_name;


/*----------------------------------------------------------------------------------
 Reset Scripts 
 
  Run the scripts below to reset your account to the state required to re-run
  this vignette.
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- drop the Harmonized Menu View
DROP VIEW IF EXISTS tb_101.harmonized.menu_v;

-- drop the Analytics Menu View
DROP VIEW IF EXISTS tb_101.analytics.menu_v;
