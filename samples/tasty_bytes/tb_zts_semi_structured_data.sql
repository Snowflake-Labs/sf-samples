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
Version:      v1
Script:       tb_zts_semi_structured_data.sql         
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
Quickstart Section 3 - Profiling our Menu Data
 As a Tasty Bytes Data Engineer, we have been tasked with profiling our Menu data 
 that includes a Semi-Structured Data column. From this menu table we need to produce
 an Analytics layer View that exposes Dietary and Ingredient data to our end users.
----------------------------------------------------------------------------------*/

-- Section 3: Step 1 - Setting our Context and Querying our Table
USE ROLE tasty_data_engineer;
USE WAREHOUSE tasty_de_wh;

SELECT TOP 10
    m.truck_brand_name,
    m.menu_type,
    m.menu_item_name,
    m.menu_item_health_metrics_obj
FROM frostbyte_tasty_bytes.raw_pos.menu m;


-- Section 3: Step 2 - Exploring our Semi-Structured Column
SHOW COLUMNS IN frostbyte_tasty_bytes.raw_pos.menu;


-- Section 3: Step 3 - Traversing Semi-Structured Data using Dot Notation
SELECT 
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    m.menu_item_health_metrics_obj:menu_item_health_metrics AS menu_item_health_metrics
FROM frostbyte_tasty_bytes.raw_pos.menu m;


/*----------------------------------------------------------------------------------
Quickstart Section 4 - Flattening Semi-Structured Data
 Having seen how we can easily query Semi-Structured Data as it exists in a Variant
 column using Dot Notation, our Tasty Data Engineer is well on the way to providing
 their internal stakeholders with the data they have requested.

 Within this section, we will conduct additional Semi-Structured Data processing 
 to meet requirements.
----------------------------------------------------------------------------------*/

-- Section 4: Step 1 - Introduction to Lateral Flatten
SELECT 
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients
FROM frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- Section 4: Step 2 - Exploring an Array Function
SELECT 
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients
FROM frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
WHERE ARRAY_CONTAINS('Lettuce'::VARIANT, obj.value:"ingredients"::VARIANT);


-- Section 4: Step 3 - Structuring Semi-Structured Data at Scale
SELECT 
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;
    

/*----------------------------------------------------------------------------------
Quickstart Section 5 - Creating Structured Views Over Semi-Structured Data

 In the last section, we constructed a query that provides the exact output our end
 users require using a suite of Snowflake Semi- Structured Data functionality along
 the way. Next we will follow the process of promoting this query against our Raw
 layer through Harmonized and eventually to Analytics where our end users are
 privileged to read from.
----------------------------------------------------------------------------------*/

-- Section 5: Step 1 - Creating our Harmonized View Using our Semi-Structured Flattening SQL
CREATE OR REPLACE VIEW frostbyte_tasty_bytes.harmonized.menu_v
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
FROM frostbyte_tasty_bytes.raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- Section 5: Step 2 - Promoting from Harmonized to Analytics with Ease
CREATE OR REPLACE VIEW frostbyte_tasty_bytes.analytics.menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including cost, price, ingredients and dietary restrictions'
    AS
SELECT 
    * 
    EXCLUDE (menu_type_id) --exclude MENU_TYPE_ID
    RENAME  (truck_brand_name AS brand_name) -- rename TRUCK_BRAND_NAME to BRAND_NAME
FROM frostbyte_tasty_bytes.harmonized.menu_v;


/*----------------------------------------------------------------------------------
Quickstart Section 6 - Analyzing Processed Semi-Structured Data in Snowsight

 With our Menu View available in our Analytics layer, let's execute a few queries
 against it that we will provide to our end users showcasing how Snowflake powers
 a relational query experience over Semi-Structured data without having to make
 additional copies or conduct any complex processing.
----------------------------------------------------------------------------------*/

-- Section 6: Step 1 - Analyzing Arrays
SELECT 
    m1.menu_type,
    m1.menu_item_name,
    m2.menu_type AS overlap_menu_type,
    m2.menu_item_name AS overlap_menu_item_name,
    ARRAY_INTERSECTION(m1.ingredients, m2.ingredients) AS overlapping_ingredients
FROM frostbyte_tasty_bytes.analytics.menu_v m1
JOIN frostbyte_tasty_bytes.analytics.menu_v m2
    ON m1.menu_item_id <> m2.menu_item_id -- avoid joining the same menu item to itself
    AND m1.menu_type <> m2.menu_type 
WHERE 1=1
    AND m1.item_category <> 'Beverage' -- remove beverages
    AND m2.item_category <> 'Beverage' -- remove beverages
    AND ARRAYS_OVERLAP(m1.ingredients, m2.ingredients) -- evaluates to TRUE if one ingredient is in both arrays
ORDER BY m1.menu_type;


-- Section 6: Step 2 - Providing Metrics to Executives
SELECT
    COUNT(DISTINCT menu_item_id) AS total_menu_items,
    SUM(CASE WHEN is_healthy_flag = 'Y' THEN 1 ELSE 0 END) AS healthy_item_count,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM frostbyte_tasty_bytes.analytics.menu_v m;


-- Section 6: Step 3 - Turning Results to Charts
SELECT
    m.brand_name,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM frostbyte_tasty_bytes.analytics.menu_v m
WHERE m.brand_name IN  ('Plant Palace', 'Peking Truck','Revenge of the Curds')
GROUP BY m.brand_name;




/**********************************************************************/
/*------               Quickstart Reset Scripts                 ------*/
/*------   These can be ran to reset your account to a state    ------*/
/*----- that will allow you to run through this Quickstart again -----*/
/**********************************************************************/
DROP VIEW IF EXISTS frostbyte_tasty_bytes.harmonized.menu_v;
DROP VIEW IF EXISTS frostbyte_tasty_bytes.analytics.menu_v;