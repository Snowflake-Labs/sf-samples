/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Geospatial
Version:      v2
Script:       tb_fy25_geospatial.sql         
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
Geospatial
    1 - Acquiring Safegraph POI Data from the Snowflake Marketplace
    2 - Harmonizing and Promoting First and Third Party Data
    3 - Creating Geography Points from Latitude and Longitude 
    4 - Calculating Straight Line Distance between Points
    5 - Collecting Coordinates, Creating a Bounding Polygon & Finding its Center Point
    6 - Finding Locations Furthest Away from our Top Selling Hub
    7 - Geospatial Analysis with H3 (Hexagonal Hierarchical Geospatial Indexing System)
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Acquiring Safegraph POI Data from the Snowflake Marketplace

 Tasty Bytes operates Food Trucks in numerous cities and countries across the
 globe with each truck having the ability to choose two different selling locations
 per day. One important item that our Executives are interested in is to learn
 more about how these locations relate to each other as well as if there are any
 locations we currently serve that are potentially too far away from top selling
 city centers.

 Unfortunately what we have seen so far is our first party data does not give us
 the building blocks required to complete this sort of Geospatial analysis.
 
 Thankfully, the Snowflake Marketplace has great listings from Safegraph that 
 can assist us here.
----------------------------------------------------------------------------------*/

/*--
 To begin, acquire the SafeGraph listing by following the steps below within Snowsight:
    1 - In the bottom left corner, ensure you are operating as ACCOUNTADMIN
    2 - In the left pane, navigate to 'Data Products' (Cloud Icon) and select 'Marketplace'
    3 - In the search bar, enter: 'SafeGraph: frostbyte'
    4 - Select the 'SafeGraph: frostbyte' listing and click 'Get'
    5 - Adjust Database name to:'TB_SAFEGRAPH'
    6 - Grant access to: 'PUBLIC'
--*/


-- with our SafeGraph Marketplace listing in place, let's set our Role, Warehouse and Database context
USE ROLE sysadmin;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;


-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit","name":"tb_zts,"version":{"major":1, "minor":1},"attributes":{"medium":"quickstart", "source":"tastybytes", "vignette": "geospatial"}}';


-- to explore our newly acquired third party data, let's find all Museums in Paris, France
SELECT 
    cpg.placekey,
    cpg.location_name,
    cpg.longitude,
    cpg.latitude,
    cpg.street_address,
    cpg.city,
    cpg.country,
    cpg.polygon_wkt
FROM tb_safegraph.public.frostbyte_tb_safegraph_s cpg
WHERE 1=1
    AND cpg.top_category = 'Museums, Historical Sites, and Similar Institutions'
    AND cpg.sub_category = 'Museums'
    AND cpg.city = 'Paris'
    AND cpg.country = 'France';


/*----------------------------------------------------------------------------------
Step 2 - Harmonizing and Promoting First and Third Party Data

 To make our Geospatial analysis seamless, let's make sure to get Safegraph POI
 data included in the analytics.orders_v so all of our downstream users can
 also access it.
----------------------------------------------------------------------------------*/

-- add SafeGraph columns to our Analytics Orders_V view
CREATE OR REPLACE VIEW analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT 
    DATE(o.order_ts) AS date,
    o.* ,
    cpg.* EXCLUDE (location_id, region, phone_number, country)
FROM tb_101.harmonized.orders_v o
JOIN tb_safegraph.public.frostbyte_tb_safegraph_s cpg
    ON o.location_id = cpg.location_id;


/*----------------------------------------------------------------------------------
Step 3 - Creating Geography Points from Latitude and Longitude 

 To begin, we will first conduct high level sales analysis in Paris and prepare
 for our analysis by creating Points from our Coordinates.
----------------------------------------------------------------------------------*/

-- assume our Data Engineer role
USE ROLE tb_data_engineer;

-- let's now create Geography Points for these Top Selling Locations in Paris
SELECT TOP 10
    location_id,
    ST_MAKEPOINT(longitude, latitude) AS geo_point,
    SUM(price) AS total_sales_usd
FROM analytics.orders_v
WHERE primary_city = 'Paris'
GROUP BY location_id, latitude, longitude
ORDER BY total_sales_usd DESC;


/*----------------------------------------------------------------------------------
Step 4 - Calculating Straight Line Distance between Points

 Leveraging the Geography Points from above, we can now begin to use native 
 Geospatial functions. Within this step, we will calculate distance from our
 locations in Miles and Kilometers
----------------------------------------------------------------------------------*/

-- let's wrap our previous query in a Window Function and calculate the Distance
-- between the Top Selling Locations in Paris converting to Miles and Kilometers
WITH _top_10_locations AS
(
    SELECT TOP 10
        location_id,
        ST_MAKEPOINT(longitude, latitude) AS geo_point,
        SUM(price) AS total_sales_usd
    FROM analytics.orders_v
    WHERE primary_city = 'Paris'
    GROUP BY location_id, latitude, longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    a.location_id,
    b.location_id,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1609,2) AS geography_distance_miles,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1000,2) AS geography_distance_kilometers
FROM _top_10_locations a
JOIN _top_10_locations b
    ON a.location_id <> b.location_id -- avoid calculating the distance between the point itself
QUALIFY a.location_id <> LAG(b.location_id) OVER (ORDER BY geography_distance_miles) -- avoid duplicate: a to b, b to a distances
ORDER BY geography_distance_miles;

    /**
     ST_DISTANCE: Returns the minimum geodesic distance or minimum Euclidean distance between two Geography or Geometry objects.
    **/

    
/*----------------------------------------------------------------------------------
Step 5 - Collecting Coordinates, Creating a Bounding Polygon & Finding its Center Point
 
 Having just calculated distance, within this step, we will collect Coordinates for the
 Top Selling Locations in Paris. From there we will construct a Minimum Bounding Polygon,
 calculate the Area of it and determine the Coordinates of its Center Point.
----------------------------------------------------------------------------------*/

-- let's create a Geographic Collection of Points and build our Minimum Bounding Polygon
WITH _top_10_locations AS
(
    SELECT TOP 10
        location_id,
        ST_MAKEPOINT(longitude, latitude) AS geo_point,
        SUM(price) AS total_sales_usd
    FROM analytics.orders_v
    WHERE primary_city = 'Paris'
    GROUP BY location_id, latitude, longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    ST_NPOINTS(ST_COLLECT( geo_point)) AS count_points_in_collection,
    ST_COLLECT(geo_point) AS collection_of_points,
    ST_ENVELOPE(collection_of_points) AS minimum_bounding_polygon,
    ROUND(ST_AREA(minimum_bounding_polygon)/1000000,2) AS area_in_sq_kilometers
FROM _top_10_locations;

    /**
      ST_NPOINTS: Returns the number of points in a Geography or Geometry object.
      ST_COLLECT: This function combines all the Geography objects in a column into one Geometry object.
      ST_ENVELOPE: Returns the minimum bounding box that encloses a specified Geography or Geometry object.
      ST_AREA: Returns the area of the Polygon(s) in a Geography or Geometry object.
    **/

-- now let's find the Geometric Center Point for these key locations
WITH _top_10_locations AS
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM analytics.orders_v o
    WHERE primary_city = 'Paris'
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    ST_COLLECT(tl.geo_point) AS collect_points,
    ST_CENTROID(collect_points) AS geometric_center_point
FROM _top_10_locations tl;

    /**
     ST_CENTROID: Returns the Point representing the geometric center of a Geography or Geometry object.
    **/

-- to assist in our next query, let's copy (CMD + C) the geometric_center_point result from above and SET it as a SQL Variable
SET center_point = '{   "coordinates": [     2.323399542234561e+00,     4.885933767767676e+01   ],   "type": "Point" }';


/*----------------------------------------------------------------------------------
Step 6 - Finding Locations Furthest Away from our Top Selling Hub

 In the last step, we were able to find the hot spot for our Top Selling
 Locations in Paris.
 
 Within this step, we will now use this Center Point and the Distance function
 to generate a list of the locations we serve furthest away from it. This list
 will be provided to the business for review to consider if certain locations
 should be removed from our routes.
----------------------------------------------------------------------------------*/

-- using our Variable, let's find the Top 50 Locations furthest away from our Top Selling Center Point in Paris
WITH _paris_locations AS
(
    SELECT DISTINCT
        location_id,
        location_name,
        ST_MAKEPOINT(longitude, latitude) AS geo_point
    FROM analytics.orders_v
    WHERE primary_city = 'Paris'
)
SELECT TOP 50
    location_id,
    location_name,
    ROUND(ST_DISTANCE(geo_point, TO_GEOGRAPHY($center_point))/1000,2) AS kilometer_from_top_selling_center
FROM _paris_locations
ORDER BY kilometer_from_top_selling_center DESC;


/*----------------------------------------------------------------------------------
Step 7 - Geospatial Analysis with H3 (Hexagonal Hierarchical Geospatial Indexing System)

 H3 is a way of dividing the Earth's surface into hexagonal shapes, organizing them into levels
 of resolution, and assigning unique codes to each hexagon for easy reference. 
 
 This system was created by Uber for tasks like mapping, location-based services, and market
 analysis. A lower resolution corresponds to larger hexagons covering broader areas, while a higher
 resolution means smaller hexagons representing more specific locations.

 As Snowflake offers H3 functionality, the Tasty Bytes Leadership Team has tasked our Data Engineer
 with generating H3 Codes and finding the Top Selling Hexagons.
----------------------------------------------------------------------------------*/

-- let's first locate H3 Codes in Integer and String form at Resolutions of 4,8 and 12 for the Louvre Museum in Paris
SELECT DISTINCT
    location_id,
    location_name,
    H3_LATLNG_TO_CELL(latitude, longitude, 4) AS h3_integer_resolution_4, 
    H3_LATLNG_TO_CELL_STRING(latitude, longitude, 4) AS h3_hex_resolution_4,
    H3_LATLNG_TO_CELL(latitude, longitude, 8) AS h3_integer_resolution_8, 
    H3_LATLNG_TO_CELL_STRING(latitude, longitude, 8) AS h3_hex_resolution_8,
    H3_LATLNG_TO_CELL(latitude, longitude, 12) AS h3_integer_resolution_12,
    H3_LATLNG_TO_CELL_STRING(latitude, longitude, 12) AS h3_hex_resolution_12 
            --> resolution 4 = 288 thousand hexagons covering the globe
            --> resolution 8 = 92 billion hexagons covering the globe
            --> resolution 12 = 1.6 billion hexagons covering the globe
FROM analytics.orders_v
WHERE location_name = 'Musee du Louvre';

    /**
     H3_LATLNG_TO_CELL: Returns the Integer value of the H3 cell ID for a given Latitude, Longitude, and Resolution.
     H3_LATLNG_TO_CELL_STRING: Returns the H3 cell ID in Hexadecimal format for a given Latitude, Longitude, and Resolution.
    **/

-- now we will locate the Parent Cell Id and an array of Children Cell ID's
SELECT DISTINCT
    location_id,
    location_name,
    H3_LATLNG_TO_CELL(latitude, longitude, 8) AS h3_int_resolution_8,
    H3_INT_TO_STRING(h3_int_resolution_8) AS h3_hex_resolution_8, -- convert above to hexadecimal format
    H3_CELL_TO_PARENT(h3_int_resolution_8, 5) AS h3_int_parent_resolution_5, -- locate resolution 5 parent of our H3 cell
    H3_CELL_TO_CHILDREN(h3_int_resolution_8, 10) AS h3_int_children_resolution_10 -- locate all children at resolution 10
FROM analytics.orders_v
WHERE location_name = 'Musee du Louvre';

    /**
     H3_CELL_TO_PARENT: Returns the ID of the parent of an H3 Cell for a given Resolution. 
     H3_INT_TO_STRING: Converts the Integer value of an H3 Cell to hexadecimal format.
     H3_CELL_TO_CHILDREN: Returns an Array of the Integer H3 Cell IDs of the children of an H3 cell for a given resolution.
    **/
    
-- what are the H3 Cell IDs at Resolution 6 for our Top 50 Selling Locations in Paris?
SELECT TOP 50
    location_id,
    location_name,
    SUM(price) AS total_sales_usd,
    H3_LATLNG_TO_CELL(latitude, longitude, 6) AS h3_integer_resolution_6,
    H3_LATLNG_TO_CELL_STRING(latitude, longitude, 6) AS h3_hex_resolution_6
FROM analytics.orders_v
WHERE primary_city = 'Paris'
GROUP BY ALL
ORDER BY total_sales_usd DESC;


-- wrapping our previous query in a Window function, let's retrieve a distinct list
-- of H3 Cell IDs with Resolution 6 that include our Top 50 selling locations
WITH _top_50_locations AS
(
    SELECT TOP 50
        location_id,
        location_name,
        H3_LATLNG_TO_CELL(latitude, longitude, 6) AS h3_integer_resolution_6,
        H3_LATLNG_TO_CELL_STRING(latitude, longitude, 6) AS h3_hex_resolution_6,
        SUM(price) AS total_sales_usd
    FROM analytics.orders_v
    WHERE primary_city = 'Paris'
    GROUP BY ALL
    ORDER BY total_sales_usd DESC
)
SELECT  
    DISTINCT h3_hex_resolution_6
FROM _top_50_locations;


-- using those H3 Cell IDs, let's now produce a list of all locations within these Top Hexagons and the Hexagons Total Sales.
-- these results will help the business locate areas and locations that we should ensure are always occupied by our trucks
WITH _top_50_locations AS
(
    SELECT TOP 50
        location_id,
        ARRAY_SIZE(ARRAY_UNIQUE_AGG(customer_id)) AS customer_loyalty_visitor_count,
        H3_LATLNG_TO_CELL(latitude, longitude, 7) AS h3_integer_resolution_6,
        H3_LATLNG_TO_CELL_STRING(latitude, longitude, 7) AS h3_hex_resolution_6,
        SUM(price) AS total_sales_usd
    FROM analytics.orders_v
    WHERE primary_city = 'Paris'
    GROUP BY ALL
    ORDER BY total_sales_usd DESC
)
SELECT
    h3_hex_resolution_6,
    COUNT(DISTINCT location_id) AS number_of_top_50_locations,
    SUM(customer_loyalty_visitor_count) AS customer_loyalty_visitor_count,
    SUM(total_sales_usd) AS total_sales_usd
FROM _top_50_locations
GROUP BY ALL
ORDER BY total_sales_usd DESC;


-- to conclude, let's see if our two Top Selling Resolution 6 Hexagons border each other 
    --> NOTE: A neighbour cell is one step away and two cells with one hexagon between them are two steps apart.
SELECT H3_GRID_DISTANCE('871fb4671ffffff', '871fb4670ffffff') AS cell_distance;

    /**
     H3_GRID_DISTANCE: Returns the distance between two H3 cells specified by their IDs in terms of grid cells. 
    **/


/*----------------------------------------------------------------------------------
 Reset Scripts 
 
  Run the scripts below to reset your account to the state required to re-run
  this vignette.
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- unset Variable
UNSET center_point;

-- unset Query Tag
ALTER SESSION UNSET query_tag;

-- uncomment and run the below query if you want to drop Weather Source Marketplace DB
-- DROP DATABASE IF EXISTS tb_safegraph;