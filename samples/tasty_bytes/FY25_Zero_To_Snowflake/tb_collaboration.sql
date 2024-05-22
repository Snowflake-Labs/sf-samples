/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Collaboration
Version:      v2
Script:       tb_fy25_collaboration.sql         
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
Collaboration
    1 - Investigating Days with Zero Sales
    2 - Acquiring Weather Source Data from the Snowflake Marketplace 
    3 - Democratizing First and Third Party Data
    4 - Using Harmonized Data to Answer Questions from the Business
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Investigating Days with Zero Sales

 Tasty Bytes Financial Analysts have let the business know that there are Trucks
 in the Raw Point of Sales System Data that are missing Sales for various days.
 
 Within this vignette, we will investigate these missing days and leverage the
 Snowflake Marketplace to enhance our analysis with Weather and Location data.

 Within this step, we dive into one example where Hamburg, Germany
 Trucks were flagged as having zero sales for a few days in February 2022.
----------------------------------------------------------------------------------*/

-- before we begin, let's set our Role, Warehouse and Database context
USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;


-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"medium":"quickstart", "source":"tastybytes", "vignette": "collaboration"}}';


-- let's first look for the days our Analysts flagged in Hamburg, Germany
WITH _feb_date_dim AS 
    (
    SELECT DATEADD(DAY, SEQ4(), '2022-02-01') AS date FROM TABLE(GENERATOR(ROWCOUNT => 28))
    )
SELECT
    fdd.date,
    ZEROIFNULL(SUM(o.price)) AS daily_sales
FROM _feb_date_dim fdd
LEFT JOIN analytics.orders_v o
    ON fdd.date = DATE(o.order_ts)
    AND o.country = 'Germany'
    AND o.primary_city = 'Hamburg'
WHERE fdd.date BETWEEN '2022-02-01' AND '2022-02-28'
GROUP BY fdd.date
ORDER BY fdd.date ASC;


/*----------------------------------------------------------------------------------
Step 2 - Acquiring Weather Source Data from the Snowflake Marketplace 

From what we saw above, it looks like we are missing sales for February 16th
through February 21st for Hamburg. Within our first party data there is not
much else we can use to investigate what might have caused this.

However, with live data sets and native applications available in the Snowflake
Marketplace we can immediately add Weather Metrics to our analysis and determine
if severe weather may have been the root cause.

Within this step, we will retrieve a free, public listing provided by Weather Source,
who is a leading provider of global weather and climate data.
----------------------------------------------------------------------------------*/

/*--
 To begin, acquire the listing by following the steps below within Snowsight:
    1 - In the bottom left corner, ensure you are operating as ACCOUNTADMIN
    2 - In the left pane, navigate to 'Data Products' (Cloud Icon) and select 'Marketplace'
    3 - In the search bar, enter: 'Weather Source frostbyte'
    4 - Select the 'Weather Source LLC: frostbyte' listing and click 'Get'
    5 - Adjust Database name to:'TB_WEATHERSOURCE'
    6 - Grant access to: 'PUBLIC'
--*/

-- let's now create a Daily Weather View in our Harmonized schema that joins Daily Weather History
-- to our Country dimension table using the Country and City columns
CREATE OR REPLACE VIEW harmonized.daily_weather_v
COMMENT = 'Weather Source Daily History filtered to Tasty Bytes supported Cities'
    AS
SELECT
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM tb_weathersource.onpoint_id.history_day hd
JOIN tb_weathersource.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN tb_101.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;


-- using our Daily Weather History View, let's find the Average Daily Weather Temperature for
-- Hamburg in February 2022 and visualize it as a Line Chart
    --> Chart Type: Line | X-Axis: DATE_VALID_STD(none) | Line: AVG_TEMPERATURE_AIR_2M_F(none)
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS avg_temperature_air_2m_f
FROM harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2' -- February
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


-- based on our results above, it does not seem like Temperature was the cause, let's also
-- check if Precipitation or Wind could have been a factor
        --> Chart Type: Line | X-Axis: DATE | Line: MAX_WIND_SPEED_100M_MPH(none)
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc IN ('Germany')
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2' -- February
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


/*----------------------------------------------------------------------------------
Step 3 - Democratizing First and Third Party Data

 In Step 2, we found that during the days that were flagged for Hamburg the city
 was experiencing abnormal Wind and Precipitation.

 Within this step, we will generate additional metrics and promote a View combining
 Sales and Weather Data to our Analytics layer where our users can begin to
 immediately leverage it for analysis.
----------------------------------------------------------------------------------*/

-- let's first create 2 SQL conversion functions to support our global user base
--> 1) Convert Fahrenheit to Celsius
CREATE OR REPLACE FUNCTION analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
RETURNS NUMBER(35,4)
AS
$$
    (temp_f - 32) * (5/9)
$$;


--> 2) Convert Inches to Millimeters
CREATE OR REPLACE FUNCTION analytics.inch_to_millimeter(inch NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;


-- to use in our View, let's first create the SQL required to generated Daily Sales and Weather for Hamburg, Germany
-- and ensure we use our SQL functions to provide converted metrics as well
SELECT
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM harmonized.daily_weather_v fd
LEFT JOIN harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
    AND fd.country_desc = 'Germany'
    AND fd.city = 'Hamburg'
    AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
ORDER BY fd.date_valid_std ASC;


-- the metrics above would have been a game changer for our Financial Analysts
-- when they were first diving in. let's now create our Analytics View
-- so that all appropriately privileged users can leverage it
CREATE OR REPLACE VIEW analytics.daily_city_metrics_v
COMMENT = 'Daily Weather Metrics and Orders Data'
    AS
SELECT
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(tb_101.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(tb_101.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM tb_101.harmonized.daily_weather_v fd
LEFT JOIN tb_101.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;


/*----------------------------------------------------------------------------------
Step 4 - Using Harmonized Data to Answer Questions from the Business

 With Harmonized Sales and Weather Data now available in our Analytics layer, our
 users can begin to leverage this data to derive insights and ensure we are using
 data when making decisions.

 In this step, we will leverage our work so far to answer a few questions an
 analyst may receive from the business.
----------------------------------------------------------------------------------*/

-- What were the 2022 Sales in Hamburg on days with over 1/4 inch of Precipitation?
SELECT
    * EXCLUDE (city_name, country_desc)
FROM analytics.daily_city_metrics_v
WHERE 1=1
    AND country_desc = 'Germany'
    AND city_name = 'Hamburg'
    AND YEAR(date) = '2022' 
    AND avg_precipitation_inches > 0.25
ORDER BY date DESC;


-- What Cities in the United States have had sales on days with temperatures below freezing?
SELECT  
    city_name AS city,
    ARRAY_AGG(date) AS date_array
FROM analytics.daily_city_metrics_v
WHERE 1=1
    AND avg_temperature_fahrenheit <= 32
    AND daily_sales > 0
    AND country_desc = 'United States'
    AND YEAR(date) >= 2022
GROUP BY city;


/*----------------------------------------------------------------------------------
 Reset Scripts 
 
  Run the scripts below to reset your account to the state required to re-run
  this vignette.
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- drop Harmonized View
DROP VIEW IF EXISTS tb_101.harmonized.daily_weather_v;

-- drop Analytics View
DROP VIEW IF EXISTS tb_101.analytics.daily_city_metrics_v;

-- drop Fahrenheit to Celsius Function
DROP FUNCTION IF EXISTS tb_101.analytics.fahrenheit_to_celsius(NUMBER(35,4));

-- drop Inch to Millimeter Function
DROP FUNCTION IF EXISTS tb_101.analytics.inch_to_millimeter(NUMBER(35,4));

-- uncomment and run the below query if you want to drop Weather Source Marketplace DB
-- DROP DATABASE IF EXISTS tb_weathersource;