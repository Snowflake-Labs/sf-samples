USE ROLE ACCOUNTADMIN;
USE WAREHOUSE LLM_DEMO;
USE DATABASE LLM_DEMO;
USE SCHEMA SUMMIT;
CREATE OR REPLACE TABLE SUBSCRIBER
(
unique_id VARCHAR,
sign_up_date DATE,
top_genre VARCHAR,
sub_type VARCHAR,
hours_watched INT,
age INT,
gender VARCHAR,
viewer_type VARCHAR,
profiles INT,
price_plan VARCHAR,
plan_type VARCHAR,
active_flag VARCHAR,
cancel_date DATE,
country VARCHAR
);

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-200, -15, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,75,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 50 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 70 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 90 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 67 THEN 'US'
        WHEN UNIFORM(0, 100, random()) < 79 THEN 'UK'
        WHEN UNIFORM(0, 100, random()) < 89 THEN 'BR'
        ELSE 'CA'
    END AS country
FROM
    TABLE(GENERATOR(ROWCOUNT => 400000));

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-400, -15, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,55,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 62 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 60 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 80 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 62 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 45 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'US'
        WHEN UNIFORM(0, 100, random()) < 88 THEN 'UK'
        WHEN UNIFORM(0, 100, random()) < 95 THEN 'BR'
        ELSE 'CA'
    END AS country
FROM
    TABLE(GENERATOR(ROWCOUNT => 400000));

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-400, -15, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,65,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 82 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 60 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 80 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 62 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 55 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 91 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 89 THEN 'US'
        WHEN UNIFORM(0, 100, random()) < 97 THEN 'UK'
        ELSE 'BR'
    END AS country
FROM
    TABLE(GENERATOR(ROWCOUNT => 300000));

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-1000, -250, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,75,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 66 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 18 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 70 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 90 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    'US'
FROM
    TABLE(GENERATOR(ROWCOUNT => 300000));

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-1000, -250, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,55,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 55 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 18 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 70 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 90 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    'US'
FROM
    TABLE(GENERATOR(ROWCOUNT => 300000));

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-1000, -250, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,75,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 55 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 18 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 70 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 90 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    'US'
FROM
    TABLE(GENERATOR(ROWCOUNT => 300000));

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-1000, -500, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,45,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 55 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 18 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 70 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 90 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    'US'
FROM
    TABLE(GENERATOR(ROWCOUNT => 500000));

INSERT INTO SUBSCRIBER (unique_id, sign_up_date, top_genre, sub_type, age, gender, viewer_type, profiles, price_plan, plan_type, active_flag, country)
SELECT
    UUID_STRING() AS unique_id,
    DATEADD(day, uniform(-1000, -100, random()), current_date) AS sign_up_date,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 55 THEN 'Comedy'
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Drama'
        ELSE 'Action'
        END AS top_genre,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 25 THEN 'Switcher'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'New'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Price-Sensitive'
        ELSE 'Mature'
    END AS sub_type,
    UNIFORM(21,40,random()) AS age,
    CASE 
        WHEN UNIFORM(0,100,random()) > 55 THEN 'Male' ELSE 'Female' 
    END AS gender,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 18 THEN 'Binger'
        WHEN UNIFORM(0, 100, random()) < 40 THEN 'Daytime'
        WHEN UNIFORM(0, 100, random()) < 65 THEN 'Late Night'
        ELSE 'Standard'
    END AS viewer_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 70 THEN '1'
        WHEN UNIFORM(0, 100, random()) < 90 THEN '2'
        ELSE '3'
    END AS profiles,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Monthly'
        WHEN UNIFORM(0, 100, random()) < 85 THEN 'Annual'
        ELSE 'Promotional'
    END AS price_plan,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 75 THEN 'Ad-Free'
        ELSE 'Ad-Supported'
    END AS plan_type,
    CASE  
        WHEN UNIFORM(0, 100, random()) < 81 THEN 'Active'
        ELSE 'Cancelled'
    END AS plan_type,
    'US'
FROM
    TABLE(GENERATOR(ROWCOUNT => 500000));


-- creating our random hours watched metric
UPDATE SUBSCRIBER 
SET hours_watched = (CURRENT_DATE - sign_up_date) * 
    CASE 
        WHEN profiles = 1 THEN UNIFORM(0.1, 1, random())
        WHEN profiles = 2 THEN UNIFORM(0.35, 1, random()) -- Adjust the range for profiles = 2
        ELSE UNIFORM(0.6, 1, random()) -- Default case for other values
    END;


UPDATE SUBSCRIBER
SET cancel_date = DATEADD(day, uniform(30, 300, random()), sign_up_date)
WHERE active_flag = 'Cancelled';

UPDATE SUBSCRIBER
SET cancel_date = DATEADD(day, uniform(-75, -5, random()), current_date)
WHERE cancel_date > CURRENT_DATE();

UPDATE LLM_DEMO.SUMMIT.SUBSCRIBER
SET 
    ACTIVE_FLAG = 'Active',
    CANCEL_DATE = NULL
WHERE 
    SIGN_UP_DATE > CANCEL_DATE;


ALTER TABLE SUBSCRIBER ADD COLUMN TOTAL_REVENUE FLOAT;

UPDATE SUBSCRIBER
SET TOTAL_REVENUE = CASE
                WHEN plan_type = 'Ad-Free' THEN
                   DATEDIFF('day', sign_up_date, COALESCE(cancel_date, CURRENT_DATE())) / 30.0 * 7.99
                WHEN plan_type = 'Ad-Supported' THEN
                   (DATEDIFF('day', sign_up_date, COALESCE(cancel_date, CURRENT_DATE())) / 30.0 * 4.99)
                   + (hours_watched * 0.30)
                 ELSE 0  -- You can adjust this else condition as needed
              END;
SELECT * FROM SUBSCRIBER SAMPLE (50 ROWS);
