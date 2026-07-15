// Set context with appropriate privileges 
USE ROLE PC_HIGHTOUCH_ROLE;
USE DATABASE PC_HIGHTOUCH_DB;
USE SCHEMA PUBLIC;
// Create tables for demo
CREATE TABLE IF NOT EXISTS events (
    user_id VARCHAR(16777216),
    product_id VARCHAR(16777216),
    event_type VARCHAR(16777216),
    timestamp DATE,
    quantity INT,
    price NUMBER(38,2),
    category VARCHAR(16777216)
);
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR,
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    email VARCHAR(16777216),
    gender VARCHAR(16777216),
    birthday DATE,
    city VARCHAR(16777216),
    state VARCHAR(16777216),
    phone VARCHAR(16777216)
);

// Create file format 
CREATE OR REPLACE FILE FORMAT mycsvformat
   TYPE = 'CSV'
   FIELD_DELIMITER = ','
   SKIP_HEADER = 1;

// Create external stage
CREATE OR REPLACE STAGE my_csv_stage 
  FILE_FORMAT = mycsvformat
  URL = 's3://snow-ht-hol' credentials=(AWS_KEY_ID='AKIAXBUYSSFICPVETY4O'           AWS_SECRET_KEY='ud/lrc0Hcr0+T4xnkMTW74yzST2GinST3cZG2/I7') ;

// Run COPY INTO commands to load .csvs into Snowflake
COPY INTO events
FROM @my_csv_stage/snowflake_hol_events.csv
FILE_FORMAT = (TYPE = CSV)
ON_ERROR = 'continue';

COPY INTO users
FROM @my_csv_stage/snowflake_hol_users.csv
FILE_FORMAT = (TYPE = CSV)
ON_ERROR = 'continue';

// Option to test tables
select * from events;
select * from users;
