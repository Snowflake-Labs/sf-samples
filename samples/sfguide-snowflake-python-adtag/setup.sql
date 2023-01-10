-- setup warehouse, database and schema
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE TAG_DEMO_WH WITH WAREHOUSE_SIZE='XSmall'  STATEMENT_TIMEOUT_IN_SECONDS=15 STATEMENT_QUEUED_TIMEOUT_IN_SECONDS=15;
USE WAREHOUSE TAG_DEMO_WH;
CREATE DATABASE TAG_DEMO;
CREATE SCHEMA TAG_DEMO.DEMO;

-- create table to hold impressions
use TAG_DEMO.DEMO;
create or replace table IMPRESSIONS
(event variant);

-- create a stage to take the data from S3
create or replace stage tag_demo_stage
url='s3://<s3_location>]/'
credentials = (AWS_KEY_ID='<accesskey>' AWS_SECRET_KEY='<secretkey>');

-- create the pipe to load it into the table
create or replace pipe tag_demo_pipe auto_ingest=true as
copy into IMPRESSIONS
from @tag_demo_stage
file_format = (type = 'JSON');

-- to get the notification channel ARN
show pipes;

-- verify that data is loading
select * from IMPRESSIONS;