-- TPCx-AI UC8 — Snowflake data load
--
-- Loads ORDERS / LINEITEM / PRODUCT for scale factors 1, 10, 100, 1000 into
-- the TPCXAI_V2 database, ready for uc8_xgboost_benchmark.ipynb.
--
-- Prerequisites:
--   1. Generate UC8 CSVs with the official TPCx-AI kit:
--        https://www.tpc.org/tpcx-ai/
--      For each scale factor you want, you need order.csv, lineitem.csv, product.csv.
--   2. Upload them to your S3 bucket under:
--        s3://YOUR_BUCKET/tpcxai/uc08/sf<N>/
--   3. Replace YOUR_BUCKET, YOUR_AWS_KEY_ID, YOUR_AWS_SECRET_KEY below.
--      For production, prefer a storage integration over inline keys.
--   4. Run this file. To skip a scale factor, comment out its block.

----------------------------------------------------------------------
-- Warehouse + compute pool (one-time)
----------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS BENCHMARK_LOAD_WH WAREHOUSE_SIZE = MEDIUM;
USE WAREHOUSE BENCHMARK_LOAD_WH;

-- Compute pool used by the Container Runtime notebook (sized for SF1000).
CREATE COMPUTE POOL IF NOT EXISTS UC8_CPU_POOL
    MIN_NODES = 1 MAX_NODES = 12 INSTANCE_FAMILY = 'CPU_X64_M';
-- Optional: keep all 12 nodes warm during benchmarking.
-- ALTER COMPUTE POOL UC8_CPU_POOL SET MIN_NODES = 12;

----------------------------------------------------------------------
-- Database + external stage (one-time)
----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS TPCXAI_V2;
USE DATABASE TPCXAI_V2;
USE SCHEMA PUBLIC;

CREATE OR REPLACE STAGE UC08_EXTERNAL_STAGE
    URL = 's3://YOUR_BUCKET/tpcxai/uc08/'
    CREDENTIALS = (AWS_KEY_ID = 'YOUR_AWS_KEY_ID' AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY');

----------------------------------------------------------------------
-- SF1  (also creates CSV_FORMAT, reused by SF10 / SF100 / SF1000)
----------------------------------------------------------------------
USE DATABASE TPCXAI_V2;
CREATE SCHEMA IF NOT EXISTS SF1;
USE SCHEMA SF1;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

CREATE OR REPLACE TABLE ORDERS (
    O_ORDER_ID    INT,
    O_CUSTOMER_SK INT,
    WEEKDAY       VARCHAR,
    O_ORDER_DATE  DATE,
    STORE         INT,
    TRIP_TYPE     INT
);
CREATE OR REPLACE TABLE LINEITEM (
    LI_ORDER_ID   INT,
    LI_PRODUCT_ID INT,
    QUANTITY      INT,
    PRICE         FLOAT
);
CREATE OR REPLACE TABLE PRODUCT (
    P_PRODUCT_ID INT,
    NAME         VARCHAR,
    DEPARTMENT   VARCHAR
);

COPY INTO ORDERS   FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf1/order.csv    FILE_FORMAT = CSV_FORMAT;
COPY INTO LINEITEM FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf1/lineitem.csv FILE_FORMAT = CSV_FORMAT;
COPY INTO PRODUCT  FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf1/product.csv  FILE_FORMAT = CSV_FORMAT;

SELECT 'orders' AS tbl, COUNT(*) AS cnt FROM ORDERS
UNION ALL SELECT 'lineitem', COUNT(*) FROM LINEITEM
UNION ALL SELECT 'product',  COUNT(*) FROM PRODUCT;

----------------------------------------------------------------------
-- SF10
----------------------------------------------------------------------
USE DATABASE TPCXAI_V2;
CREATE SCHEMA IF NOT EXISTS SF10;
USE SCHEMA SF10;

CREATE OR REPLACE TABLE ORDERS (
    O_ORDER_ID INT, O_CUSTOMER_SK INT, WEEKDAY VARCHAR,
    O_ORDER_DATE DATE, STORE INT, TRIP_TYPE INT
);
CREATE OR REPLACE TABLE LINEITEM (
    LI_ORDER_ID INT, LI_PRODUCT_ID INT, QUANTITY INT, PRICE FLOAT
);
CREATE OR REPLACE TABLE PRODUCT (
    P_PRODUCT_ID INT, NAME VARCHAR, DEPARTMENT VARCHAR
);

COPY INTO ORDERS   FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf10/order.csv    FILE_FORMAT = SF1.CSV_FORMAT;
COPY INTO LINEITEM FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf10/lineitem.csv FILE_FORMAT = SF1.CSV_FORMAT;
COPY INTO PRODUCT  FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf10/product.csv  FILE_FORMAT = SF1.CSV_FORMAT;

SELECT 'orders' AS tbl, COUNT(*) AS cnt FROM ORDERS
UNION ALL SELECT 'lineitem', COUNT(*) FROM LINEITEM
UNION ALL SELECT 'product',  COUNT(*) FROM PRODUCT;

----------------------------------------------------------------------
-- SF100
----------------------------------------------------------------------
USE DATABASE TPCXAI_V2;
CREATE SCHEMA IF NOT EXISTS SF100;
USE SCHEMA SF100;

CREATE OR REPLACE TABLE ORDERS (
    O_ORDER_ID INT, O_CUSTOMER_SK INT, WEEKDAY VARCHAR,
    O_ORDER_DATE DATE, STORE INT, TRIP_TYPE INT
);
CREATE OR REPLACE TABLE LINEITEM (
    LI_ORDER_ID INT, LI_PRODUCT_ID INT, QUANTITY INT, PRICE FLOAT
);
CREATE OR REPLACE TABLE PRODUCT (
    P_PRODUCT_ID INT, NAME VARCHAR, DEPARTMENT VARCHAR
);

COPY INTO ORDERS   FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf100/order.csv    FILE_FORMAT = SF1.CSV_FORMAT;
COPY INTO LINEITEM FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf100/lineitem.csv FILE_FORMAT = SF1.CSV_FORMAT;
COPY INTO PRODUCT  FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf100/product.csv  FILE_FORMAT = SF1.CSV_FORMAT;

SELECT 'orders' AS tbl, COUNT(*) AS cnt FROM ORDERS
UNION ALL SELECT 'lineitem', COUNT(*) FROM LINEITEM
UNION ALL SELECT 'product',  COUNT(*) FROM PRODUCT;

----------------------------------------------------------------------
-- SF1000
----------------------------------------------------------------------
USE DATABASE TPCXAI_V2;
CREATE SCHEMA IF NOT EXISTS SF1000;
USE SCHEMA SF1000;

CREATE OR REPLACE TABLE ORDERS (
    O_ORDER_ID INT, O_CUSTOMER_SK INT, WEEKDAY VARCHAR,
    O_ORDER_DATE DATE, STORE INT, TRIP_TYPE INT
);
CREATE OR REPLACE TABLE LINEITEM (
    LI_ORDER_ID INT, LI_PRODUCT_ID INT, QUANTITY INT, PRICE FLOAT
);
CREATE OR REPLACE TABLE PRODUCT (
    P_PRODUCT_ID INT, NAME VARCHAR, DEPARTMENT VARCHAR
);

COPY INTO ORDERS   FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf1000/order.csv    FILE_FORMAT = SF1.CSV_FORMAT;
COPY INTO LINEITEM FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf1000/lineitem.csv FILE_FORMAT = SF1.CSV_FORMAT;
COPY INTO PRODUCT  FROM @PUBLIC.UC08_EXTERNAL_STAGE/sf1000/product.csv  FILE_FORMAT = SF1.CSV_FORMAT;

SELECT 'orders' AS tbl, COUNT(*) AS cnt FROM ORDERS
UNION ALL SELECT 'lineitem', COUNT(*) FROM LINEITEM
UNION ALL SELECT 'product',  COUNT(*) FROM PRODUCT;
