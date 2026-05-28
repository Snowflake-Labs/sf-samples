-- TPCx-AI UC9 — Snowflake data load
--
-- Loads CUSTOMER_IMAGES from external stage into internal stage,
-- ready for distributed PyTorch training notebook.
--
-- Prerequisites:
--   1. Generate UC9 images with the official TPCx-AI kit (PDGF):
--        https://www.tpc.org/tpcx-ai/
--      You need CUSTOMER_IMAGES/ directory with subdirectories per identity
--      (ImageFolder format):
--        CUSTOMER_IMAGES/
--          ├── identity_001/
--          │   ├── sample_001.png
--          │   ├── sample_002.png
--          │   └── ...
--          ├── identity_002/
--          └── ...
--
--   2. Upload to your S3 bucket:
--        s3://YOUR_BUCKET/tpcxai/uc9/CUSTOMER_IMAGES/
--
--   3. Replace YOUR_BUCKET, YOUR_AWS_KEY_ID, YOUR_AWS_SECRET_KEY below.
--      For production, use a storage integration instead of inline credentials.
--
--   4. Run this file.
--
--   5. Training notebook will read from internal stage:
--        STAGE_PATH = "UC9_INTERNAL_STAGE/CUSTOMER_IMAGES/"

----------------------------------------------------------------------
-- Warehouse + compute pool (one-time)
----------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS BENCHMARK_LOAD_WH WAREHOUSE_SIZE = MEDIUM;
USE WAREHOUSE BENCHMARK_LOAD_WH;

-- Compute pool for distributed PyTorch training
CREATE COMPUTE POOL IF NOT EXISTS UC9_GPU_POOL
    MIN_NODES = 1 MAX_NODES = 8 INSTANCE_FAMILY = 'GPU_NV_M';
-- For CPU-only testing:
-- CREATE COMPUTE POOL IF NOT EXISTS UC9_CPU_POOL
--     MIN_NODES = 1 MAX_NODES = 8 INSTANCE_FAMILY = 'CPU_X64_L';

----------------------------------------------------------------------
-- Database + schema (one-time)
----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS TPCXAI_UC9;
USE DATABASE TPCXAI_UC9;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- External stage pointing to S3 where customer uploaded images
CREATE OR REPLACE STAGE UC9_EXTERNAL_STAGE
    URL = 's3://YOUR_BUCKET/tpcxai/uc9/'
    CREDENTIALS = (AWS_KEY_ID = 'YOUR_AWS_KEY_ID' AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY');

-- Verify external stage is accessible
LIST @UC9_EXTERNAL_STAGE;

----------------------------------------------------------------------
-- Internal stage for training
----------------------------------------------------------------------
-- Internal stage where images will be copied for training
CREATE OR REPLACE STAGE UC9_INTERNAL_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Copy images from external to internal stage
COPY FILES
    INTO @UC9_INTERNAL_STAGE/CUSTOMER_IMAGES/
    FROM @UC9_EXTERNAL_STAGE/CUSTOMER_IMAGES/
    ;

-- Verify copy succeeded
LIST @UC9_INTERNAL_STAGE/CUSTOMER_IMAGES/;


----------------------------------------------------------------------
-- Summary
----------------------------------------------------------------------
-- Data is now loaded into internal stage and ready for training.
--
-- To use in training notebook (uc9_pytorch_benchmark_snowflake.ipynb):
--
--   DATABASE = "TPCXAI_UC9"
--   SCHEMA = "PUBLIC"
--   STAGE_PATH = "UC9_INTERNAL_STAGE/CUSTOMER_IMAGES/"
