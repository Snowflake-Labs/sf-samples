-- Create a separate database and warehouse for demo
CREATE OR REPLACE DATABASE demo;
CREATE OR REPLACE WAREHOUSE demo_wh;
USE DATABASE demo;
USE WAREHOUSE demo_wh;

-- External Volume named s3_vol created in advance
CREATE OR REPLACE EXTERNAL VOLUME s3_vol
    STORAGE_LOCATIONS = 
        (
            (
                NAME = '<your-name>'
                STORAGE_PROVIDER = 'S3'
                STORAGE_BASE_URL = 's3://<bucket>/'
                STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<role-id>:role/<role-name>'
            )
        );

-- Create a Snowflake-managed Iceberg table that writes to Amazon S3
CREATE OR REPLACE ICEBERG TABLE demo.public.product_reviews (
    id STRING,
    product_name STRING,
    product_id STRING,
    reviewer_name STRING,
    review_date DATE,
    review STRING
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 's3_vol'
    BASE_LOCATION = 'demo/product_reviews/'
;
SELECT * FROM demo.public.product_reviews LIMIT 10;

-- Create a file format to specify how CSVs should be parsed
CREATE OR REPLACE FILE FORMAT csv_ff
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

-- Create a stage to store the CSV files
CREATE OR REPLACE STAGE demo.public.files
    FILE_FORMAT = csv_ff
    DIRECTORY = (ENABLED = TRUE);

-- Create a stream to capture new records in the Iceberg table
CREATE STREAM product_reviews_stream ON TABLE product_reviews;

-- Create task to process new records with Cortex sentiment LLM function
CREATE OR REPLACE TASK cortex_sentiment_score
    SCHEDULE = 'USING CRON 0 0 * * * America/Los_Angeles'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
UPDATE demo.public.product_reviews pr
SET sentiment = stream_sentiment
FROM (
    SELECT
        id,
        snowflake.cortex.sentiment(review) AS stream_sentiment
    FROM demo.public.product_reviews
) s
WHERE pr.id = s.id;

-- Use UI to create a reader account
-- Use UI to create a share with reader account and add both secure views to share

-- Query
-- For each product, what was the change in sentiment from January to February?
WITH jan AS (
    SELECT
        product_name,
        AVG(sentiment) AS avg_sentiment
    FROM shared_product_reviews.public.product_reviews
    WHERE MONTHNAME(review_date) = 'Jan'
    GROUP BY 1
)
, feb AS (
    SELECT
        product_name,
        AVG(sentiment) AS avg_sentiment
    FROM shared_product_reviews.public.product_reviews
    WHERE MONTHNAME(review_date) = 'Feb'
    GROUP BY 1
)
SELECT
    COALESCE(j.product_name, f.product_name) AS product_name,
    j.avg_sentiment AS jan_sentiment,
    f.avg_sentiment AS feb_sentiment,
    feb_sentiment - jan_sentiment AS sentiment_diff
FROM jan j
FULL OUTER JOIN feb f
    ON j.product_name = f.product_name
ORDER BY sentiment_diff DESC;

-- Reload
DELETE FROM demo.public.product_reviews
    WHERE REVIEW_DATE >= DATE('2024-02-01');

-- Cleanup
DROP DATABASE demo;
DROP WAREHOUSE demo_wh;
DROP EXTERNAL VOLUME s3_vol;
