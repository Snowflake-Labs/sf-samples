/*======================
CREATE EXTERNAL VOLUME
========================*/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL VOLUME amazon_reviews_iceberg_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'amazon-reviews-s3'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://amazon-product-reviews-dataset/curated/product_reviews/product_reviews_db/product_reviews_100k/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::849350360261:role/snowflake-iceberg-role'
      STORAGE_AWS_EXTERNAL_ID = 'snowflake_iceberg_ext_id'
    )
  )
  ALLOW_WRITES = TRUE;

  DESC EXTERNAL VOLUME amazon_reviews_iceberg_volume;

  SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('amazon_reviews_iceberg_volume');

/*=========================
CREATE CATALOG INTEGRATION
===========================*/

  CREATE OR REPLACE CATALOG INTEGRATION amazon_reviews_catalog_int
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

/*===================
CREATE ICEBERG TABLE
=====================*/

  CREATE OR REPLACE ICEBERG TABLE product_reviews_iceberg
  EXTERNAL_VOLUME = 'amazon_reviews_iceberg_volume'
  CATALOG = 'amazon_reviews_catalog_int'
  METADATA_FILE_PATH = 'metadata/00000-00a5b7be-48a2-4966-918f-99af062f8e85.metadata.json';

/*===================
QUERY YOUR DATA
=====================*/

  SELECT * FROM product_reviews_iceberg LIMIT 10;

  SELECT COUNT(*) AS total_reviews,
       ROUND(AVG(OVERALL), 2) AS avg_rating,
       COUNT(DISTINCT ASIN) AS unique_products,
       COUNT(DISTINCT REVIEWERID) AS unique_reviewers
  FROM product_reviews_iceberg;

  