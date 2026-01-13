USE LAKEHOUSE_VHOL.PUBLIC;
USE WAREHOUSE VHOL_XS;
USE ROLE ACCOUNTADMIN;

/*===============================================
Data Metric Functions (Measure Data Quality)
=================================================*/

USE ROLE ACCOUNTADMIN;

-- Create custom DMF to check for empty reviews
CREATE OR REPLACE DATA METRIC FUNCTION LAKEHOUSE_VHOL.PUBLIC.empty_review_count(
  ARG_T TABLE(ARG_C STRING)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE ARG_C IS NULL OR TRIM(ARG_C) = \'\'';

-- Create DMF for average rating validation
CREATE OR REPLACE DATA METRIC FUNCTION LAKEHOUSE_VHOL.PUBLIC.invalid_rating_count(
  ARG_T TABLE(ARG_C FLOAT)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T WHERE ARG_C < 1 OR ARG_C > 5 OR ARG_C IS NULL';

-- Change schedule to run every 5 minutes to trigger evaluation

ALTER ICEBERG TABLE LAKEHOUSE_VHOL.PUBLIC.product_reviews_iceberg
  SET DATA_METRIC_SCHEDULE = '5 MINUTE';
  
-- Run empty_review_count directly
SELECT LAKEHOUSE_VHOL.PUBLIC.empty_review_count(
  SELECT REVIEWTEXT FROM LAKEHOUSE_VHOL.PUBLIC.product_reviews_iceberg
) AS empty_review_count;

-- Run invalid_rating_count directly
SELECT LAKEHOUSE_VHOL.PUBLIC.invalid_rating_count(
  SELECT OVERALL FROM LAKEHOUSE_VHOL.PUBLIC.product_reviews_iceberg
) AS invalid_rating_count;

