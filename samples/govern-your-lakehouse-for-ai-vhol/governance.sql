/*===============================
CREATE ROLES & GRANT PERMISSIONS
=================================*/
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS DATA_ADMIN;
CREATE ROLE IF NOT EXISTS ANALYST;


GRANT USAGE ON DATABASE vino_lakehouse_vhol TO ROLE DATA_ADMIN;
GRANT USAGE ON DATABASE vino_lakehouse_vhol TO ROLE ANALYST;
GRANT USAGE ON SCHEMA vino_lakehouse_vhol.public TO ROLE DATA_ADMIN;
GRANT USAGE ON SCHEMA vino_lakehouse_vhol.public TO ROLE ANALYST;
GRANT SELECT ON ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg TO ROLE DATA_ADMIN;
GRANT SELECT ON ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE vino_xs TO ROLE DATA_ADMIN;
GRANT USAGE ON WAREHOUSE vino_xs TO ROLE ANALYST;


/*===========================================================
CREATE A ROW ACCESS POLICY: ANALYST SEES 2010 AND LATER ONLY
=============================================================*/

CREATE OR REPLACE ROW ACCESS POLICY vino_lakehouse_vhol.public.time_based_review_policy
AS (UNIXREVIEWTIME NUMBER) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_ADMIN') THEN TRUE
    WHEN CURRENT_ROLE() = 'ANALYST' 
      AND TO_TIMESTAMP(UNIXREVIEWTIME) >= '2010-01-01'::TIMESTAMP THEN TRUE
    ELSE FALSE
  END;


ALTER ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg
  ADD ROW ACCESS POLICY vino_lakehouse_vhol.public.time_based_review_policy ON (UNIXREVIEWTIME);

/*===============================================
VERIFY THE ROW ACCESS POLICY FOR DIFFERENT ROLES
=================================================*/

-- DATA_ADMIN: should see all 100,000 rows (1997-2014)
USE ROLE DATA_ADMIN;
SELECT 'DATA_ADMIN' AS role, COUNT(*) AS visible_rows,
       MIN(TO_TIMESTAMP(UNIXREVIEWTIME))::DATE AS earliest,
       MAX(TO_TIMESTAMP(UNIXREVIEWTIME))::DATE AS latest
FROM vino_lakehouse_vhol.public.product_reviews_iceberg;

-- ANALYST: should see fewer rows (2010-2014 only)
USE ROLE ANALYST;
SELECT 'ANALYST' AS role, COUNT(*) AS visible_rows,
       MIN(TO_TIMESTAMP(UNIXREVIEWTIME))::DATE AS earliest,
       MAX(TO_TIMESTAMP(UNIXREVIEWTIME))::DATE AS latest
FROM vino_lakehouse_vhol.public.product_reviews_iceberg;

/*===============================================
Dynamic Data Masking (Protect Sensitive Data)
=================================================*/

USE ROLE ACCOUNTADMIN;

-- Create masking policy to hide reviewer names
CREATE OR REPLACE MASKING POLICY vino_lakehouse_vhol.public.mask_reviewer_name
AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_ADMIN') THEN val
    ELSE '***MASKED***'
  END;

-- Apply to REVIEWERNAME column
ALTER ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg
  MODIFY COLUMN REVIEWERNAME SET MASKING POLICY vino_lakehouse_vhol.public.mask_reviewer_name;

-- DATA_ADMIN sees real names
USE ROLE DATA_ADMIN;
SELECT REVIEWERID, REVIEWERNAME, SUMMARY 
FROM vino_lakehouse_vhol.public.product_reviews_iceberg LIMIT 5;

-- ANALYST sees masked names
USE ROLE ANALYST;
SELECT REVIEWERID, REVIEWERNAME, SUMMARY 
FROM vino_lakehouse_vhol.public.product_reviews_iceberg LIMIT 5;

/*===============================================
Object Tagging (Data Classification)
=================================================*/

USE ROLE ACCOUNTADMIN;

-- Create tags for data classification
CREATE OR REPLACE TAG vino_lakehouse_vhol.public.pii_type ALLOWED_VALUES 'name', 'email', 'id', 'none';
CREATE OR REPLACE TAG vino_lakehouse_vhol.public.data_sensitivity ALLOWED_VALUES 'high', 'medium', 'low';

-- Apply tags to columns
ALTER ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg
  MODIFY COLUMN REVIEWERNAME SET TAG vino_lakehouse_vhol.public.pii_type = 'name';

ALTER ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg
  MODIFY COLUMN REVIEWERID SET TAG vino_lakehouse_vhol.public.pii_type = 'id';

ALTER ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg
  MODIFY COLUMN REVIEWTEXT SET TAG vino_lakehouse_vhol.public.data_sensitivity = 'medium';

-- Tag the table itself
ALTER ICEBERG TABLE vino_lakehouse_vhol.public.product_reviews_iceberg
  SET TAG vino_lakehouse_vhol.public.data_sensitivity = 'medium';

-- See all tags on the table
SELECT * FROM TABLE(
  INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'vino_lakehouse_vhol.public.product_reviews_iceberg',
    'TABLE'
  )
);

