-- Creating GDPR Compliance Tags
-- This step establishes a GDPR tag with allowed values indicating categories of personal information for compliance.
CREATE OR REPLACE TAG GDPR ALLOWED_VALUES ('EMAIL', 'NAME', 'GENDER', 'BANK_ACCOUNT');

-- Creating a Table for Classification Results
-- This table will hold the results of semantic category classification for the LPS_REFERENTIAL table's columns.
CREATE OR REPLACE TABLE SNOW_INVEST.SILVER.CLASSIFICATION_RESULT AS
SELECT
    f.key AS COLUMN_NAME,
    CASE
        WHEN f.value:"extra_info":"probability" IS NULL AND f.value:"extra_info":"alternates"[0]:"probability" <> 'null'
        THEN f.value:"extra_info":"alternates"[0]:"privacy_category"
        ELSE f.value:"privacy_category"
    END ::VARCHAR AS PRIVACY_CATEGORY,
    CASE
        WHEN f.value:"extra_info":"probability" IS NULL AND f.value:"extra_info":"alternates"[0]:"probability" <> 'null'
        THEN f.value:"extra_info":"alternates"[0]:"semantic_category"
        ELSE f.value:"semantic_category"
    END ::VARCHAR AS SEMANTIC_CATEGORY,
    CASE
        WHEN f.value:"extra_info":"probability" IS NULL AND f.value:"extra_info":"alternates"[0]:"probability" <> 'null'
        THEN f.value:"extra_info":"alternates"[0]:"probability"
        ELSE f.value:"extra_info":"probability"
    END ::NUMBER(10,2) AS PROBABILITY
FROM
    TABLE(
        FLATTEN(EXTRACT_SEMANTIC_CATEGORIES('SNOW_INVEST.SILVER.LPS_REFERENTIAL')::VARIANT)
    ) AS f
WHERE PRIVACY_CATEGORY IS NOT NULL;

-- Stored Procedure for Associating GDPR Tags
-- This procedure dynamically associates GDPR tags with columns based on their semantic classification.
CREATE OR REPLACE PROCEDURE SNOW_INVEST.BRONZE.ASSOCIATE_TAG("CLASS_TABLE" VARCHAR(16777216), "LPS_REFERENTIAL_TABLE" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python==1.0.0')
HANDLER = 'associate_tag'
EXECUTE AS OWNER
AS 
$$
import snowflake.snowpark as snowpark

def associate_tag(session: snowpark.Session, class_table: str, LPS_REFERENTIAL_TABLE: str) -> None:
    class_df = session.table(class_table).to_pandas()
    class_df_identifier = class_df[class_df['PRIVACY_CATEGORY'] == 'IDENTIFIER']
    for index, row in class_df_identifier.iterrows():
        column = row['COLUMN_NAME']
        tag = row['SEMANTIC_CATEGORY']
        sql_string = f"ALTER TABLE {LPS_REFERENTIAL_TABLE} MODIFY COLUMN {column} SET TAG GDPR = '{tag}'"
        print(sql_string)
        session.sql(sql_string).show()
$$;

-- Defining a Masking Policy for GDPR Compliance
-- This masking policy anonymizes sensitive data based on GDPR tags and user roles, enhancing data protection.
CREATE OR REPLACE MASKING POLICY PII_DEMO_STRING AS (val STRING) RETURNS STRING ->
    CASE
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('GDPR') = 'BANK_ACCOUNT'
        THEN
            CASE
                WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN VAL 
                ELSE CONCAT(SUBSTRING(VAL, 1, 2), '********', SUBSTRING(VAL, LENGTH(VAL)-3, 4))
            END
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('GDPR') = 'EMAIL'
        THEN
            CASE
                WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN VAL 
                ELSE CONCAT('***', SUBSTRING(VAL, POSITION('@' IN VAL), LENGTH(VAL)))
            END
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('GDPR') IN ('NAME', 'GENDER')
        THEN
            CASE
                WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN VAL 
                ELSE '*** PII CONFIDENTIAL - MASKED ***'
            END
        ELSE
            '*** PII CONFIDENTIAL - MASKED ***'
    END;

-- Applying the Masking Policy to the GDPR Tag
-- This command links the created masking policy with the GDPR tag for automatic enforcement on tagged columns.
ALTER TAG GDPR SET MASKING POLICY PII_DEMO_STRING;

-- Applying GDPR Compliance Tags to Sensitive Columns:
ALTER TABLE SNOW_INVEST.SILVER.LPS_REFERENTIAL ADD TAG GDPR_TYPE('EMAIL') ON COLUMN EMAIL;
ALTER TABLE SNOW_INVEST.SILVER.LPS_REFERENTIAL ADD TAG GDPR_TYPE('NAME') ON COLUMN LAST_NAME_CONTACT;
ALTER TABLE SNOW_INVEST.SILVER.LPS_REFERENTIAL ADD TAG GDPR_TYPE('BANK_ACCOUNT') ON COLUMN IBAN;
