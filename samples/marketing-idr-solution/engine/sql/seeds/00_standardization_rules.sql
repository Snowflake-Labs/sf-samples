/*
 * ============================================================================
 * Standardization Rules Table
 * ============================================================================
 * 
 * Purpose:
 *   Maps Snowflake semantic categories to SQL standardization expressions.
 *   This makes the standardization process metadata-driven and extensible.
 * 
 * Usage:
 *   SP_STANDARDIZE_DATA queries this table to find the appropriate SQL
 *   expression to apply for each semantic category detected on a column.
 * 
 * SQL Expression Placeholder:
 *   {col} - will be replaced with the actual column name at runtime
 * 
 * ============================================================================
 */

USE DATABASE IDR;
USE SCHEMA CONFIG;

-- Create the rules table
CREATE TABLE IF NOT EXISTS IDR.CONFIG.IDR_CORE_STANDARDIZATION_RULES (
    RULE_ID VARCHAR(50) PRIMARY KEY,
    SEMANTIC_CATEGORY VARCHAR(100) NOT NULL,
    RULE_NAME VARCHAR(100) NOT NULL,
    SQL_EXPRESSION VARCHAR(4000) NOT NULL,
    PRIORITY INT DEFAULT 100,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    DESCRIPTION VARCHAR(500),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed standardization rules (idempotent using MERGE)
MERGE INTO IDR.CONFIG.IDR_CORE_STANDARDIZATION_RULES AS t
USING (
    SELECT column1 AS RULE_ID, column2 AS SEMANTIC_CATEGORY, column3 AS RULE_NAME, 
           column4 AS SQL_EXPRESSION, column5 AS DESCRIPTION
    FROM VALUES
        -- Email standardization: lowercase and trim
        ('EMAIL_STD', 'EMAIL', 'Email Normalize', 
         'LOWER(TRIM({col}))', 
         'Lowercase and trim email addresses'),
        
        -- Phone standardization: extract digits, format as E.164 (US)
        ('PHONE_STD', 'PHONE_NUMBER', 'Phone E.164 Format', 
         'CASE WHEN LENGTH(REGEXP_REPLACE({col}, ''[^0-9]'', '''')) >= 10 THEN ''+1'' || RIGHT(REGEXP_REPLACE({col}, ''[^0-9]'', ''''), 10) ELSE {col} END', 
         'Format phone numbers to E.164 standard (+1XXXXXXXXXX)'),
        
        -- Name standardization: proper case, remove special characters
        ('NAME_STD', 'NAME', 'Name Proper Case', 
         'UPPER(TRIM(REGEXP_REPLACE({col}, ''[^A-Za-z ]'', '''')))', 
         'Uppercase names, remove special characters'),
        
        -- Date of birth: validate and convert to DATE
        ('DOB_STD', 'DATE_OF_BIRTH', 'Date Validation', 
         'TRY_TO_DATE({col})', 
         'Validate and standardize date of birth'),
        
        -- Gender: normalize to single uppercase character
        ('GENDER_STD', 'GENDER', 'Gender Normalize', 
         'UPPER(LEFT(TRIM({col}), 1))', 
         'Normalize gender to single character (M/F/O)'),
        
        -- Postal code: uppercase, remove special characters
        ('POSTAL_STD', 'POSTAL_CODE', 'Postal Code Format', 
         'UPPER(TRIM(REGEXP_REPLACE({col}, ''[^A-Z0-9]'', '''')))', 
         'Uppercase postal codes, keep alphanumeric only'),
        
        -- City: proper case
        ('CITY_STD', 'CITY', 'City Proper Case', 
         'UPPER(TRIM({col}))', 
         'Uppercase city names'),
        
        -- State/Province: uppercase
        ('STATE_STD', 'ADMINISTRATIVE_AREA_1', 'State Normalize', 
         'UPPER(TRIM({col}))', 
         'Uppercase state/province codes'),
        
        -- Country: uppercase
        ('COUNTRY_STD', 'COUNTRY', 'Country Code', 
         'UPPER(TRIM({col}))', 
         'Uppercase country codes'),
        
        -- Street address: proper case
        ('ADDR_STD', 'STREET_ADDRESS', 'Address Proper Case', 
         'UPPER(TRIM({col}))', 
         'Uppercase street addresses')
) AS s
ON t.RULE_ID = s.RULE_ID
WHEN MATCHED THEN UPDATE SET
    SEMANTIC_CATEGORY = s.SEMANTIC_CATEGORY,
    RULE_NAME = s.RULE_NAME,
    SQL_EXPRESSION = s.SQL_EXPRESSION,
    DESCRIPTION = s.DESCRIPTION,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (RULE_ID, SEMANTIC_CATEGORY, RULE_NAME, SQL_EXPRESSION, DESCRIPTION)
VALUES (s.RULE_ID, s.SEMANTIC_CATEGORY, s.RULE_NAME, s.SQL_EXPRESSION, s.DESCRIPTION);

-- Verify rules loaded
SELECT RULE_ID, SEMANTIC_CATEGORY, RULE_NAME, IS_ACTIVE 
FROM IDR.CONFIG.IDR_CORE_STANDARDIZATION_RULES 
ORDER BY SEMANTIC_CATEGORY;
