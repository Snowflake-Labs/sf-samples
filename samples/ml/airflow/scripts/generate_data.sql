-- Basic setup
USE ROLE SYSADMIN;
CREATE OR REPLACE WAREHOUSE ML_DEMO_WH; --by default, this creates an XS Standard Warehouse
CREATE OR REPLACE DATABASE ML_DEMO_DB;
CREATE OR REPLACE SCHEMA ML_DEMO_SCHEMA;

-- Create 10M rows of synthetic data
CREATE OR REPLACE TABLE loan_applications AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as application_id,
    ROUND(NORMAL(40, 10, RANDOM())) as age,
    ROUND(NORMAL(65000, 20000, RANDOM())) as income,
    ROUND(NORMAL(680, 50, RANDOM())) as credit_score,
    ROUND(NORMAL(5, 2, RANDOM())) as employment_length,
    ROUND(NORMAL(25000, 8000, RANDOM())) as loan_amount,
    ROUND(NORMAL(35, 10, RANDOM()), 2) as debt_to_income,
    ROUND(NORMAL(5, 2, RANDOM())) as number_of_credit_lines,
    GREATEST(0, ROUND(NORMAL(1, 1, RANDOM()))) as previous_defaults,
    ARRAY_CONSTRUCT(
        'home_improvement', 'debt_consolidation', 'business', 'education',
        'major_purchase', 'medical', 'vehicle', 'other'
    )[UNIFORM(1, 8, RANDOM())] as loan_purpose,
    RANDOM() < 0.15 as is_default,
    TIMEADD("MINUTE", UNIFORM(-525600, 0, RANDOM()), CURRENT_TIMESTAMP()) as created_at
FROM TABLE(GENERATOR(rowcount => 10000000))
ORDER BY created_at;

-- Create 1B rows of synthetic data
CREATE OR REPLACE TABLE loan_applications_1b AS
SELECT
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as application_id,
    ROUND(NORMAL(40, 10, RANDOM())) as age,
    ROUND(NORMAL(65000, 20000, RANDOM())) as income,
    ROUND(NORMAL(680, 50, RANDOM())) as credit_score,
    ROUND(NORMAL(5, 2, RANDOM())) as employment_length,
    ROUND(NORMAL(25000, 8000, RANDOM())) as loan_amount,
    ROUND(NORMAL(35, 10, RANDOM()), 2) as debt_to_income,
    ROUND(NORMAL(5, 2, RANDOM())) as number_of_credit_lines,
    GREATEST(0, ROUND(NORMAL(1, 1, RANDOM()))) as previous_defaults,
    ARRAY_CONSTRUCT(
        'home_improvement', 'debt_consolidation', 'business', 'education',
        'major_purchase', 'medical', 'vehicle', 'other'
    )[UNIFORM(1, 8, RANDOM())] as loan_purpose,
    RANDOM() < 0.15 as is_default,
    TIMEADD("MINUTE", UNIFORM(-525600, 0, RANDOM()), CURRENT_TIMESTAMP()) as created_at
FROM TABLE(GENERATOR(rowcount => 1000000000))
ORDER BY created_at;