/*
Project Name: INSPECTIONS
Current Version: 2
Project Owner: BUSINESS_USER (Please refer to DOCAI documentation to grand BUSINESS_USER ROLE TO YOUR USER https://docs.snowflake.com/en/LIMITEDACCESS/document-ai/index)
Pre-requisites:
- Documents have been uploaded to the specified S3 stage.
- Two documents have also been uploaded and utilized within Snowflake Document AI to train and publish the model.
The following SQL script structures the information into a table, facilitating the setup for the Due Diligence part in the Streamlit application.
*/

USE ROLE BUSINESS_USER;
USE WAREHOUSE DOC_AI_WH;
USE DATABASE DOC_AI_DB;
USE SCHEMA DOC_AI_SCHEMA;

-- Creating the stage for Due Diligence documents
CREATE OR REPLACE STAGE DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE
  URL='replace by your stage URL'
  CREDENTIALS=(AWS_KEY_ID='replace by your AWS_KEY_ID' AWS_SECRET_KEY='replace by your AWS_SECRET_KEY')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'replace by your KMS_KEY_ID');

-- Listing documents in the stage
list @DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE;
list @DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE/Manual_2022-04-01;

-- Generating presigned URLs for the documents
SELECT 
    relative_path, 
    GET_PRESIGNED_URL('@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE', relative_path) AS file_url
FROM DIRECTORY(@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE);

-- Creating a raw table with predictions from Document AI
CREATE OR REPLACE TABLE DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE_RAW AS
SELECT RELATIVE_PATH, INSPECTIONS!PREDICT(
  GET_PRESIGNED_URL('@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE', RELATIVE_PATH), 4) AS inspection_prediction
FROM DIRECTORY(@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE);

-- Viewing raw predictions
SELECT * FROM DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE_RAW;

-- Extracting specific fields from the prediction results
SELECT 
    f.path AS json_path,
    f.value:score::FLOAT AS score,
    f.value:value::STRING AS value
FROM 
   DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE_RAW,
    LATERAL FLATTEN(input => inspection_prediction, recursive => TRUE) f
WHERE 
    TYPEOF(f.value) = 'OBJECT' AND
    OBJECT_KEYS(f.value) = ARRAY_CONSTRUCT('score', 'value');

-- Transforming and structuring the data into a usable table
CREATE OR REPLACE TABLE DUE_DILLIGENCE_TRANSFORMED AS
SELECT
    relative_path,
    inspection_prediction AS variant,
    inspection_prediction:ADDRESS[0]:value::STRING AS Address,
    inspection_prediction:ADDRESS[0]:score::FLOAT AS Address_score,
    inspection_prediction:COMPANY[0]:value::STRING AS Company,
    inspection_prediction:COMPANY[0]:score::FLOAT AS Company_score,
    inspection_prediction:INSPECTION_GRADE[0]:value::STRING AS Grade,
    inspection_prediction:INSPECTION_GRADE[0]:score::FLOAT AS Grade_score,
    inspection_prediction:INSPECTOR_NAME[0]:value::STRING AS Inspector,
    inspection_prediction:INSPECTOR_NAME[0]:score::FLOAT AS Inspector_score,
    inspection_prediction:INSPECTION_DATE[0]:value::STRING AS Inspection_date,
    inspection_prediction:INSPECTION_DATE[0]:score::FLOAT AS Inspection_date_score,
    inspection_prediction:MACHINE_NAME[0]:value::STRING AS MACHINE_NAME,
    inspection_prediction:MACHINE_NAME[0]:score::FLOAT AS MACHINE_NAME_SCORE,
    inspection_prediction:MACHINE_SERIAL_NUMBER[0]:value::STRING AS SERIAL_NUMBER,
    inspection_prediction:MACHINE_SERIAL_NUMBER[0]:score::FLOAT AS SERIAL_NUMBER_SCORE
FROM DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE_RAW;

-- Viewing transformed data
select * from DUE_DILLIGENCE_TRANSFORMED;

-- Simplified view of essential fields
select relative_path, Address, Company, Grade, Inspector, Inspection_date from DUE_DILLIGENCE_TRANSFORMED;

-- Optional: List all documents for verification
SELECT RELATIVE_PATH FROM DIRECTORY(@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE);

-- TIP FOR STREAMLIT APP DISPLAY : PLEASE CREATE AN OTHER STAGE DUE_DILLIGENCES (with S) IN DOC_AI_DB.DOC_AI_SCHEMA and upload files here also