CREATE STAGE PDF_STAGE DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TASK REFRESH_PDF_STAGE
SCHEDULE = '1 MINUTE'
AS
ALTER STAGE PDF_STAGE REFRESH;

ALTER TASK REFRESH_PDF_STAGE RESUME;

CREATE OR REPLACE STREAM PDF_STREAM ON DIRECTORY(@PDF_STAGE);

SELECT * FROM PDF_STREAM;

CREATE OR REPLACE FUNCTION PDF_PARSE(file_path string)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'parse_pdf_fields'
PACKAGES=('typing-extensions','PyPDF2','snowflake-snowpark-python')
AS
$$
from pathlib import Path
import PyPDF2 as pypdf
from io import BytesIO 
from snowflake.snowpark.files import SnowflakeFile

def parse_pdf_fields(file_path):
    with SnowflakeFile.open(file_path, 'rb') as f:
       buffer = BytesIO(f.readall())
    reader = pypdf.PdfFileReader(buffer) 
    fields = reader.getFields()
    field_dict = {}
    for k, v in fields.items():
        if "/V" in v.keys():
            field_dict[v["/T"]] = v["/V"].replace("/", "") if v["/V"].startswith("/") else v["/V"]
            
    return field_dict
$$;

SELECT PDF_PARSE(build_scoped_file_url('@PDF_STAGE','Product_Sample.pdf'));

CREATE OR REPLACE TABLE PDF_RESULTS (FILEPATH VARCHAR, PARSED_PDF VARIANT);

CREATE OR REPLACE TASK PDF_INGEST
WAREHOUSE = BHANU_TEST
AFTER REFRESH_PDF_STAGE
WHEN
  SYSTEM$STREAM_HAS_DATA('PDF_STREAM')
AS
  INSERT INTO PDF_RESULTS(FILEPATH, PARSED_PDF) 
  SELECT RELATIVE_PATH, 
         PARSE_JSON(PDF_PARSE(build_scoped_file_url('@PDF_STAGE',RELATIVE_PATH))) 
  FROM PDF_STREAM 
  WHERE METADATA$ACTION = 'INSERT';

SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('REFRESH_PDF_STAGE');

SELECT *, 
       PARSED_PDF:FirstName::varchar AS FIRST_NAME, 
       PARSED_PDF:LastName::varchar AS LAST_NAME, 
       PARSED_PDF:Product::varchar AS PRODUCT,
       PARSED_PDF:"Purchase Date"::date AS PURCHASE_DATE,
       PARSED_PDF:Recommend::varchar AS RECOMMEND
FROM PDF_RESULTS;
