-- Define the file format if not already created
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

-- Copy the staged data into the ACCOUNT table
COPY INTO ACCOUNT
FROM @sfdc.salescloud.semantics/Account.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE';

-- Copy the staged data into the OPPORTUNITY table
COPY INTO OPPORTUNITY
FROM @sfdc.salescloud.semantics/Opportunity.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE';

-- Copy the staged data into the USER table
COPY INTO USER
FROM @sfdc.salescloud.semantics/User.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE';
