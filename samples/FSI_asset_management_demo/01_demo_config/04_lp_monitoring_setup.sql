/*
LP Data Upload:
   Initially, ensure to upload the six files named LP_data, along with referential_lps and lps_investments, into your external stage. These files comprise critical information regarding Limited Partners (LPs), detailing engagements, distributions, and investments, along with referential data. Adjust the COPY INTO command to match your S3 bucket's nomenclature for importing this data into Snowflake.*/

-- Table Creation:

-- LP Equity Data Table
CREATE OR REPLACE TABLE SNOW_INVEST.SILVER.LP_EQUITY (
    FUND VARCHAR,
    COMPANY_NAME VARCHAR,
    DATE DATE,
    MARKET_CAP NUMBER,
    PRICE NUMBER,
    PRICE_50D_MOVING_AVERAGE NUMBER,
    PRICE_200D_MOVING_AVERAGE NUMBER,
    TOTAL_SHARES NUMBER,
    CHANGES_IN_SHARE NUMBER
);

-- LP Investments Data Table
CREATE OR REPLACE TABLE SNOW_INVEST.SILVER.LP_INVESTMENTS (
    LP VARCHAR,
    FUND VARCHAR,
    ASSET_CLASS VARCHAR,
    COMMENTS VARCHAR,
    COMMITMENT_CALLED_EU NUMBER,
    COMMITMENT_UNCALLED_EU NUMBER,
    DISTRIBUTED_EU NUMBER,
    NAV_EU NUMBER,
    RECALLABLE_EU NUMBER,
    SFDR NUMBER,
    TOTAL_COMMITMENT_EU NUMBER,
    VINTAGE NUMBER
);

-- LPs Referential Data Table
CREATE OR REPLACE TABLE SNOW_INVEST.SILVER.LPS_REFERENTIAL (
    LP VARCHAR,
    LAST_NAME_CONTACT VARCHAR,
    FIRST_NAME_CONTACT VARCHAR,
    GENDER VARCHAR,
    BIRTH_DATE DATE,
    EMAIL VARCHAR,
    IBAN VARCHAR
);

-- Populating LP Equity Data Table
COPY INTO SNOW_INVEST.SILVER.LP_EQUITY FROM @EXTERNAL_DATA_STAGE/path_to_LP_data_files
FILE_FORMAT = (format_name_or_options);

-- Populating LP Investments Data Table
COPY INTO SNOW_INVEST.SILVER.LP_INVESTMENTS FROM @EXTERNAL_DATA_STAGE/path_to_LP_investments_file
FILE_FORMAT = (format_name_or_options);

-- Populating LPs Referential Data Table
COPY INTO SNOW_INVEST.SILVER.LPS_REFERENTIAL FROM @EXTERNAL_DATA_STAGE/path_to_referential_lps_file
FILE_FORMAT = (format_name_or_options);

