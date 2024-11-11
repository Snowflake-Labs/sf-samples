# Time Series Analytics Data Setup for Cortex Analyst

## Data Setup
This example utilizes the FactSet Tick History dataset from the Snowflake Marketplace, focusing on historical trading information.

You will use [Snowsight](https://docs.snowflake.com/en/user-guide/ui-snowsight.html#), the Snowflake web interface, to:
- Create Snowflake objects (warehouse, database, schema)
- Access FactSet data from Snowflake Marketplace
- Create tables needed

### Create Snowflake objects
Copy & paste the following in a sql worksheet and run all:

```sql
USE ROLE SYSADMIN;

-- Assign Query Tag to Session. This helps with performance monitoring and troubleshooting.
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"time_series_analysis","version":{"major":1, "minor":0},"attributes":{"is_quickstart":0, "source":"sql"}}';

-- Create a new warehouse, database, schema, and stage
CREATE OR REPLACE WAREHOUSE time_series_cortex_analyst_wh;
CREATE OR REPLACE DATABASE time_series_analytics_db;
CREATE OR REPLACE SCHEMA cortex_analyst_schema;
CREATE OR REPLACE STAGE cortex_analyst_stage;
```

### Access Data from Snowflake Marketplace

Follow below instructions to get the FactSet Tick History data from Snowflake Marketplace.
- Navigate to [Snowsight](https://app.snowflake.com/)
- Click: Data Products
- Click: Marketplace
- Search: FactSet Tick History
- Scroll Down and Click: Tick History
- Click: Get
- Make sure the Database name is: Tick_History
- Which roles, in addition to ACCOUNTADMIN, can access this database? PUBLIC
- Click: Get

## Cortex Analyst Setup
Use the [Getting Started with Cortex Analyst](https://quickstarts.snowflake.com/guide/getting_started_with_cortex_analyst/index.html#0) QuickStart guide to setup the demo but swap the semantic model and data objects to the ones provided here.

Within the streamlit application python file, make sure to update to the following:
```
DATABASE = "time_series_analytics_db"
SCHEMA = "cortex_analyst_schema"
STAGE = "cortex_analyst_stage"
FILE = "Time_Series_Analytics.yaml"
```

## Sample questions to ask
- What are the top 10 tickers with the largest number of records?
- What are the lowest and highest prices of META?
- what is average last price of META on September 2nd, 2022?
