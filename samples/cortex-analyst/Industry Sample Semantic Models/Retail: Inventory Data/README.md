# Retail & CPG Inventory Planning Data Setup for Cortex Analyst

## Data Setup
**`TODO`: Describe the data we have and the types of questions that can be asked.**

### Get data on Snowflake Marketplace
**`TODO`: Steps to access CRISP data on Snowflake Marketplace.**

### Create Snowflake objects
Copy & paste the following in a sql worksheet and run all:

```sql
USE ROLE SYSADMIN;

-- Assign Query Tag to Session. This helps with performance monitoring and troubleshooting.
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"retail_cpg_inventory_cortex_analyst","version":{"major":1, "minor":0},"attributes":{"is_quickstart":0, "source":"sql"}}';

-- Create a new warehouse, database, schema, and stage
CREATE OR REPLACE WAREHOUSE RETAIL_CPG_CORTEX_ANALYST_WH;
CREATE OR REPLACE DATABASE RETAIL_CPG_INVENTORY_DB;
CREATE OR REPLACE SCHEMA RETAIL_CPG_INVENTORY_SCHEMA;
CREATE OR REPLACE STAGE RETAIL_CPG_INVENTORY_STAGE;
```

## Cortex Analyst Setup
Use the [Getting Started with Cortex Analyst](https://quickstarts.snowflake.com/guide/getting_started_with_cortex_analyst/index.html#0) QuickStart guide to setup the demo but swap the semantic model and data objects to the ones provided here.

Within the streamlit application python file, make sure to update to the following:
```
DATABASE = "RETAIL_CPG_INVENTORY_DB"
SCHEMA = "RETAIL_CPG_INVENTORY_SCHEMA"
STAGE = "RETAIL_CPG_INVENTORY_STAGE"
FILE = "Harmonized_Retailer_Inventory_DC.yaml"
```

## Sample questions to ask
- How much is there in stock for each product for RETAILER 1?
- Which distribution center has the least amount of product stock for RETAILER 1?
- What is my on hand inventory count by geography for RETAILER 1?
- Which distribution center and state has the lowest stock in the East Coast for RETAILER 1?
- Which distribution center and state has the highest stock in the East Coast for RETAILER 1?
