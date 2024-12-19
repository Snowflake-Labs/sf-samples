# Salesforce Sales Cloud YAML Templates for Cortex Analyst
**Author**: Rachel Blum, Solution Innovation Team
## Use Case
The provided semantic YAML file enables analysts to answer questions about accounts, opportunities, and sales reps out-of-the-box when using Cortex Analyst against Salesforce Sales Cloud tables in Snowflake.

This repository has instructions for implementing the semantic YAML file with the included demo Salesforce data using semantic file sfdc_sales_demo.yaml, or against your own Salesforce data in Snowflake using semantic file sfdc_sales.yaml. The only difference between the two files is that the former has sample values based on the included demo data.

IMPORTANT: Please note that the semantic file table and field names are based on the standard [Field Reference documentation](https://developer.salesforce.com/docs/atlas.en-us.sfFieldRef.meta/sfFieldRef/salesforce_field_reference.htm) from Salesforce for Account, Opportunity and User. This will work out of the box for the demo tables included in this repository. If you are deploying against your own Salesforce data in Snowflake and your field names have been customized, you may have to adjust the 'name' section(s) of the yaml file under dimensions, time dimensions and/or measures.

## Key Assets & Pre-Requisites
1. You will need to install a Cortex Analyst Chatbot to deploy this solution to end users, preferably in Streamlit-in-Snowflake (SiS). Here are some recommended options:
[Basic Cortex Analyst SiS Demo](https://github.com/Snowflake-Labs/sfguide-getting-started-with-cortex-analyst/blob/main/cortex_analyst_sis_demo_app.py) - Easy setup.
[Advanced Cortex Analyst SiS Demo](https://medium.com/snowflake/deploying-cortex-analyst-a-modular-codebase-for-streamlit-in-snowflake-e9bc856e069c)- I recommend this one because of its modular approach and advanced features. This is the version used in the rest of the demo.

## Setup for Using Sample Salesforce Data
1. Open `setup_01.sql` in VS Code or Snowsight and run all.  This script will create a database, schema, stage, and 3 tables (Account, Opportunity, and User).
2. Upload `sfdc_sales_demo.yaml`, `Account.csv`, `Opportunity.csv` and `User.csv` to the stage `SFDC.SALESCLOUD.SEMANTICS`.
3. Open `setup_02.sql` in VS Code or Snowsight and run all. This script will copy your csv files from Stage into your Account, Opportunity and User tables.
4. If you are using the Advanced Cortex Analyst SiS application, open your Cortex Analyst UI in SiS and change the value of AVAILABLE_SEMANTIC_MODEL_PATHS to "SFDC.SALESCLOUD.SEMANTICS/sfdc_sales_demo.yaml".
5. Click Run and start asking questions.

## Setup to Connect to Your Salesforce Data in Snowflake
> **Note:** While this template only includes non-custom fields common to most Salesforce implementations, your tables may have additional fields (custom or otherwise) not currently listed in the semantic template. You may add your custom or other fields to the semantic file as needed.

1. Open the `sfdc_sales.yaml` file in the Semantic Model Generator or any text editor.
2. Change the database and schema entries in each base_table section (Account, Opportunity and User) from SFDC and SALESCLOUD to the database and schema names of your Salesforce data in Snowflake. You should be changing only lines 7, 8, 111, 112, 269 and 270 in the yaml file (see images below). Note: If any of the standard field names used in the yaml file have been changed in your Salesforce data in Snowflake database, you will need to adjust those as well. Using the Semantic Model Generator will help with resolving those issues and validating your final yaml file.
3. Upload `sfdc_sales.yaml` to a stage in Snowflake. You can use an existing stage or create a new one. The stage does not need to be in your SFDC database as the editing you have done in the previous step will point the yaml file to the correct database and schema for query generation.
4. If you are using the Advanced Cortex Analyst SiS application, open your Cortex Analyst UI in SiS and change the value of AVAILABLE_SEMANTIC_MODEL_PATHS to "<yourdatabase>.<yourschema>.<yourstage>/sfdc_sales.yaml" based on the stage you used in Step 3.
5. Click Run in the SiS app and start asking questions.

## Run Cortex Analyst in SiS

## Additonal Information

### Tables Included
- Account
- Opportunity
- User

See `setup_01.sql` or the semantic YAML file for included columns.

### Example Questions
Accounts Analysis:
- What are the top 10 accounts by total closed revenue?
- How many new accounts were created this quarter?
- What are my 5 largest accounts, show account name and amount?

Opportunity Analysis:
- What is the total value of opportunities in each stage of the pipeline?
- How many deals are expected to close this month/quarter?
- Which deals have been stagnant for too long?

Sales Rep Performance:
- Which sales rep owns the most accounts, and how many?
- What is the total revenue closed by each sales rep this month?
- What is the average deal size and cycle length for won opportunities?

