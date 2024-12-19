# Salesforce Sales Cloud YAML Templates for Cortex Analyst
**Author**: Rachel Blum, Solution Innovation Team
## Use Case
The provided semantic YAML file enables analysts to answer questions about accounts, opportunities, and sales reps out-of-the-box when using Cortex Analyst against Salesforce Sales Cloud tables in Snowflake.

This repository has instructions for implementing the semantic YAML file:
1. With the included sample Salesforce data
2. Against your own Salesforce data in Snowflake

## Key Assets & Pre-Requisites
1. If you are planning on editing the sample yaml template file, you will need the [The Semantic Model Generator for Cortex Analyst](https://github.com/Snowflake-Labs/semantic-model-generator). This is a developer solution for validation, iteration testing, editing and saving of a final file to stage. 
2. To deploy this solution in Cortex Analyst to end users, you will need a [a Cortex Analyst SiS (Streamlit-in-Snowflake) UI](https://medium.com/snowflake/deploying-cortex-analyst-a-modular-codebase-for-streamlit-in-snowflake-e9bc856e069c).  There are several options for chat UIs but I recommend this one because of its modular approach. 

## Setup for Using Sample Salesforce Data
1. Open `setup_01.sql` in VS Code or Snowsight and run all.  This script will create a database, schema, stage, and 3 tables (Account, Opportunity, and User).
2. Upload `sfdc_sales_samples.yaml`, 'Account.csv', 'Opportunity.csv' and 'User.csv' to the stage `SFDC.SALESCLOUD.SEMANTICS`.
3. Open `setup_02.sql` in VS Code or Snowsight and run all. This script will copy your csv files from Stage into your Account, Opportunity and User tables.
4. Open your Cortex Analyst UI in SiS and change AVAILABLE_SEMANTIC_MODEL_PATHS to "SFDC.SALESCLOUD.SEMANTICS/sfdc_sales_samples.yaml" 
  - INSERT IMAGE
6. Click Run nd start asking questions.

## Setup to Connect to Your Salesforce Data in Snowflake
> **Note:** While this template only includes non-custom fields common to most Salesforce implementations, your tables may have additional fields (custom or otherwise) not currently listed in the semantic template. You may add your custom or other fields to the semantic file as needed.

1. Upload `sfdc_sales.yaml` to a stage in your SFDC database (create a stage if necessary; it is not required to be in the SFDC database). This template file differs from `sfdc_sales_samples.yaml` in that it doesn't have any sample values populated.
2. Use the Semantic Model Generator to validate, iterate on, and edit the template. Some options:
    - **Add custom dimensions, time dimensions, and measures** from your Account, Opportunity, or User tables that are required to answer your user questions.
    - **Add sample values** from your tables in the `sample_values` section for each/any dimension and measure.
    - **Add synonyms** based on the semantics of your organization.
3. When you are satisfied with responses to your questions, and if those questions are common, use "Save as verified query" in the Semantic Model Generator.

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
- Which accounts have not been contacted in the last 30 days?

Opportunity Analysis:
- What is the total value of opportunities in each stage of the pipeline?
- How many deals are expected to close this month/quarter?
- Which deals have been stagnant for too long?

Sales Rep Performance:
- Which sales rep owns the most accounts, and how many?
- What is the total revenue closed by each sales rep this month?
- What is the average deal size and cycle length for won opportunities?

### Other Relevant Links
- [Sales Cloud Overview](https://architect.salesforce.com/diagrams/data-models/sales-cloud/sales-cloud-overview#More_Sales_Cloud_Data_Models)

