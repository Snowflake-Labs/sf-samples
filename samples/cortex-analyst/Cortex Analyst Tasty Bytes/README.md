# Cortex Analyst Demo Using TastyBytes Demo Data

Author: [Jeremy Griffith](https://www.linkedin.com/in/jeremygriffith/)

This sample sets up a dataset in your Snowflake account and provides a semantic model for use by Cortex Analyst. It also creates a Notebook and Streamlit app in your Snowflake account so you can explore and experment with the Cortex Analyst service. 

## Prerequisites
* A Snowflake account. Sign up for a free trial at [https://signup.snowflake.com](signup.snowflake.com).
* Access to the Cortex Analyst service (in Private Preview as of July 10, 2024).

## Setup
[setup.sql](setup.sql) creates several objects in our account that are necessary to run this demo. The script provided performs the following steps. Modify this script as necessary for your environment. 

1. Create a database called `GIT_DB` that we will use for our Git integration. 
1. Create a warehouse called `NOTEBOOK_WH`.
1. Create a [SECRET](https://docs.snowflake.com/en/user-guide/api-authentication) for GitHub. This will use your [PAT](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) that you can get from your GitHub account. 
1. Create an API integration to GitHub. Modify the `API_ALLOWED_PREFIXES` if necessary. 
1. Create a [GIT REPOSITORY](https://docs.snowflake.com/en/developer-guide/git/git-overview). This will be used to optionally set up the TastyBytes dataset in the next step and for creating the notebook in your account. 
1. _Optional:_ Execute the [TastyBytes setup script](https://github.com/Snowflake-Labs/sf-samples/blob/main/samples/tasty_bytes/FY25_Zero_To_Snowflake/tb_introduction.sql?utm_cta=website-workload-data-science-5-factors-webinar) from GitHub using `EXECUTE IMMEDIATE`. You can skip this step if you already set up TastyBytes in your account. 
1. Create a schema called `CORTEX_ANALYST_DEMO` in the `TB_101` database created in the last step and grant privileges for `CREATE NOTEBOOK` and `CREATE STREAMLIT`.
1. Create a schema called `SEMANTIC_MODELS` in the `TB_101` database and create a stage to host our semantic model. 
1. Copy the semantic model from GitHub to the `SEMANTIC_MODELS` stage. 
1. Create a Notebook based on code in our GitHub stage. 
1. Create a Streamlit app based on code in our GitHub stage. 

## Create or Modify Semantic Model
This repo includes a sample semantic model. Details on creating your own semantic model are available in the [docs](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/cortex-analyst-overview#label-copilot-create-semantic-model).

You can use the [semantic-model-generator](https://github.com/Snowflake-Labs/semantic-model-generator) to give you a good head start on creating your semantic model.

If you decide to create your own semantic model, simply upload it to a stage and change the `semantic_model_file` argument in the API call to point at your own model file. 

## Running the example Notebook
Navigate to the Notebooks page of the Snowflake UI and open the notebook titled *Tasty Bytes Analyst Demo*. This notebook gives examples of calling the Cortex Analyst API and parsing the results to extract the generated SQL statement.

You can modify the database, schema, stage, and semantic model name in the first cell if you changed anything from the default setup. 

## Running the example Streamlit app
Navigate to the Streamlit page of the Snowflake UI and open the Streamlit app titled *Tasty Bytes Cortex Analyst*. The code here is similar to what was run in the Notebook, but also includes the Streamlit `CHAT_MESSAGE` and `CHAT_INPUT` widgets to create a user-friendly interface. 

[streamlit_standalone.py](streamlit_standalone.py) can be used if you'd like to run the Streamlit app outside of Snowflake. You will need to provide your Snowflake connection details to run this example outside of Snowflake ([docs](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file).
