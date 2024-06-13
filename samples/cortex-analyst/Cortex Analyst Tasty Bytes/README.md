# Cortex Analyst
Run [setup.sql](setup.sql). This scripts sets up our GIT REPOSITORIES, optionally sets up the Tasty Bytes dataset, and uploads a semantic model, and creates our Notebook and Streamlit app. 

If you'd like to run the Streamlit app someplace other than Streamlit in Snowflake, use [streamlit_standalone.py](streamlit_standalone.py).

## Create Semantic Model
This repo includes a sample repository. Details on creating your own semantic model are available in the [docs](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/cortex-analyst-overview#label-copilot-create-semantic-model).

You can use the [semantic-model-generator](https://github.com/Snowflake-Labs/semantic-model-generator) to give you a good head start on creating your semantic model.