# Cortex Analyst - advanced SiS (Streamlit in Snowflake) demo

This repository hosts a cutting-edge [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) application showcasing the capabilities of [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst). The demo serves three primary purposes:

* Demonstrate the full potential of Cortex Analyst.
* Illustrate the possibilities of building innovative applications with Streamlit in Snowflake and Cortex Analyst.
* Offer a well-structured, easily replicable foundation for developing your own custom app.


##  App Features

This app offers the following features:

* 💬 Converse with [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst): Get instant insights on your data by chatting with our AI-powered analyst.
* 📈 Visualize your data: Turn your query results into stunning charts and tables.
* 💾 Save and refine: Save your favorite queries and charts, and edit them whenever you need to.
* 👥 Collaborate and share: Share your discoveries with others and explore what the community has shared in the Public Charts page.
* 💡 Discover new insights: Browse the Public Charts page to see what others have discovered, and get inspired by new ideas.

### Extra LLM-based features and how to enable them

This codebase also provides additional, LLM-based features enhancing your chat experience:

* **Generate Text Summary for Query Execution Results**: Automatically summarize the results of your queries in a concise text format.
* **Generate Plot Config Suggestion for Generated Data**: Receive suggestions on how to best visualize your query results with appropriate plot configurations.
* **Generate Additional Follow-up Questions for Further Data Exploration**: Get intelligent suggestions for follow-up questions to help you explore your data more deeply.

These features can be enabled or disabled by setting the following constants in [constants.py](./constants.py):

```python
ENABLE_SMART_DATA_SUMMARY = True|False
ENABLE_SMART_CHART_SUGGESTION = True|False
ENABLE_SMART_FOLLOWUP_QUESTIONS_SUGGESTIONS = True|False
```

All of these features utilize [Snowflake Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) to generate text with the use of LLMs. You can set which particular models are used through the following constants, also placed in [constants.py](./constants.py):

```python
SMART_DATA_SUMMARY_MODEL = "..."
SMART_CHART_SUGGESTION_MODEL = "..."
SMART_FOLLOWUP_QUESTIONS_SUGGESTIONS_MODEL = "..."
```

For the list of available models, visit the [documentation page](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability).


## Local app development
This repo provides an out-of-the-box local development setup which should enable developers to iterate on their app's versions more smoothly.

### Get the right Python version and install packages

[Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) currently supports Python 3.8, so we'll use that version for local development. You can install it via [pyenv](https://github.com/pyenv/pyenv):

```bash
pyenv install 3.8
```

Create a virtual environment and install packages:

```bash
python -m virtualenv -p ~/.pyenv/versions/3.8.19/bin/python venv
source venv/bin/activate
pip install -r requirements.txt
```

### Setup your Snowflake connection config

Despite the application being a local development environment, you will still connect to your Snowflake instance. To do this, create a `connections.toml` file defining your Snowflake connection. We recommend placing this file in the default location suggested by [documentation](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file): `~/.snowflake/connections.toml`.

Here's an example `~/.snowflake/connections.toml` file:
```toml
[ca-sis-demo-connection]
host = "<host>"
account = "<account>"
user = "<user>"
password = "<password>"
warehouse = "<warehouse>"
role = "<role>"
```

**Note:** In this example, the connection is named `ca-sis-demo-connection`. If your connection has a different name, you can use the environment variable `SNOWPARK_CONNECTION_NAME` to overwrite it. For example, you can set `SNOWPARK_CONNECTION_NAME=my_connection` to use a connection named `my_connection` instead.


### Setup app data

Before running the app, you need to set up the required data in your Snowflake database. This includes adding a YAML file with a semantic model and creating app working schema.

To set up the data, follow these steps:

* Create a semantic model in your Snowflake database. You can use demo database and semantic model from [Cortex Analyst Quick Start guide](https://quickstarts.snowflake.com/guide/getting_started_with_cortex_analyst/index.html#2).
* Create a working schema in your Snowflake account to store the app data:

```sql
USE DATABASE cortex_analyst_demo;
CREATE SCHEMA ca_sis_demo_app_schema;
```

Once you have set up the data, you need to point the app to the correct semantic model and database. You can do this by updating the variables in the [constants.py](./constants.py) project file.

The default values in the `constants.py` file are set to match the [Quick Start guide](https://quickstarts.snowflake.com/guide/getting_started_with_cortex_analyst/index.html#0). 
__If you have used different names for your semantic model or database, you will need to update the variables accordingly.__


### Run the app
```bash
python -m streamlit run --server.runOnSave true Talk_to_your_data.py
```
This will start the Streamlit app in your default web browser. You can interact with the app and see the changes reflected in real-time thanks to the `--server.runOnSave true` flag.

If the app doesn't start, check if you've activated the virtual environment and installed all required packages — see the [Get the right Python version and install packages](#get-the-right-python-version-and-install-packages) sub-section.


## SiS setup
This section presents step by step guide on how to deploy this code as SiS app on Snowsight.

### Setup app data

Refer to the [section above](#setup-app-data).

### Prepare the working schema for app

Create schema and stage:
```sql
USE DATABASE cortex_analyst_demo;
CREATE SCHEMA ca_sis_demo_app_schema;
CREATE STAGE cortex_analyst_demo.ca_sis_demo_app_schema.app_code;
```

### Upload files to stage
Upload all files from this repository to the `cortex_analyst_demo.ca_sis_demo_app_schema.app_code` stage. You can use your preferred method to upload the files:
* [Snowsight UI](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui)
* [SnowSQL](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage)
* [Snowflake git integratrion](https://docs.snowflake.com/en/developer-guide/git/git-overview)
* [Snowflake CLI](https://docs.snowflake.com/developer-guide/snowflake-cli/index)

**Note:** Make sure to maintain the same directory structure as in this repository.

#### Example of uploading files to stage through SnowSQL

1. Install SnowSQL - Snowflake CLI client. [Instructions: Installing SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql-install-config). We recommend to use version >= 3.0:
```bash
snow --version
Snowflake CLI version: 3.1.0
```

2. Setup [connection config](https://docs.snowflake.com/en/user-guide/snowsql-config#about-the-snowsql-config-file). You can do it throught creating `.toml` file and place it in `~/snowflake/connections.toml`. Here is an example configuration for connection aliased `ca-sis-demo-connection`:
```toml
[ca-sis-demo-connection]
host = "<host>"
account = "<account>"
user = "<user>"
password = "<password>"
warehouse = "<warehouse>"
role = "<role>"
```

3. Put files to stage by executing set of `PUT` commands from `sis_setup/upload_files_to_stage.sql`:
```bash
snow sql -c ca-sis-demo-connection -f sis_setup/upload_files_to_stage.sql
```
> Those commands assume that code for your deployed app should be uploaded to `@CORTEX_ANALYST_DEMO.CA_SIS_DEMO_APP_SCHEMA.app_code` stage, you will need to update it accordingly, path in your setup differ.

### Create SiS app
Just run a following Snowflake SQL command:
```sql
CREATE STREAMLIT cortex_analyst_demo_app
ROOT_LOCATION = '@cortex_analyst_demo.ca_sis_demo_app_schema.app_code'
MAIN_FILE = 'Talk_to_your_data.py'
QUERY_WAREHOUSE = <warehouse name>
TITLE = 'Cortex Analyst - extended demo';
```

If you followed SnowSQL setup for the section above, you can also do it through following command:
```bash
snow sql -c ca-sis-demo-connection -f sis_setup/create_app.sql
```
Just remember to replace `<warehouse name>` with the actual name of the warehouse intended for this app.

### Note on Applying Code Changes deployed SiS app

If you have already deployed your SiS app and made changes to the code, it can happen that you'll need to recreate the app in roder to reflect those changes to the deployed app. Just `CREATE OR REPLACE STREAMLIT` command after pushing updated code to app's stage.

### Example of deploying using Snowflake CLI

1. [Install Snowflake CLI](https://docs.snowflake.com/developer-guide/snowflake-cli/installation/installation)
2. [Configure the connection to your account](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/connect)
3. Run the Streamlit deployment command in this app's directory:

    ```shell
    snow streamlit deploy --replace --role=CORTEX_USER_ROLE
    ```

    The `snowflake.yml` file provided in this repository will configure `snow` to deploy the code into the schema created above.

    `--role` switch will ensure that the application gets the proper owner's role.

---

Now you can navigate to Streamlit projects on Snowsight and preview the app!
