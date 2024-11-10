# Cortex Analyst - advanced SiS (Streamlit in Snowflake) demo

This repository hosts a cutting-edge Streamlit in Snowflake application showcasing the capabilities of Cortex Analyst. The demo serves three primary purposes:

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


## Local app development
This repo provides an out-of-the-box local development setup which should enable developers to iterate on their app's versions more smoothly.

### Get the right Python version and install packages

[Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) currently supports Python 3.8, so we'll use that version for local development. You can install it via [pyenv](https://github.com/pyenv/pyenv):

```bash
pyenv install 3.8
```

Create virtual environment and install packages:

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

If the app doesn't start, check that you've activated the virtual environment and installed all required packages — see the [Get the right Python version and install packages](#get-the-right-python-version-and-install-packages) sub-section.


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

**Note:** Make sure to maintain the same directory structure as in this repository.

### Create SiS app
Just run a following Snowflake SQL command:
```sql
CREATE STREAMLIT cortex_analyst_demo_app
FROM @cortex_analyst_demo.ca_sis_demo_app_schema.app_code
MAIN_FILE = 'Talk_to_your_data.py'
QUERY_WAREHOUSE = <warehouse name>
TITLE = 'Cortex Analyst - extended demo';
```

### Note on Applying Code Changes deployed SiS app

If you have already deployed your SiS app and made changes to the code, it can happen that you'll need to recreate the app in roder to reflect those changes to the deployed app. Just `CREATE OR REPLACE STREAMLIT` command after pushing updated code to app's stage.

---

Now you can navigate to Streamlit projects on Snowsight and preview the app!
