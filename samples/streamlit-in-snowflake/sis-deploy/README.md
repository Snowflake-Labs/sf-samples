# Test and Deploy apps with the Snowcli

Often when developing SiS apps, you might want to graduate from the Snowsight UI and begin to develop locally with your preferred development environment (like VSCode), or collaborate with your team on GitHub or another git based environment. This repo provides an example of how to:
- Set up your Streamlit and Snowflake credentials
- Install the right dependencies
- Test and deploy your SiS apps from the command line

If you have issues with this example, feel free to open an issue, talk to your SE, or email me at tyler.richards@snowflake.com. You can find full documentation for the snowcli [here](https://github.com/Snowflake-Labs/snowcli) and specific documentation for the snowcli and SiS [here](https://docs.snowflake.com/LIMITEDACCESS/snowcli/streamlit-apps/overview).

# Secrets

Create your Streamlit secrets file (.streamlit/secrets.toml) and fill it out with the information below.

```toml
[connections.snowflake]
account = "<ACCOUNT>"
user = "<USER>"
authenticator = "externalbrowser"
role = "<ROLE>"
database = "<DATABASE>"
schema = "<SCHEMA>"
warehouse = "<WAREHOUSE>"
```

Edit your global Snowflake config file (~/.snowflake/config.toml) with the information below.

```toml
[connections.dev]
account = "<ACCOUNT>"
user = "<USERNAME>"
authenticator = "externalbrowser"
role = "<ROLE>"
database = "<DATABASE>"
schema = "<SCHEMA>"
warehouse = "<WAREHOUSE>"
```

# Set up local environment

```sh
pip install snowflake-cli-labs==1.2.1
brew install miniconda
conda env update
conda activate sis-deploy
```

# Generate data

`python generate_data.py`

You need to upload this data to a table in Snowflake, feel free to upload in the Snowsight UI [here](https://docs.snowflake.com/en/user-guide/data-load-web-ui).

# Test locally

```sh
streamlit run app.py
```

# Deployment

```sh
snow streamlit deploy MY_APP --replace --query-warehouse=MY_WAREHOUSE
snow stage put "*.py" DATABASE.SCHEMA.streamlit/MY_APP --overwrite
```
