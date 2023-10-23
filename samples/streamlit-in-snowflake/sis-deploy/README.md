# Dependencies

pip install git+https://github.com/Snowflake-Labs/snowcli.git@v1.2.0-rc5

# Secrets

.streamlit/secrets.toml

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

~/.snowflake/config.toml

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

# Set up local conda environment

```sh
brew install miniconda`
conda env update`
conda activate sis-deploy`
```

# Generate data

`python generate_data.py`

Upload data to a table in Snowflake. Docs [here](https://docs.snowflake.com/en/user-guide/data-load-web-ui)

# Test locally

```sh
streamlit run app.py
```

# Deployment

```sh
snow streamlit deploy MY_APP --replace --query-warehouse=MY_WAREHOUSE
snow stage put "*.py" DATABASE.SCHEMA.streamlit/MY_APP --overwrite
```
