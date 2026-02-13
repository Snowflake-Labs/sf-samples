# Test and Deploy apps with the Snowcli

Often when developing SiS apps, you might want to graduate from the Snowsight UI and begin to develop locally with your preferred development environment (like VSCode), or collaborate with your team on GitHub or another git based environment. This repo provides an example of how to:
- Set up your Streamlit and Snowflake credentials
- Install the right dependencies
- Test and deploy your SiS apps from the command line

If you have issues with this example, feel free to open an issue, talk to your SE, or email me at tyler.richards@snowflake.com or zachary.blackwood@snowflake.com. You can find full documentation for the snowcli [here](https://github.com/Snowflake-Labs/snowcli) and specific documentation for the snowcli and SiS [here](https://docs.snowflake.com/LIMITEDACCESS/snowcli-v2/streamlit-apps/).

# Install snowcli

```sh
pip install snowflake-cli-labs==2.0.0
```

# Download this folder

You can either download the files individually, clone this repo in git, or download the whole folder
by pasting this page's url in https://download-directory.github.io

NOTE: you can get a similar setup by simply doing
```
snow streamlit create my_app_name
```

Here's a brief summary of the files in the repo

    ├── .streamlit
    │   └── secrets.toml # This is where your credentials go for testing your app locally
    ├── common
    │   ├── get_data.py # Some functions for loading the sample data from Snowflake
    │   └── utils.py # Some general utilty methods
    ├── environment.yml # Defines the python packages your app will use
    ├── event_data.csv # Some sample data
    ├── pages
    │   └── users.py # An example page
    ├── snowflake.yml # Specifies your app's name and files for snowcli
    └── streamlit_app.py # An example home page

Once you've downloaded the folder, unzip it if necessary and go into it.

# Secrets

Edit the file called `.streamlit/secrets.toml` information below (
everything in <BRACKETS> should be filled in with your specific info):

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

Now add the same info to the global Snowflake config file (~/.snowflake/config.toml) by running
```sh
snow connection add --connection-name default
```
and putting in the same account, username, etc. as you put in your secrets.toml.

You can look at `~/.snowflake/config.toml` and you should see an entry that looks like this.

```toml
[connections.default]
account = "<ACCOUNT>"
user = "<USERNAME>"
authenticator = "externalbrowser"
role = "<ROLE>"
database = "<DATABASE>"
schema = "<SCHEMA>"
warehouse = "<WAREHOUSE>"
```

# Set up local environment

Because streamlit-in-snowflake uses conda to install and packages, it's best to install
conda locally and use that. These are defined in environment.yml

```yml
name: sf_env
channels:
  - snowflake
dependencies:
  - python=3.8.12 # The latest python available in snowflake as of 2024-02-12
  - streamlit=1.26.0 # The latest streamlit available in snowflake as of 2024-02-12
  - snowflake-snowpark-python
  - plotly
```

If you want to use other packages, just check that they're available on the Snowflake
anaconda channel https://repo.anaconda.com/pkgs/snowflake/, and if they're on there,
they should work fine.

You can then install conda locally and create a conda environment from this file

```sh
brew install miniconda
conda env update
conda activate sf_env
```

# Update files

Update `snowflake.yml` to have the name and warehouse you want to use

```yml
definition_version: 1
streamlit:
  name: streamlit_app
  query_warehouse: my_streamlit_warehouse # Note that an XS warehouse is recommended
  main_file: streamlit_app.py
  env_file: environment.yml
  pages_dir: pages/
  additional_source_files:
    - common/*.py
```

# Upload data

There is a file called `event_data.csv`, which you can download and then upload into a table in Snowflake.
You can do this by by uploading it through the Snowsight UI [here](https://docs.snowflake.com/en/user-guide/data-load-web-ui).

Once you have uploaded the file, update `TABLE_NAME` in `common/get_data.py` to the
full location of the table.

# Test locally

To test and debug the app locally, run:

```sh
streamlit run streamlit_app.py
```

# Deployment

To deploy the app to Snowflake, run:

```sh
snow streamlit deploy --open
```
