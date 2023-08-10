# Example usage of streamlit_in_snowflake

1. Create a secrets.toml file in this repo, as described in the parent directory README
2. Create and activate a python 3.8 virtual environment, such as with Conda (see below)
3. Run the following

```shell
pip install "streamlit_in_snowflake @ git+https://github.com/Snowflake-Labs/sf-samples.git#subdirectory=samples/streamlit-in-snowflake/sis-local"
python -m streamlit run sis_app.py
```

**Enjoy!** You can also try copying the source for sis_app.py into a SiS app, it should just work.

## Setup using Conda

Since SiS manages environments with conda, it's convenient if you do the same locally. Here's a
minimal example of a conda setup using `environment.yml`. Read more in the
[SiS docs](https://docs.snowflake.com/en/LIMITEDACCESS/streamlit-in-snowflake#installing-packages-manually).

```sh
# One time setup on your machine
brew install --cask miniconda # If not already installed
conda config --set auto_activate_base false
conda config --add channels snowflake
conda init --all # See conda docs on what this does / how to use it

# Per app
conda env create -f environment.yml
conda activate sis-example
# Install streamlit_in_snowflake and run the example app as above
pip install "streamlit_in_snowflake @ git+https://github.com/Snowflake-Labs/sf-samples.git#subdirectory=samples/streamlit-in-snowflake/sis-local"
python -m streamlit run sis_app.py
```
