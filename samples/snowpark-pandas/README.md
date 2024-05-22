# Snowpark pandas API Demo and Examples 

The Snowpark pandas API lets you run your pandas code directly on your data in Snowflake. Just by changing the import statement and a few lines of code, you can get the same pandas-native experience you know and love at the speed and scale of Snowflake. See the [Snowpark pandas API documentation](https://docs.snowflake.com/LIMITEDACCESS/snowpark-pandas) to learn more.

1. Download the notebook file and data from the corresponding directory.
2. Create a new conda environment using the command:
    ```bash
    conda create --name snowpark-pandas-demo python=3.9 --y
    ```
3. Activate the conda environment with:
    ```bash
    conda activate snowpark-pandas-demo
    ```
4. Install the Snowpark python library with Modin:
    ```bash
    pip install "snowflake-snowpark-python[modin]"
    ```
5. Install the jupyter package:
    ```bash
    pip install jupyter
    ```
6. Launch Jupyter Notebook:
    ```bash
    jupyter notebook
    ```
7. Open the notebook.
8. Follow the instructions [here](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-a-default-connection) to set up a default Snowflake connection. For example,
    ```python
    # Create a Snowpark session with a default connection.
    from snowflake.snowpark.session import Session
    session = Session.builder.create()
    ``` 
9. Import Modin and the Snowpark pandas plugin for Modin  
    ```python
    import modin.pandas as spd
    import snowflake.snowpark.modin.plugin
    ```
10. Start using Snowpark pandas!