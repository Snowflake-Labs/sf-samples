# Snowpark pandas API Demo and Examples 


1. Download the notebook file and data from the corresponding directory.
2. Follow the instructions [here](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-a-default-connection) to set up a default Snowflake connection.
3. Create a new conda environment using the command:
    ```bash
    conda create --name snowpark-pandas-demo python=3.9 --y
    ```
4. Activate the conda environment with:
    ```bash
    conda activate snowpark-pandas-demo
    ```
5. Install the Snowpark python library with Modin:
    ```bash
    pip install "snowflake-snowpark-python[modin]"
    ```
6. Install the jupyter package:
    ```bash
    pip install jupyter
    ```
7. Launch Jupyter Notebook:
    ```bash
    jupyter notebook
    ```
8. Upload the notebook and data, and then run the cells!
