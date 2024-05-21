# Snowpark pandas API Demo and Examples 


1. Download the notebook file from the corresponding directory.
1. Follow the instructions [here](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-a-default-connection) to set up a default Snowflake connection.
1. Download the latest private Snowpark library, which includes Snowpark pandas, [here](https://drive.google.com/drive/folders/1n2ijLihBVe3KuryqverajnqH7fxih3TL) and place the wheel file in the same project directory where you have the demo notebook.
1. Navigate to the project directory folder in the terminal.
1. Create a new conda environment using the command:
    ```bash
    conda create --name snowpark-pandas-demo python=3.9 --y
    ```
1. Activate the conda environment with:
    ```bash
    conda activate snowpark-pandas-demo
    ```
1. Install the Snowpark wheel that you downloaded:
    ```bash
    pip install "snowflake_snowpark_python-1.15.0a1-py3-none-any.whl[modin]"
    ```
1. Install the jupyter package:
    ```bash
    pip install jupyter
    ```
1. Launch Jupyter Notebook:
    ```bash
    jupyter notebook
    ```
1. Open the demo notebook and run the cells!
