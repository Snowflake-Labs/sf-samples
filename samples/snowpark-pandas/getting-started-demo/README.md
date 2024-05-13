# Snowpark pandas API Getting Started Demo


1. Install the Cybersyn data set used in the demo from Snowflake marketplace [here](https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7/cybersyn-inc-financial-economic-essentials).
1. Download the [demo notebook](demo.ipynb) and [creds.json](creds.json) files in this directory.
1. Add your Snowflake account credentials to the creds.json file.
1. Download the latest private Snowpark library, which incluees Snowpark pandas, [here](https://drive.google.com/drive/folders/1n2ijLihBVe3KuryqverajnqH7fxih3TL) and place the wheel file in the same project directory where you have the demo notebook.
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
1. Open the demo notebook and run the cells !
