# XGBoost Classifier Example

## Setup

> NOTE: The MLJob API currently only supports Python 3.10 clients.

Install Python requirements using `pip install -r requirements.txt` from the sample directory.

This sample uses synthetic data for training and evaluation. Be sure to run data
generation using the [Jupyter notebook](#jupyter) or [prepare_data.py](src/prepare_data.py)
before attempting to run the [VSCode](#vscode) scripts.

### Connecting to Snowflake in Python

The scripts included in this example use the `SnowflakeLoginOptions` utility API
from `snowflake-ml-python` to retrieve Snowflake connection settings from config
files must be authored before use. See [Configure Connections](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections)
for information on how to define default Snowflake connection(s) in a config.toml
file.

```python
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

# Requires valid ~/.snowflake/config.toml file
session = Session.builder.configs(SnowflakeLoginOptions()).create()
```

## Jupyter

[Headless_Runtime_Demo.ipynb](jupyter/Headless_Runtime_Demo.ipynb)
shows an example of using Headless Container Runtimes to push function execution
into a Container Runtime instance from a Jupyter Notebook

```bash
jupyter notebook jupyter/Headless_Runtime_Demo.ipynb 
```

## VSCode

Payloads can also be dispatched from VSCode or any other IDE. [main.py](src/main.py)
demonstrates how payloads can be dispatched using either a function decorator or
via the `submit_job` API.

### Script Parameters

- `--source_table` (OPTIONAL) Training data location. Defaults to `loan_applications`
  which is created in the [setup step](#setup)
- `--save_mode` (OPTIONAL) Controls whether to save model to a local path or into Model Registry. Defaults to local
- `--output_dir` (OPTIONAL) Local save path. Only used if `save_mode=local`

### Example

```bash
python headless/single-node/xgb-loan-apps/src/main.py
```