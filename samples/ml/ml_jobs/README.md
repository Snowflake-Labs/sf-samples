# ML Jobs (PuPr)

Snowflake ML Jobs enables you to run machine learning workloads inside Snowflake
[ML Container Runtimes](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml)
from any environment. This solution allows you to:

- Leverage GPU and high-memory CPU instances for resource-intensive tasks
- Use your preferred development environment (VS Code, external notebooks, etc.)
- Maintain flexibility with custom dependencies and packages
- (PrPr) Scale workloads across multiple nodes effortlessly

Whether you're looking to productionize your ML workflows or prefer working in
your own development environment, Snowflake ML Jobs provides the same powerful
capabilities available in Snowflake Notebooks in a more flexible, integration-friendly
format.

## Setup

The Runtime Job API (`snowflake.ml.jobs`) API is available in
`snowflake-ml-python>=1.8.2`.

```bash
pip install snowflake-ml-python>=1.8.2
```

> NOTE: The Runtime Job API currently only supports Python 3.10.
  Attempting to use the API with a different Python version may yield
  unexpected errors.

## Getting Started

### Prerequisites

Create a compute pool if you don't already have one ready to use.

```sql
CREATE COMPUTE POOL IF NOT EXISTS DEMO_POOL_CPU -- Customize as desired
    MIN_NODES = 1
    MAX_NODES = 1               -- Increase if more concurrency desired
    INSTANCE_FAMILY = CPU_X64_S -- See https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool
```

### Function Dispatch

Python functions can be executed as Runtime Jobs using the `snowflake.ml.jobs.remote`
decorator.

```python
from snowflake.ml.jobs import remote

compute_pool = "MY_COMPUTE_POOL"
@remote(compute_pool, stage_name="payload_stage")
def hello_world(name: str = "world"):
    # We recommend importing any needed modules *inside* the function definition
    from datetime import datetime

    print(f"{datetime.now()} Hello {name}!")

# Function invocation returns a job handle (snowflake.ml.jobs.MLJob)
job = hello_world("developer")

print(job.id)               # Jobs are given unique IDs
print(job.status)           # Check job status
print(job.get_logs())       # Check job's console logs
print(job.wait(timeout=10)) # Block until job completion with optional timeout
```

> NOTE: Compute pool startup can take several minutes and can cause job execution
  to be delayed; subsequent job executions should start much faster.
  Consider manually starting the compute pool using
  `ALTER COMPUTE POOL <POOL_NAME> RESUME` prior to job execution.

### File-based Dispatch

The API also supports submitting entire Python files for execution for more
flexibility.

```python
# /path/to/repo/my_script.py

def main(*args):
    print("Hello world", *args)

if __name__ == '__main__':
    import sys
    main(*sys.argv[1:])
```

```python
from snowflake.ml.jobs import submit_file, submit_directory

compute_pool = "MY_COMPUTE_POOL"

# Upload and run a single script
job1 = submit_file(
    "/path/to/repo/my_script.py",
    compute_pool,
    stage_name="payload_stage",
    args=["arg1", "--arg2_key", "arg2_value"],  # (Optional) args are passed to script as-is
)

# Upload an entire directory and run a contained entrypoint
# This is useful if your code is organized into multiple modules/files
job2 = submit_directory(
    "/path/to/repo/",
    compute_pool,
    entrypoint="my_script.py",
    stage_name="payload_stage",
    args=["arg1", "arg2"],  # (Optional) args are passed to script as-is
)
```

`job1` and `job2` are job handles, see [Function Dispatch](#function-dispatch)
for usage examples.

### Retrieving Results

You can retrieve the job execution result using the `MLJob.result()` API.
The API returns the payload's return value or, if execution failed, raises an exception.

> NOTE: Return values are currently only supported for function-based jobs.
  File-based jobs will return `None` on success. Exception handling is supported
  for all types of jobs.

```python
from snowflake.ml.jobs import get_job

job = get_job('MLJOB_00000000_0000_0000_0000_000000000000')

# Blocks until job completion and returns the execution result on success
# or raises an exception on failure
result = job.result()
```

## Advanced Usage

### Custom Python Dependencies

The Runtime Job API runs payloads inside the Snowflake
[Container Runtime for ML](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml)
environment which comes pre-installed with most commonly used Python packages
for machine learning and data science. Most use cases should work "out of the box"
with no additional Python packages needed. If custom dependencies are required,
the API supports specifying `pip_requirements` which will be installed on runtime
startup.

Installing packages to the runtime environment requires an
[External Access Integration](https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access)

```sql
-- Requires ACCOUNTADMIN
-- Snowflake provides a pre-configured network rule for PyPI access
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PYPI_EAI
    ALLOWED_NETWORK_RULES = (snowflake.external_access.pypi_rule)
    ENABLED = true;
GRANT USAGE ON INTEGRATION PYPI_EAI TO ROLE <role_name>;
```

```python
from snowflake.ml.jobs import remote, submit_file, submit_directory

compute_pool = "MY_COMPUTE_POOL"

# Example only; numpy is already installed in the runtime environment by default
@remote(
    compute_pool,
    stage_name="payload_stage",
    pip_requirements=["numpy"],
    external_access_integrations=["pypi_eai"],
)
def hello_world(name: str = "world"):
    # We recommend importing any needed modules *inside* the function definition
    from datetime import datetime

    print(f"{datetime.now()} Hello {name}!")

job1 = hello_world("developer")

# Can use standard pip/requirements syntax to specify versions
job2 = submit_file(
    "/path/to/repo/my_script.py",
    compute_pool,
    stage_name="payload_stage",
    pip_requirements=["numpy==2.2.*"],
    external_access_integrations=["pypi_eai"],
)

# Can provide PIP_INDEX_URL to install packages from private source(s)
job3 = submit_directory(
    "/path/to/repo/",
    compute_pool,
    entrypoint="my_script.py",
    stage_name="payload_stage",
    pip_requirements=["custom-package"],
    external_access_integrations=["custom_feed_eai"],  # Configure EAI as needed
    env_vars={'PIP_INDEX_URL': 'https://my-private-pypi-server.com/simple'},
)
```

### Multi-Node Capabilities (PrPr)

ML Jobs also support running distributed machine learning workloads across multiple nodes, allowing you to:
- Scale workloads across multiple compute instances via [Ray](https://docs.ray.io/en/latest/ray-overview/examples.html)
- Process larger datasets and train more complex models through distributed data connectors and trainers that can efficiently handle data processing and model training across multiple nodes
- Speed up training through parallelization

Multi-node requires the `ENABLE_BATCH_JOB_SERVICES` to be enabled.
Contact your Snowflake account admin to enable the feature on your account.

```sql
ALTER ACCOUNT <account> SET ENABLE_BATCH_JOB_SERVICES = TRUE;
```

To use multi-node capabilities, specify the `num_instances` parameter:

```python
@remote(compute_pool, stage_name="payload_stage", num_instances=3)
def my_distributed_function():
    # Your distributed code here
    # Access instance-specific details via Ray
    import ray
    ray.init(address='auto', ignore_reinit_error=True)
    print(f"Ray nodes: {ray.nodes()}")
```

For multi-node jobs, you can access logs from individual instances:

```python
# Get logs from specific instances
job.get_logs()  # Head node
job.get_logs(instance_id=1)  # Node 1
job.get_logs(instance_id=2)  # Node 2
```

## Examples

### IDE

Examples showcasing how ML Jobs can be used from an IDE such as VSCode, Cursor, or PyCharm.

- [xgb_classifier](./xgb_classifier) - train a simple XGBoost classifier
- [pytorch_image_classifier](./pytorch_image_classifier) - train a simple PyTorch model for CIFAR-10
  image classification. Also demonstrates integration with Weights and Biases for experiment tracking
- [distributed_xgb_classifier](./distributed_xgb_classifier) - train an XGBoost model using the [Snowflake Container Runtime's distributor APIs](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml#xgboost)
  for distributed training across multiple nodes (PrPr)

### Jupyter Notebooks

Examples showcasing how ML Jobs can be used from a notebook environment like Jupyter.

- [xgb_classifier_nb](./xgb_classifier_nb) - train a simple XGBoost classifier
- [distributed_xgb_classifier_nb](./distributed_xgb_classifier_nb) - train an XGBoost model using the [Snowflake Container Runtime's distributor APIs](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml#xgboost)
  for distributed training across multiple nodes (PrPr)

### Pipelines / DAGs

Examples showcasing how ML Jobs can be integrated with workflow/DAG frameworks like Airflow.

- [xgb_classifier_airflow](./xgb_classifier_airflow/) - orchestrate model training and evaluation using Apache Airflow

## Known Limitations

1. The Headless Runtime currently only supports Python 3.10. Attempting to use 
other Python versions may throw unexpected errors like `UnpicklingError` or `TypeError`.
1. Running a large number of jobs can result in service start failure due to
`Number of services limit exceeded for the account`. This will be fixed in an upcoming release.
    - This prevents any kind of SPCS service from starting on the account, including Notebooks and Model Inference
    - As a temporary workaround, please avoid launching more than 200 concurrent
    jobs and manually delete completed and failed jobs.
      ```sql
      SHOW JOB SERVICES LIKE 'MLJOB%';
      DROP SERVICE <service_name>;
      ```
      ```python
      from snowflake.ml.jobs import list_jobs, delete_job
      for row in list_jobs(limit=-1).collect():
        if row["status"] in {"DONE", "FAILED"}:
          delete_job(row["id"])
      ```
1. Job logs are lost upon compute pool suspension even if the job entity itself has not been deleted.
This may happen either due to manual suspension `ALTER COMPUTE POOL MY_POOL SUSPEND`
or auto suspension on idle timeout.
    - Compute pool auto suspension can be disabled using `ALTER COMPUTE POOL MY_POOL SET AUTO_SUSPEND_SECS = 0`
    - For more information, see
      [Compute pool privileges](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-compute-pool#compute-pool-privileges)
      and [Compute pool cost](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/accounts-orgs-usage-views#compute-pool-cost)
1. Job payload stages (specified via `stage_name` param) are not automatically 
    cleaned up. Please manually clean up  the payload stage(s) to prevent
    excessive storage costs.