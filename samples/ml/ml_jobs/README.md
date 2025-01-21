# Headless Container Runtime Jobs

## Setup

The Runtime Job API (`snowflake.ml.jobs`) API is available in
`snowflake-ml-python>=1.7.4`.

```bash
pip install snowflake-ml-python>=1.7.4

# Alternative: Install from private build/wheel file
pip install snowflake_ml_python-1.7.4a20240117-py3-none-any.whl
```

> NOTE: The Runtime Job API currently only supports Python 3.10.
  Attempting to use the API with a different Python version may yield
  unexpected errors.

The Runtime Job API jobs requires the `ENABLE_SNOWSERVICES_ASYNC_JOBS`
to be enabled in your Snowflake account or session.


```sql
-- Enable for session
ALTER SESSION SET ENABLE_SNOWSERVICES_ASYNC_JOBS = TRUE;

-- Enable for account (requires ACCOUNTADMIN)
ALTER ACCOUNT SET ENABLE_SNOWSERVICES_ASYNC_JOBS = TRUE;
```

## Getting Started

### Prerequisites

Create a compute pool if you don't already have one ready to use.

```sql
CREATE COMPUTE POOL IF NOT EXISTS HEADLESS_JOB_POOL -- Customize as desired
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
    import datetime

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
    import datetime

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

### Airflow Integration

The Runtime Job API can be used in Airflow using the
[SnowparkOperator](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowpark.html).

> NOTE: Make sure `snowflake-ml-python>=1.7.4` is installed in your Airflow worker environment(s)

```python
import datetime
from airflow.decorators import dag, task
from snowflake.ml import remote, submit_file

@dag(start_date=datetime.datetime(2025, 1, 1), schedule="@daily")
def my_dag():
    @task.snowpark()
    def task_from_function():
        # SnowparkOperator automatically creates a Snowpark Session
        # which the Runtime Job API can infer from context
        @remote("HEADLESS_JOB_POOL", stage_name="payload_stage")
        def my_function():
            print("Hello world")
        job = my_function()
        print("Job %s submitted" % job.id)
        print("Job %s ended with status %s. Logs:\n" % (job.id, job.wait(), job.get_logs()))

    @task.snowpark()
    def task_from_file():
        # SnowparkOperator automatically creates a Snowpark Session
        # which the Runtime Job API can infer from context
        job = submit_file(
            "./my_script.py",
            "HEADLESS_JOB_POOL",
            stage_name="payload_stage",
        )
        print("Job %s submitted" % job.id)
        print("Job %s ended with status %s. Logs:\n" % (job.id, job.wait(), job.get_logs()))

    task_from_function()
    task_from_file()

my_dag()
```

## Next Steps

- See the [XGBoost Classifier Example](./single-node/xgb-loan-apps/) for a full
  walkthrough of training and deploying an XGBoost model.
- See the [PyTorch Classifier Example](./single-node/pytorch-cifar10/) for a full
  walkthrough of training a PyTorch model with Weights and Biases integration

## Known Limitations

1. The Headless Runtime currently only supports Python 3.10. Attempting to use 
other Python versions may throw errors like `UnpicklingError`.
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