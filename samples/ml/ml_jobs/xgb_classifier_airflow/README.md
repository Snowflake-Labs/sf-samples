# Airflow Integration

The Runtime Job API can be easily integrated with Airflow using the
[SnowparkOperator](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowpark.html).

> NOTE: Make sure `snowflake-ml-python>=1.8.2` is installed in your Airflow worker environment(s)

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

See [single_node_xgb_dag_example.py](./samples/ml/ml_jobs/airflow/single_node_xgb_dag_example.py)
for a basic working example of a DAG using ML Jobs.