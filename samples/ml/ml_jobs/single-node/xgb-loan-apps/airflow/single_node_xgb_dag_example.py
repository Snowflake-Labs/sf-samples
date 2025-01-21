import os
from datetime import datetime, timedelta, UTC

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from snowflake.ml import jobs

_PAYLOAD_SOURCE = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "src"))

@dag(
    dag_id="single_node_xgb_dag_example",
    dag_display_name="Single Node XGBoost Example DAG",
    schedule=timedelta(weeks=1),
    start_date=datetime(2024, 12, 15, tzinfo=UTC),
    catchup=False,
)
def single_node_xgb_dag_example():
    """
    Basic sample of a DAG that trains an XGBoost model on a single node
    using the preview MLJob APIs in Snowflake.
    """

    @task.snowpark()
    def prepare_data():
        # Kick off preprocessing job on SPCS
        job = jobs.submit_directory(
            _PAYLOAD_SOURCE,
            entrypoint="prepare_data.py",
            args=["--table_name", "HEADLESS_DEMO_DB.DAG_DEMO.DATA_TABLE", "--num_rows", "100000"],
            compute_pool="SYSTEM_COMPUTE_POOL_CPU",
            stage_name="HEADLESS_DEMO_DB.DAG_DEMO.PAYLOAD_STAGE",
        )

        # Block until job completes
        job.wait()

        # Print logs for observability
        job.show_logs()

        return job.id

    @task.snowpark()
    def start_training_job(run_id: str):
        job = jobs.submit_directory(
            _PAYLOAD_SOURCE,
            entrypoint="train.py",
            args=[
                "--source_data", "HEADLESS_DEMO_DB.DAG_DEMO.DATA_TABLE",
                "--output_dir", f"@HEADLESS_DEMO_DB.DAG_DEMO.MODELS/{run_id}",
            ],
            compute_pool="SYSTEM_COMPUTE_POOL_GPU",
            stage_name="HEADLESS_DEMO_DB.DAG_DEMO.PAYLOAD_STAGE",
            # num_instances=4, # Multi-node not supported in PrPr
        )

        return job.id

    @task.snowpark()
    def wait_for_completion(job_id: str):
        job = jobs.get_job(job_id)
        job.wait()
        if job.status == "DONE":
            print("Job completed. Logs:\n%s" % job.get_logs())
        elif job.status == "FAILED":
            raise AirflowException("Job failed. Logs:\n%s" % job.get_logs())
        raise AirflowException("Invalid job status %s. Logs:\n%s" % (job.status, job.get_logs()))

    @task.snowpark()
    def evaluate_model(run_id: str):
        # Run eval job to completion and retrieve result
        job = jobs.submit_directory(
            _PAYLOAD_SOURCE,
            entrypoint="evaluate.py",
            args=["--model_path", f"@HEADLESS_DEMO_DB.DAG_DEMO.MODELS/{run_id}", "--source_data", "HEADLESS_DEMO_DB.DAG_DEMO.DATA_TABLE"],
            compute_pool="SYSTEM_COMPUTE_POOL_GPU",
            stage_name="HEADLESS_DEMO_DB.DAG_DEMO.PAYLOAD_STAGE",
        )

        job.wait()
        job.show_logs()

    run_id = prepare_data()
    job_id = start_training_job(run_id)
    wait_for_completion(job_id) >> evaluate_model(run_id)

single_node_xgb_dag_example()
