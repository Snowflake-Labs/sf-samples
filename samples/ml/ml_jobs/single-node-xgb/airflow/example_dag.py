import json
from datetime import datetime, timedelta, UTC

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from snowflake.ml.jobs import manager as job_manager

@dag(
    schedule=timedelta(weeks=1),
    start_date=datetime(2024, 11, 4, tzinfo=UTC),
    catchup=False,
)
def future_ml_training_mockup():
    """
    This is **not** a working sample!! This is a mockup of what a DAG for
    MLOps in Snowflake may look like in the future. `snowflake.ml.jobs`
    does not currently exist and the APIs below are only for illustrative
    purposes.
    """

    @task.snowpark()
    def prepare_data():
        # Kick off preprocessing job on SPCS
        job = job_manager.submit_job(
            source="/path/to/repo",
            entrypoint="src/prepare_data.py",
            args=["--input_table", "DB.SCHEMA.MY_TABLE", "--output_path", "@DB.SCHEMA.MY_STAGE/"],
            compute_pool="cpu_pool",
            stage_name="DB.SCHEMA.PAYLOAD_STAGE",
        )

        # Block until job completes
        job.wait()

        # Print logs for observability
        job.show_logs()

        return job.id

    @task.snowpark()
    def start_training_job(run_id: str, model_config: dict):
        job = job_manager.submit_job(
            source="/path/to/repo",
            entrypoint="src/train_model.py",
            args=[
                "--input_data", f"@DB.SCHEMA.MY_STAGE/{run_id}",
                "--output_path", f"@DB.SCHEMA.MODELS/{run_id}/{job_id}",
                "--model_config", json.dumps(model_config),
            ],
            compute_pool="gpu_pool",
            stage_name="DB.SCHEMA.PAYLOAD_STAGE",
            num_instances=4,
            external_access_integrations=["pypi_eai"],
        )

        return job.id

    @task.snowpark_sensor(poke_interval=60, timeout=7200, mode="reschedule")
    def wait_for_completion(job_id: str) -> bool:
        job = job_manager.MLJob(job_id)
        if job.status == "COMPLETE":
            print("Job completed. Logs:\n%s" % job.get_logs())
            return True
        elif job.status == "FAILED":
            raise AirflowException("Job failed. Logs:\n%s" % job.get_logs())
        return False

    @task.snowpark()
    def evaluate_model(run_id: str, model_id: str):
        # Run eval job to completion and retrieve result
        job = job_manager.submit_job(
            source="/path/to/repo",
            entrypoint="src/evaluate_model.py",
            args=["--model_path", f"@DB.SCHEMA.MODELS/{run_id}/{model_id}", "--eval_data", "DB.SCHEMA.EVAL_DATA"],
            compute_pool="gpu_pool",
            num_instances=1,
            external_access_integrations=["pypi_eai"],
        )

        job.wait()
        job.show_logs()

    run_id = prepare_data()
    configs = Variable.get("model_configs", deserialize_json=True)
    for config in configs:
        job_id = start_training_job(run_id, config)
        wait_for_completion(job_id) >> evaluate_model(run_id, job_id)

future_ml_training_mockup()
