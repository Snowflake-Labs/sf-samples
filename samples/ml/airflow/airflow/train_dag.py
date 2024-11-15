import os
import re
import time
from datetime import datetime, timedelta, UTC
from textwrap import dedent

from airflow.decorators import dag, task
from airflow.models.taskinstance import TaskInstance
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

@dag(
    schedule=timedelta(weeks=1),
    start_date=datetime(2024, 11, 4, tzinfo=UTC),
    catchup=False,
)
def ml_training_example():
    @task.snowpark()
    def run_training_job(session: Session, task_instance: TaskInstance | None = None):
        service_name = f"ML_DEMO_{task_instance.run_id}"
        service_name = re.sub(r"[\-:+.]", "_", service_name).upper()
        query_template = dedent("""\
            EXECUTE JOB SERVICE
            IN COMPUTE POOL ML_DEMO_POOL
            FROM SPECIFICATION $$
            {spec}
            $$
            NAME = {service_name}
            EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_EAI)
            QUERY_WAREHOUSE = ML_DEMO_WH
            """)

        spec_path = os.path.join(os.path.dirname(__file__), "job_spec.yaml")
        with open(spec_path, "r") as spec_file:
            spec = spec_file.read()
            query = query_template.format(service_name=service_name, spec=spec)

        try:
            session.sql(query).collect()
            return service_name
        finally:
            (logs,) = session.sql(f"CALL SYSTEM$GET_SERVICE_LOGS('{service_name}', '0', 'main', 500)").collect()
            print("Console logs:\n", logs[0])

    job_id = run_training_job()

ml_training_example()
