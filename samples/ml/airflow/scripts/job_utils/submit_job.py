import os
import yaml
import time
from textwrap import dedent
from uuid import uuid4
from pathlib import Path
from typing import List, Optional

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.context import get_active_session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

from spec_utils import prepare_spec

def _get_session() -> Session:
    try:
        return get_active_session()
    except SnowparkSessionException:
        # Initialize Snowflake session
        # See https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections
        # for how to define default connections in a config.toml file
        return Session.builder.configs(SnowflakeLoginOptions("preprod8")).create()

def submit_job(
    compute_pool: str,
    stage_name: str,
    payload_path: Path,
    service_name: Optional[str] = None,
    entrypoint: Optional[Path] = None,
    entrypoint_args: Optional[List[str]] = None,
    external_access_integrations: Optional[List[str]] = None,
    query_warehouse: Optional[str] = None,
    comment: Optional[str] = None,
    dry_run: bool = False,
) -> Optional[str]:
    session = _get_session()
    if not service_name:
        service_name = "JOB_" + str(uuid4()).replace('-', '_').upper()
        print("Generated service name: %s" % service_name)
    if not entrypoint:
        if payload_path.is_file():
            raise ValueError("entrypoint is required if payload_path is not a file")
        entrypoint = payload_path

    spec = prepare_spec(
        session=session,
        service_name=service_name,
        compute_pool=compute_pool,
        stage_name=stage_name,
        payload=payload_path,
        entrypoint=entrypoint,
        args=entrypoint_args,
    )

    query_template = dedent(f"""\
        EXECUTE JOB SERVICE
        IN COMPUTE POOL {compute_pool}
        FROM SPECIFICATION $$
        {{}}
        $$
        NAME = {service_name}
        """)
    query = query_template.format(yaml.dump(spec)).splitlines()
    if external_access_integrations:
        external_access_integration_list = ",".join(
            f"{e}" for e in external_access_integrations
        )
        query.append(
            f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_list})"
        )
    if query_warehouse:
        query.append(f"QUERY_WAREHOUSE = {query_warehouse}")
    if comment:
        query.append(f"COMMENT = {comment}")

    query_text = "\n".join(line for line in query if line)
    if dry_run:
        print("\n================= GENERATED SERVICE SPEC =================")
        print(query_text)
        print("==================== END SERVICE SPEC ====================\n")
        return None

    try:
        async_job = session.sql(query_text).collect_nowait()
        time.sleep(0.1) # Check if query failed "immediately" before exiting
        _ = session.connection.get_query_status_throw_if_error(async_job.query_id)
        return async_job.query_id
    finally:
        session.connection.close()

if __name__ == '__main__':
    import argparse

    default_payload_path = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    default_entrypoint = "train.py"

    parser = argparse.ArgumentParser("SPCS Job Launcher")
    parser.add_argument("-c", "--compute_pool", default="ML_DEMO_POOL", type=str, help="Compute pool to use")
    parser.add_argument("-s", "--stage_name", default="ML_DEMO_STAGE", type=str, help="Stage for payload upload and job artifacts")
    parser.add_argument("-p", "--payload_path", default=default_payload_path, type=Path, help="Path to local payload")
    parser.add_argument("-e", "--entrypoint", default=default_entrypoint, type=Path, help="Relative path to entrypoint in payload")
    parser.add_argument("-a", "--entrypoint_args", type=str, nargs='*', help="Arguments to pass to entrypoint")
    parser.add_argument("-n", "--service_name", type=str, help="Name for created job service")
    parser.add_argument("--external_access_integrations", type=str, nargs='*', help="External access integrations to enable in service")
    parser.add_argument("--query_warehouse", type=str, help="Warehouse to use for queries executed by service")
    parser.add_argument("--comment", type=str, help="Comment to add to created service")
    parser.add_argument("--dry_run", action='store_true', help="Flag to enable dry run mode")
    args = parser.parse_args()

    job_id = submit_job(**vars(args))
    print("Submitted job id: " + (job_id or "<dry run>"))