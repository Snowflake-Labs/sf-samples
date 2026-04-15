#!/usr/bin/env python3
"""Submit medical SOAP RL training using SPCS managed runtime image.

Usage:
    SNOWFLAKE_DEFAULT_CONNECTION_NAME=preprod8 python scripts/submit_train.py --no-wait
"""
import argparse
import os

from snowflake.snowpark import Session
from snowflake.ml import jobs

PAYLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "src")
RUNTIME_IMAGE_TAG = "2.5.0-test"


def _load_pip_requirements(payload_dir):
    """Read pip requirements from src/requirements.txt."""
    req_file = os.path.join(payload_dir, "requirements.txt")
    reqs = []
    with open(req_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                reqs.append(line)
    return reqs


def submit_training_job(
    session: Session,
    payload_dir: str,
    config: str,
    compute_pool: str,
    external_access_integrations: list,
    database: str = None,
    schema: str = None,
    stage_name: str = "rl_payload_stage",
) -> jobs.MLJob:
    """Submit medical SOAP RL training job."""
    config_path = os.path.join(payload_dir, config)
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    spec_overrides = {
        "spec": {
            "containers": [
                {
                    "name": "main",
                    "resources": {
                        "requests": {"nvidia.com/gpu": 8, "memory": "160Gi"},
                        "limits": {"nvidia.com/gpu": 8, "memory": "320Gi"},
                    },
                    "secrets": [
                        {
                            "snowflakeSecret": {
                                "objectName": "rl_training_db.rl_schema.wandb_api_key_secret",
                            },
                            "secretKeyRef": "secret_string",
                            "envVarName": "WANDB_API_KEY",
                        }
                    ],
                }
            ],
            "volumes": [
                {"name": "dev-shm", "source": "memory", "size": "96Gi"},
            ],
        }
    }

    env_vars = {
        "HF_HOME": "/tmp/hf_local",
        "TRANSFORMERS_CACHE": "/tmp/hf_local",
        "HUGGINGFACE_HUB_CACHE": "/tmp/hf_local",
        "HF_HUB_DISABLE_XET": "1",
        "WANDB_BASE_URL": "https://snowflake.wandb.io",
        "PYTHONUNBUFFERED": "1",
        # Local vLLM judge configuration (2 judges on GPUs 6-7)
        "LOCAL_JUDGE_MODEL": "Qwen/Qwen3-8B",
        "LOCAL_JUDGE_PORTS": "38899,38900",
        "LOCAL_JUDGE_BASE_PORT": "38899",
    }

    pip_requirements = _load_pip_requirements(payload_dir)

    return jobs.submit_directory(
        payload_dir,
        entrypoint=["python", "run_medical_soap.py", "--config", config],
        compute_pool=compute_pool,
        external_access_integrations=external_access_integrations,
        env_vars=env_vars,
        pip_requirements=pip_requirements,
        runtime_environment=RUNTIME_IMAGE_TAG,
        spec_overrides=spec_overrides,
        stage_name=f"{database}.{schema}.{stage_name}" if database and schema else stage_name,
        database=database,
        schema=schema,
        session=session,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit medical SOAP RL training")
    parser.add_argument(
        "-p", "--compute-pool", default="RL_LOCAL_JUDGE_POOL", help="GPU compute pool"
    )
    parser.add_argument("--database", default="RL_TRAINING_DB")
    parser.add_argument("--schema", default="RL_SCHEMA")
    parser.add_argument("--stage-name", default="rl_payload_stage")
    parser.add_argument(
        "-e", "--external-access-integrations",
        nargs="+",
        default=["RL_TRAINING_EAI", "ALLOW_ALL_INTEGRATION"],
    )
    parser.add_argument("-c", "--config", default="config.yaml")
    parser.add_argument("--no-wait", action="store_true", help="Submit and exit")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    connection_name = os.getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
    builder = Session.builder
    if connection_name:
        builder = builder.config("connection_name", connection_name)
    if args.database:
        builder = builder.config("database", args.database)
    if args.schema:
        builder = builder.config("schema", args.schema)
    builder = builder.config("role", "SYSADMIN")
    session = builder.create()

    if args.database:
        session.sql(f"USE DATABASE {args.database}").collect()
    if args.schema:
        session.sql(f"USE SCHEMA {args.schema}").collect()

    pip_reqs = _load_pip_requirements(PAYLOAD_DIR)
    print(f"Submitting medical SOAP RL training (SPCS runtime)...")
    print(f"  Payload dir: {PAYLOAD_DIR}")
    print(f"  Config: {args.config}")
    print(f"  Compute pool: {args.compute_pool}")
    print(f"  Runtime image: {RUNTIME_IMAGE_TAG}")
    print(f"  Pip requirements: {len(pip_reqs)} packages")

    job = submit_training_job(
        session=session,
        payload_dir=PAYLOAD_DIR,
        config=args.config,
        compute_pool=args.compute_pool,
        external_access_integrations=args.external_access_integrations,
        database=args.database,
        schema=args.schema,
        stage_name=args.stage_name,
    )

    print(f"Job submitted: {job.id}")

    if args.no_wait:
        print("--no-wait: job running in background.")
    else:
        print("Waiting for job to complete...")
        status = job.wait()
        print(f"Job finished with status: {status}")
        if args.verbose:
            print(f"\nLogs:\n{job.get_logs()}")
