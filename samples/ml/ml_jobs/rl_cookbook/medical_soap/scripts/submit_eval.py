#!/usr/bin/env python3
"""Submit medical SOAP evaluation job using SPCS managed runtime image.

Usage:
    SNOWFLAKE_DEFAULT_CONNECTION_NAME=preprod8 python scripts/submit_eval.py \
        --models "cortex:claude-sonnet-4-6,hf:Qwen/Qwen3-1.7B" \
        --num-samples 100 \
        --no-wait
"""
import argparse
import os

from snowflake.snowpark import Session
from snowflake.ml import jobs

PAYLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "src")
RUNTIME_IMAGE_TAG = "2.4.1-thong-pytorch-29"


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


def submit_eval_job(
    session: Session,
    payload_dir: str,
    compute_pool: str,
    external_access_integrations: list,
    models: str,
    num_samples: int = 100,
    checkpoint_path: str = "",
    database: str = None,
    schema: str = None,
    stage_name: str = "rl_payload_stage",
) -> jobs.MLJob:
    """Submit medical SOAP evaluation job."""
    spec_overrides = {
        "spec": {
            "containers": [
                {
                    "name": "main",
                    "resources": {
                        "requests": {"nvidia.com/gpu": 4, "memory": "80Gi"},
                        "limits": {"nvidia.com/gpu": 4, "memory": "160Gi"},
                    },
                }
            ],
            "volumes": [
                {"name": "dev-shm", "source": "memory", "size": "16Gi"},
            ],
        }
    }

    env_vars = {
        "HF_HOME": "/tmp/hf_local",
        "TRANSFORMERS_CACHE": "/tmp/hf_local",
        "HUGGINGFACE_HUB_CACHE": "/tmp/hf_local",
        "HF_HUB_DISABLE_XET": "1",
        "PYTHONUNBUFFERED": "1",
        "EVAL_MODELS": models,
        "NUM_SAMPLES": str(num_samples),
    }

    if checkpoint_path:
        env_vars["CHECKPOINT_STAGE_PATH"] = checkpoint_path

    pip_requirements = _load_pip_requirements(payload_dir)

    return jobs.submit_directory(
        payload_dir,
        entrypoint=["python", "eval_medical_soap.py"],
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
    parser = argparse.ArgumentParser(description="Submit medical SOAP evaluation job")
    parser.add_argument(
        "--models", required=True,
        help="Comma-separated model specs, e.g. 'cortex:claude-sonnet-4-6,hf:Qwen/Qwen3-1.7B'",
    )
    parser.add_argument("--num-samples", type=int, default=100, help="Number of test samples")
    parser.add_argument("--checkpoint-path", default="", help="Stage path for checkpoint model")
    parser.add_argument(
        "-p", "--compute-pool", default="RL_EVAL_A10_POOL", help="GPU compute pool",
    )
    parser.add_argument("--database", default="RL_TRAINING_DB")
    parser.add_argument("--schema", default="RL_SCHEMA")
    parser.add_argument("--stage-name", default="rl_payload_stage")
    parser.add_argument(
        "-e", "--external-access-integrations",
        nargs="+",
        default=["RL_TRAINING_EAI", "ALLOW_ALL_INTEGRATION"],
    )
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
    print(f"Submitting medical SOAP evaluation job (SPCS runtime)...")
    print(f"  Payload dir: {PAYLOAD_DIR}")
    print(f"  Models: {args.models}")
    print(f"  Num samples: {args.num_samples}")
    print(f"  Compute pool: {args.compute_pool}")
    print(f"  Runtime image: {RUNTIME_IMAGE_TAG}")
    print(f"  Pip requirements: {len(pip_reqs)} packages")
    if args.checkpoint_path:
        print(f"  Checkpoint path: {args.checkpoint_path}")

    job = submit_eval_job(
        session=session,
        payload_dir=PAYLOAD_DIR,
        compute_pool=args.compute_pool,
        external_access_integrations=args.external_access_integrations,
        models=args.models,
        num_samples=args.num_samples,
        checkpoint_path=args.checkpoint_path,
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
