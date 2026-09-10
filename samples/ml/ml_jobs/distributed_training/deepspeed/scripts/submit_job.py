#!/usr/bin/env python3
"""Submit the DeepSpeed no-SSH sample as a native per-instance ML Job."""

from __future__ import annotations

import argparse
import dataclasses
from pathlib import Path

from snowflake.ml import jobs
from snowflake.snowpark import Session


SAMPLE_DIR = Path(__file__).resolve().parents[1]
PAYLOAD_DIR = SAMPLE_DIR / "src"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compute-pool", required=True)
    parser.add_argument("--stage-name", default="DISTRIBUTED_TRAINING_STAGE")
    parser.add_argument("--target-instances", type=int, default=2)
    parser.add_argument(
        "--external-access-integration",
        action="append",
        default=[],
        help="EAI used for PyPI and Hugging Face access; repeat for multiple EAIs.",
    )
    parser.add_argument("--connection-name")
    parser.add_argument("--database")
    parser.add_argument("--schema")
    parser.add_argument("--runtime-environment")
    parser.add_argument("--nproc-per-node", type=int)
    parser.add_argument(
        "--preflight",
        choices=("none", "wiring", "reference"),
        default="wiring",
    )
    parser.add_argument("--no-wait", action="store_true")
    return parser.parse_args()


def create_session(args: argparse.Namespace) -> Session:
    builder = Session.builder
    if args.connection_name:
        builder = builder.config("connection_name", args.connection_name)
    if args.database:
        builder = builder.config("database", args.database)
    if args.schema:
        builder = builder.config("schema", args.schema)
    return builder.create()


def submit(args: argparse.Namespace):
    if args.target_instances < 2:
        raise ValueError("--target-instances must be at least 2 for this sample.")

    env_vars = {
        "HF_HOME": "/tmp/huggingface",
        "HF_HUB_CACHE": "/tmp/huggingface/hub",
        "HF_DATASETS_CACHE": "/tmp/huggingface/datasets",
        "HF_HUB_DISABLE_XET": "1",
        "PYTHONUNBUFFERED": "1",
    }
    if args.nproc_per_node is not None:
        env_vars["NPROC_PER_NODE"] = str(args.nproc_per_node)

    submit_kwargs = {
        "entrypoint": ["bash", "launch.sh"],
        "compute_pool": args.compute_pool,
        "stage_name": args.stage_name,
        "target_instances": args.target_instances,
        "min_instances": args.target_instances,
        "parallel": True,
        "env_vars": env_vars,
        "external_access_integrations": args.external_access_integration,
        "session": create_session(args),
    }
    if args.preflight != "none":
        submit_kwargs["preflight"] = args.preflight
    if args.runtime_environment:
        submit_kwargs["runtime_environment"] = args.runtime_environment

    job = jobs.submit_directory(str(PAYLOAD_DIR), **submit_kwargs)
    print(f"Submitted job: {job.id}")
    if args.no_wait:
        return job

    try:
        result = job.distributed_result()
    except jobs.DistributedJobError as error:
        if error.result is not None:
            print(f"Distributed result: {dataclasses.asdict(error.result)}")
            failed_instance = error.result.failed_instance
        else:
            failed_instance = None
        if failed_instance is not None:
            print(
                f"Instance {failed_instance} failed. View its logs with: "
                f'get_job("{job.id}").get_logs(instance_id={failed_instance}, verbose=True)'
            )
        raise

    print(f"Distributed result: {dataclasses.asdict(result)}")
    return job


if __name__ == "__main__":
    submit(parse_args())
