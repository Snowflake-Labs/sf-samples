#!/usr/bin/env python3
"""Submit the Open MPI LightGBM sample as a native per-instance ML Job."""

from __future__ import annotations

import argparse
import dataclasses
import shutil
import subprocess
import tempfile
from pathlib import Path

from snowflake.ml import jobs
from snowflake.snowpark import Session


SAMPLE_DIR = Path(__file__).resolve().parents[1]
SOURCE_PAYLOAD_DIR = SAMPLE_DIR / "src"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compute-pool", required=True)
    parser.add_argument("--stage-name", default="DISTRIBUTED_TRAINING_STAGE")
    parser.add_argument("--target-instances", type=int, default=2)
    parser.add_argument("--runtime-environment")
    parser.add_argument(
        "--external-access-integration",
        action="append",
        default=[],
        help="EAI used for PyPI and HIGGS access; repeat for multiple EAIs.",
    )
    parser.add_argument("--connection-name")
    parser.add_argument("--database")
    parser.add_argument("--schema")
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--num-iterations", type=int, default=20)
    parser.add_argument(
        "--preflight",
        choices=("none", "wiring"),
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


def create_job_scoped_payload(destination: Path) -> Path:
    payload_dir = destination / "src"
    shutil.copytree(SOURCE_PAYLOAD_DIR, payload_dir)
    key_dir = payload_dir / "keys"
    key_dir.mkdir()
    private_key = key_dir / "id_ed25519"
    try:
        subprocess.run(
            [
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                "snowflake-ml-job-openmpi",
                "-f",
                str(private_key),
            ],
            check=True,
        )
    except FileNotFoundError as error:
        raise RuntimeError(
            "ssh-keygen is required locally to create the job-scoped MPI key."
        ) from error
    private_key.chmod(0o600)
    return payload_dir


def submit(args: argparse.Namespace):
    if args.target_instances < 2:
        raise ValueError("--target-instances must be at least 2 for this sample.")
    if args.max_rows < args.target_instances:
        raise ValueError("--max-rows must provide at least one row per instance.")

    workload_args = [
        "--max-rows",
        str(args.max_rows),
        "--num-iterations",
        str(args.num_iterations),
    ]
    submit_kwargs = {
        "entrypoint": ["bash", "launch.sh"],
        "args": workload_args,
        "compute_pool": args.compute_pool,
        "stage_name": args.stage_name,
        "target_instances": args.target_instances,
        "min_instances": args.target_instances,
        "parallel": True,
        "env_vars": {"PYTHONUNBUFFERED": "1"},
        "external_access_integrations": args.external_access_integration,
        "session": create_session(args),
    }
    if args.preflight != "none":
        submit_kwargs["preflight"] = args.preflight
    if args.runtime_environment:
        submit_kwargs["runtime_environment"] = args.runtime_environment

    with tempfile.TemporaryDirectory(prefix="snowflake-openmpi-") as temp_dir:
        payload_dir = create_job_scoped_payload(Path(temp_dir))
        job = jobs.submit_directory(str(payload_dir), **submit_kwargs)

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
