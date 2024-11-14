import os
import re
from math import ceil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from snowflake.snowpark import Session

# See https://docs.snowflake.com/en/sql-reference/identifiers-syntax for identifier syntax
UNQUOTED_IDENTIFIER_REGEX = r"([a-zA-Z_])([a-zA-Z0-9_$]{0,254})"
QUOTED_IDENTIFIER_REGEX = r'"((""|[^"]){0,255})"'
VALID_IDENTIFIER_REGEX = f"(?:{UNQUOTED_IDENTIFIER_REGEX}|{QUOTED_IDENTIFIER_REGEX})"
_SECRET_IDENTIFIER_REGEX = rf"(?P<fqn>(?:(?:{VALID_IDENTIFIER_REGEX})?[.]{VALID_IDENTIFIER_REGEX}[.])?(?P<name>{VALID_IDENTIFIER_REGEX}))"
_SECRET_CONFIG_REGEX = rf"(?:(?P<mount_path>\w+)=)?{_SECRET_IDENTIFIER_REGEX}(?:[.](?P<subkey>username|password))?"


@dataclass
class _SecretConfig:
    name: str
    fqn: str
    subkey: str
    mount_path: str
    # TODO: Add support for file mount
    mount_type: Literal["environment"] = "environment"


def _parse_secret_config(s: str) -> _SecretConfig:
    m = re.fullmatch(_SECRET_CONFIG_REGEX, s)
    if not m:
        raise ValueError(f"{s} is not a valid secret config string")
    name, fqn = m.group("name"), m.group("fqn")
    subkey = m.group("subkey") or "secret_string"
    mount_path = m.group("mount_path") or name.upper()

    # Validate (inferred) mount_path
    # TODO: Do different validation based on mount type (env var vs directory)
    if not re.fullmatch(r"\w+", mount_path):
        raise ValueError(
            f"Failed to infer secret placement. Please explicitly specify placement in format 'ENV_VAR_NAME=SECRET_NAME'"
        )

    return _SecretConfig(name=name, fqn=fqn, mount_path=mount_path, subkey=subkey)

@dataclass
class _ComputeResources:
    cpu: float  # Number of vCPU cores
    memory: float  # Memory in GiB
    gpu: int = 0  # Number of GPUs
    gpu_type: Optional[str] = None


@dataclass
class _ImageSpec:
    repo: str
    arch: str
    family: str
    tag: str
    resource_requests: _ComputeResources
    resource_limits: _ComputeResources

    @property
    def full_name(self) -> str:
        return f"{self.repo}/st_plat/runtime/{self.arch}/{self.family}:{self.tag}"


# TODO: Query Snowflake for resource information instead of relying on this hardcoded
#       table from https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool
_COMMON_INSTANCE_FAMILIES = {
    "CPU_X64_XS": _ComputeResources(cpu=1, memory=6),
    "CPU_X64_S": _ComputeResources(cpu=3, memory=13),
    "CPU_X64_M": _ComputeResources(cpu=6, memory=28),
    "CPU_X64_L": _ComputeResources(cpu=28, memory=116),
    "HIGHMEM_X64_S": _ComputeResources(cpu=6, memory=58),
}
_AWS_INSTANCE_FAMILIES = {
    "HIGHMEM_X64_M": _ComputeResources(cpu=28, memory=240),
    "HIGHMEM_X64_L": _ComputeResources(cpu=124, memory=984),
    "GPU_NV_S": _ComputeResources(cpu=6, memory=27, gpu=1, gpu_type="A10G"),
    "GPU_NV_M": _ComputeResources(cpu=44, memory=178, gpu=4, gpu_type="A10G"),
    "GPU_NV_L": _ComputeResources(cpu=92, memory=1112, gpu=8, gpu_type="A100"),
}
_AZURE_INSTANCE_FAMILIES = {
    "HIGHMEM_X64_M": _ComputeResources(cpu=28, memory=244),
    "HIGHMEM_X64_L": _ComputeResources(cpu=92, memory=654),
    "GPU_NV_XS": _ComputeResources(cpu=3, memory=26, gpu=1, gpu_type="T4"),
    "GPU_NV_SM": _ComputeResources(cpu=32, memory=424, gpu=1, gpu_type="A10"),
    "GPU_NV_2M": _ComputeResources(cpu=68, memory=858, gpu=2, gpu_type="A10"),
    "GPU_NV_3M": _ComputeResources(cpu=44, memory=424, gpu=2, gpu_type="A100"),
    "GPU_NV_SL": _ComputeResources(cpu=92, memory=858, gpu=4, gpu_type="A100"),
}
_CLOUD_INSTANCE_FAMILIES = {
    "aws": _AWS_INSTANCE_FAMILIES,
    "azure": _AZURE_INSTANCE_FAMILIES,
}


def _get_node_resources(session: Session, compute_pool: str) -> _ComputeResources:
    """Extract resource information for the specified compute pool"""
    # Get the instance family
    (row,) = session.sql(f"show compute pools like '{compute_pool}'").collect()
    instance_family: str = row["instance_family"]

    # Get the cloud we're using (AWS, Azure, etc)
    (row,) = session.sql(f"select current_region()").collect()
    region: str = row[0]
    region_group, region_name = f".{region}".split(".")[
        -2:
    ]  # Prepend a period so we always get at least 2 splits
    regions = session.sql(f"show regions like '{region_name}'").collect()
    if region_group:
        regions = [r for r in regions if r["region_group"] == region_group]
    cloud = regions[0]["cloud"]

    return (
        _COMMON_INSTANCE_FAMILIES.get(instance_family)
        or _CLOUD_INSTANCE_FAMILIES[cloud][instance_family]
    )


def _get_image_spec(session: Session, compute_pool: str) -> _ImageSpec:
    # Retrieve compute pool node resources
    resources = _get_node_resources(session, compute_pool=compute_pool)

    # Use MLRuntime image
    # TODO: Build new image if needed
    image_repo = "/snowflake/images/snowflake_images"
    image_arch = "x86"
    image_family = (
        "generic_gpu/runtime_image/snowbooks"
        if resources.gpu > 0
        else "runtime_image/snowbooks"
    )
    image_tag = "0.4.0"

    # Try to pull latest image tag from server side if possible
    query_result = session.sql(
        f"SHOW PARAMETERS LIKE 'RUNTIME_BASE_IMAGE_TAG' IN ACCOUNT"
    ).collect()
    if query_result:
        image_tag = query_result[0]["value"]

    # TODO: Should each instance consume the entire pod?
    return _ImageSpec(
        repo=image_repo,
        arch=image_arch,
        family=image_family,
        tag=image_tag,
        resource_requests=resources,
        resource_limits=resources,
    )


def _generate_spec(
    image_spec: _ImageSpec,
    stage_path: Path,
    script_path: Path,
    args: Optional[List[str]] = None,
    env_vars: Optional[Dict[str, str]] = None,
    secrets: Optional[List[str]] = None,
) -> dict:
    volumes: List[Dict[str, str]] = []
    volume_mounts: List[Dict[str, str]] = []

    # Set resource requests/limits, including nvidia.com/gpu quantity if applicable
    resource_requests: Dict[str, Union[str, int]] = {
        "cpu": f"{image_spec.resource_requests.cpu * 1000}m",
        "memory": f"{image_spec.resource_limits.memory}Gi",
    }
    resource_limits: Dict[str, Union[str, int]] = {
        "cpu": f"{image_spec.resource_requests.cpu * 1000}m",
        "memory": f"{image_spec.resource_limits.memory}Gi",
    }
    if image_spec.resource_limits.gpu > 0:
        resource_requests["nvidia.com/gpu"] = image_spec.resource_requests.gpu
        resource_limits["nvidia.com/gpu"] = image_spec.resource_limits.gpu

    # Create container spec
    main_container: Dict[str, Any] = {
        "name": "main",
        "image": image_spec.full_name,
        "volumeMounts": volume_mounts,
        "resources": {
            "requests": resource_requests,
            "limits": resource_limits,
        },
    }

    # Add local volumes for ephemeral logs and artifacts
    for volume_name, mount_path in [
        ("system-logs", "/var/log/managedservices/system/mlrs"),
        ("user-logs", "/var/log/managedservices/user/mlrs"),
    ]:
        volume_mounts.append(
            {
                "name": volume_name,
                "mountPath": mount_path,
            }
        )
        volumes.append(
            {
                "name": volume_name,
                "source": "local",
            }
        )

    # Mount 30% of memory limit as a memory-backed volume
    memory_volume_name = "dshm"
    memory_volume_size = min(
        ceil(image_spec.resource_limits.memory * 0.3),
        image_spec.resource_requests.memory,
    )
    volume_mounts.append(
        {
            "name": memory_volume_name,
            "mountPath": "/dev/shm",
        }
    )
    volumes.append(
        {
            "name": memory_volume_name,
            "source": "memory",
            "size": f"{memory_volume_size}Gi",
        }
    )

    # Mount payload as volume
    stage_mount = "/opt/app"
    stage_volume_name = "stage-volume"
    volume_mounts.append(
        {
            "name": stage_volume_name,
            "mountPath": stage_mount,
        }
    )
    volumes.append(
        {
            "name": stage_volume_name,
            "source": str(stage_path),
        }
    )

    # TODO: Add hooks for endpoints for integration with TensorBoard, W&B, etc

    # Propagate user payload config
    commands = {
        ".py": "python",
        ".sh": "bash",
        ".rb": "ruby",
        ".pl": "perl",
        ".js": "node",
        # Add more formats as needed
    }
    command = commands[script_path.suffix]
    main_container["command"] = [
        command,
        os.path.join(stage_mount, script_path),
        *(args or []),
    ]

    if env_vars:
        main_container["env"] = env_vars

    if secrets:
        secrets_spec = []
        for s in secrets:
            # TODO: Add support for other secret types (e.g. username/password)
            # TODO: Add support for other mount types
            secret = _parse_secret_config(s)
            assert secret.mount_type == "environment"
            secrets_spec.append(
                {
                    "snowflakeSecret": secret.fqn,
                    "envVarName": secret.mount_path,
                    "secretKeyRef": secret.subkey,
                }
            )
        main_container["secrets"] = secrets_spec

    return {
        "spec": {
            "containers": [main_container],
            "volumes": volumes,
        }
    }


def _prepare_payload(
    session: Session,
    stage_path: Path,
    source: Path,
    entrypoint: Path,
) -> Path:
    """Load payload onto stage"""
    # TODO: Detect if source is a git repo or existing stage
    if not entrypoint.exists():
        entrypoint = source / entrypoint
    if not (source.exists() and entrypoint.exists()):
        raise FileNotFoundError(f"{source} or {entrypoint} does not exist")

    # Create stage if necessary
    stage_name = stage_path.parts[0]
    session.sql(f"create stage if not exists {stage_name.lstrip('@')}").collect()

    # Upload payload to stage
    if source.is_dir():
        # Filter to only files in source since Snowflake PUT can't handle directories
        for path in set(
            p.parent.joinpath(f"*{p.suffix}") if p.suffix else p
            for p in source.rglob("*")
            if p.is_file()
        ):
            session.file.put(
                str(path.resolve()),
                str(stage_path.joinpath(path.parent.relative_to(source))),
                overwrite=True,
                auto_compress=False,
            )
    else:
        session.file.put(
            str(source.resolve()),
            str(stage_path),
            overwrite=True,
            auto_compress=False,
        )

    return entrypoint.relative_to(source)


def prepare_spec(
    session: Session,
    service_name: str,
    compute_pool: str,
    stage_name: str,
    payload: Path,
    entrypoint: Path,
    args: Optional[List[str]] = None,
    secrets: Optional[List[str]] = None,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:

    # Generate image spec based on compute pool
    image_spec = _get_image_spec(session, compute_pool=compute_pool)

    # Prepare payload
    stage_path = Path(f"@{stage_name}/{service_name}")
    script_path = _prepare_payload(
        session,
        stage_path,
        source=payload,
        entrypoint=entrypoint,
    )

    spec = _generate_spec(
        image_spec=image_spec,
        stage_path=stage_path,
        script_path=script_path,
        args=args,
        env_vars=env,
        secrets=secrets,
    )
    return spec
