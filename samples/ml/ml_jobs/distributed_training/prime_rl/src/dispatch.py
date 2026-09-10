#!/usr/bin/env python3
"""Map two ML Job instances to PrimeRL inference and training roles."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import sysconfig
import threading
import time
import urllib.request
from pathlib import Path

OUTPUT_ROOT = Path("output/prime_rl")
LOCAL_ROOT = Path("/tmp/snowflake-prime-rl")
PRIME_RL_ROOT = Path("/tmp/prime-rl")
PRIME_RL_COMMIT = "9e3be00b39df5c4f917a95d8296e6239a6179a35"
GPUS_PER_ROLE_NODE = 1
ROLE_CUDA_VISIBLE_DEVICES = ",".join(str(index) for index in range(GPUS_PER_ROLE_NODE))
CONTROL_FD_ENV = "SNOWFLAKE_PRIME_RL_CONTROL_FD"


class PrimeRLChildProcessError(subprocess.CalledProcessError):
    """The locked-environment child already handled peer notification."""


def current_python_script(name: str) -> str | None:
    """Prefer a console script installed alongside the running Python."""
    candidate = Path(sysconfig.get_path("scripts")) / name
    if candidate.is_file() and os.access(candidate, os.X_OK):
        return str(candidate)
    return shutil.which(name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--model-name",
        default="PrimeIntellect/Qwen3-0.6B-Reverse-Text-SFT",
    )
    parser.add_argument(
        "--model-revision",
        default="c97a910849ec6aa962add3dc253a0817d61c0210",
    )
    parser.add_argument("--max-steps", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--group-size", type=int, default=16)
    parser.add_argument("--sequence-length", type=int, default=2048)
    parser.add_argument("--max-completion-tokens", type=int, default=128)
    return parser.parse_args()


def topology() -> tuple[int, list[str], dict[str, int]]:
    required = (
        "SNOWFLAKE_JOB_INDEX",
        "SNOWFLAKE_JOBS_COUNT",
        "MLRS_NODE_IPS",
        "MLRS_EPHEMERAL_PORT_MIN",
        "MLRS_EPHEMERAL_PORT_MAX",
    )
    missing = [name for name in required if name not in os.environ]
    if missing:
        raise RuntimeError("Missing ML Job topology: " + ", ".join(missing))

    instance_index = int(os.environ["SNOWFLAKE_JOB_INDEX"])
    instance_count = int(os.environ["SNOWFLAKE_JOBS_COUNT"])
    node_ips = os.environ["MLRS_NODE_IPS"].split(",")
    if instance_count != 2 or len(node_ips) != 2:
        raise RuntimeError("This reference topology requires exactly two instances.")

    port_min = int(os.environ["MLRS_EPHEMERAL_PORT_MIN"])
    port_max = int(os.environ["MLRS_EPHEMERAL_PORT_MAX"])
    port_names = (
        "inference",
        "weight_broadcast",
        "rollout_transport",
        "env_server",
        "trainer_rendezvous",
        "control",
    )
    ports = {name: port_min + offset for offset, name in enumerate(port_names)}
    if ports["control"] > port_max:
        raise RuntimeError("PrimeRL requires at least six exported ephemeral ports.")
    return instance_index, node_ips, ports


def interface_for_ip(local_ip: str) -> str:
    """Return the Linux interface that owns an exported ML Job node IP."""
    request_code = 0x8915  # Linux SIOCGIFADDR.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        for _, interface in socket.if_nameindex():
            request = struct.pack("256s", interface[:15].encode("utf-8"))
            try:
                response = fcntl.ioctl(probe.fileno(), request_code, request)
            except OSError:
                continue
            if socket.inet_ntoa(response[20:24]) == local_ip:
                return interface
    raise RuntimeError(
        f"Could not find the network interface for exported node IP {local_ip}."
    )


def configure_network_environment(local_ip: str) -> None:
    interface = None
    if "NCCL_SOCKET_IFNAME" not in os.environ or "GLOO_SOCKET_IFNAME" not in os.environ:
        interface = interface_for_ip(local_ip)
    defaults = {
        "NCCL_DEBUG": "WARN",
        "TORCH_NCCL_ASYNC_ERROR_HANDLING": "1",
        "TORCH_NCCL_BLOCKING_WAIT": "1",
        "TORCH_DISABLE_COALESCED_COLLECTIVES": "1",
        "TORCH_NCCL_USE_TENSOR_REGISTER_ALLOCATOR_HOOK": "0",
    }
    if interface is not None:
        defaults["NCCL_SOCKET_IFNAME"] = interface
        defaults["GLOO_SOCKET_IFNAME"] = interface
    for name, value in defaults.items():
        os.environ.setdefault(name, value)
    print(
        f"[network] local_ip={local_ip} detected_interface={interface or 'explicit'} "
        f"nccl_interface={os.environ['NCCL_SOCKET_IFNAME']} "
        f"gloo_interface={os.environ['GLOO_SOCKET_IFNAME']}",
        flush=True,
    )


def validate_gpu_topology() -> None:
    nvidia_smi = shutil.which("nvidia-smi")
    if nvidia_smi is None:
        raise RuntimeError("nvidia-smi is required to validate the GPU topology.")
    result = subprocess.run(
        [
            nvidia_smi,
            "--query-gpu=index,name",
            "--format=csv,noheader",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    devices = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    visible_gpus = len(devices)
    if visible_gpus < GPUS_PER_ROLE_NODE:
        raise RuntimeError(
            f"This PrimeRL topology requires {GPUS_PER_ROLE_NODE} visible GPUs per "
            f"ML Job instance, but found {visible_gpus}. Use GPU_NV_S or another "
            "GPU instance family."
        )
    print(
        f"[gpu] visible={visible_gpus} using={ROLE_CUDA_VISIBLE_DEVICES} "
        f"devices={devices[:GPUS_PER_ROLE_NODE]}",
        flush=True,
    )


def toml_string(value: str) -> str:
    return json.dumps(value)


def ensure_prime_rl_environment(*, install_flash_attn: bool, control_fd: int) -> bool:
    """Run the dispatcher in PrimeRL's locked environment when needed.

    Returns ``False`` inside the locked environment so role execution can
    continue. The bootstrap interpreter waits for that child and returns
    ``True`` so the ML Job launcher's in-process Python entrypoint can finish
    normally and persist its per-instance result record.
    """
    if os.environ.get("SNOWFLAKE_PRIME_RL_BOOTSTRAPPED") == "1":
        return False
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError(
            "PrimeRL requires Python 3.12. Select a Python 3.12 Snowflake Container Runtime."
        )

    git = shutil.which("git")
    uv = current_python_script("uv")
    if git is None or uv is None:
        raise RuntimeError("The PrimeRL bootstrap requires git and uv.")

    uv_version = subprocess.run(
        [uv, "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    print(f"[bootstrap] using {uv_version} from {uv}", flush=True)

    print(f"[bootstrap] cloning PrimeRL at {PRIME_RL_COMMIT}", flush=True)
    subprocess.run(
        [
            git,
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            "https://github.com/PrimeIntellect-ai/prime-rl.git",
            str(PRIME_RL_ROOT),
        ],
        check=True,
    )
    subprocess.run(
        [git, "-C", str(PRIME_RL_ROOT), "checkout", PRIME_RL_COMMIT],
        check=True,
    )
    https_submodules = {
        "verifiers": "https://github.com/PrimeIntellect-ai/verifiers.git",
        "renderers": "https://github.com/PrimeIntellect-ai/renderers.git",
        "prime-envs": "https://github.com/PrimeIntellect-ai/prime-envs.git",
        "pydantic-config": "https://github.com/PrimeIntellect-ai/pydantic-config.git",
        "prime-kernels": "https://github.com/PrimeIntellect-ai/prime-kernels.git",
    }
    for name, url in https_submodules.items():
        subprocess.run(
            [
                git,
                "-C",
                str(PRIME_RL_ROOT),
                "config",
                f"submodule.{name}.url",
                url,
            ],
            check=True,
        )
    subprocess.run(
        [
            git,
            "-C",
            str(PRIME_RL_ROOT),
            "submodule",
            "update",
            "--init",
            "--recursive",
            "--depth",
            "1",
        ],
        check=True,
    )
    sync_command = [
        uv,
        "sync",
        "--frozen",
        "--extra",
        "gpu",
    ]
    if install_flash_attn:
        # The trainer instance also needs the separately locked flash-attn
        # extra; ring-flash-attn imports flash_attn at startup.
        sync_command.extend(("--extra", "flash-attn"))
    sync_command.extend(
        (
            "--package",
            "prime-rl",
            "--package",
            "reverse-text",
            "--no-dev",
        )
    )
    subprocess.run(sync_command, cwd=PRIME_RL_ROOT, check=True)

    venv_python = PRIME_RL_ROOT / ".venv" / "bin" / "python"
    if not venv_python.is_file():
        raise RuntimeError("PrimeRL bootstrap did not create its locked environment.")
    if install_flash_attn:
        subprocess.run(
            [
                venv_python,
                "-c",
                (
                    "import flash_attn; "
                    "print(f'[bootstrap] flash-attn {flash_attn.__version__} "
                    "import succeeded', flush=True)"
                ),
            ],
            check=True,
        )
    environment = {
        **os.environ,
        "PATH": f"{venv_python.parent}:{os.environ.get('PATH', '')}",
        CONTROL_FD_ENV: str(control_fd),
        "SNOWFLAKE_PRIME_RL_BOOTSTRAPPED": "1",
    }
    # The ML Job launcher can inject its system paths here. Do not let runtime
    # packages shadow the versions installed in PrimeRL's locked environment.
    environment.pop("PYTHONPATH", None)
    script = Path(__file__).resolve()
    command = [str(venv_python), str(script), *sys.argv[1:]]
    result = subprocess.run(command, env=environment, pass_fds=(control_fd,))
    if result.returncode != 0:
        # subprocess uses negative return codes for signals. Normalize those
        # to the conventional shell status before the ML Job launcher records
        # this instance's outcome.
        exit_code = (
            result.returncode
            if result.returncode > 0
            else 128 - result.returncode
        )
        raise PrimeRLChildProcessError(exit_code, command)
    return True


def prepare_resolved_configs(
    args: argparse.Namespace,
    trainer_ip: str,
    inference_ip: str,
    ports: dict[str, int],
) -> tuple[Path, Path, Path, list[Path]]:
    from huggingface_hub import snapshot_download

    model_path = snapshot_download(
        repo_id=args.model_name,
        revision=args.model_revision,
    )
    print(f"[assets] model={args.model_name}@{args.model_revision} -> {model_path}")

    LOCAL_ROOT.mkdir(parents=True, exist_ok=True)
    template = Path("rl.toml").read_text(encoding="utf-8")
    template = template.replace(
        'name = "PrimeIntellect/Qwen3-0.6B-Reverse-Text-SFT"',
        f"name = {toml_string(model_path)}",
        1,
    )
    effective_config = LOCAL_ROOT / "rl.effective.toml"
    effective_config.write_text(template, encoding="utf-8")

    run_name = "snowflake-prime-rl"
    command = [
        "rl",
        "@",
        str(effective_config),
        "--output-dir",
        str(LOCAL_ROOT / "run"),
        "--run.name",
        run_name,
        "--max-steps",
        str(args.max_steps),
        "--orchestrator.batch-size",
        str(args.batch_size),
        "--orchestrator.group-size",
        str(args.group_size),
        "--seq-len",
        str(args.sequence_length),
        "--inference.server.host",
        "0.0.0.0",
        "--inference.server.port",
        str(ports["inference"]),
        "--inference.router",
        "None",
        "--inference.vllm.max-model-len",
        str(args.sequence_length),
        "--orchestrator.model.client.base-url",
        f"http://{inference_ip}:{ports['inference']}/v1",
        "--orchestrator.model.client.admin-base-url",
        json.dumps([f"http://{inference_ip}:{ports['inference']}/v1"]),
        "--weight_broadcast.host",
        trainer_ip,
        "--weight_broadcast.port",
        str(ports["weight_broadcast"]),
        "--rollout_transport.host",
        trainer_ip,
        "--rollout_transport.port",
        str(ports["rollout_transport"]),
        "--orchestrator.env-server-base-port",
        str(ports["env_server"]),
    ]
    if args.max_completion_tokens is not None:
        command.extend(
            (
                "--orchestrator.train.sampling.max-completion-tokens",
                str(args.max_completion_tokens),
            )
        )
    command.append("--dry-run")
    print("[config] " + " ".join(command), flush=True)
    subprocess.run(command, check=True)

    resolved = LOCAL_ROOT / "run" / run_name / "configs" / "resolved"
    trainer_config = resolved / "trainer.json"
    orchestrator_config = resolved / "orchestrator.json"
    inference_config = resolved / "inference.json"
    env_configs = sorted((resolved / "envs" / "train").glob("*.json"))
    expected = [trainer_config, orchestrator_config, inference_config]
    if not all(path.is_file() for path in expected) or not env_configs:
        raise RuntimeError(f"PrimeRL dry-run did not create expected configs under {resolved}.")
    return trainer_config, orchestrator_config, inference_config, env_configs


class ManagedProcess:
    def __init__(self, name: str, command: list[str], env: dict[str, str], log_dir: Path):
        self.name = name
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = log_dir / f"{name}.log"
        self.log_file = self.log_path.open("w", encoding="utf-8")
        print(f"[dispatch] starting {name}: {' '.join(command)}", flush=True)
        try:
            self.process = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
        except Exception:
            self.log_file.close()
            raise
        self.thread = threading.Thread(target=self._tee, daemon=True)
        self.thread.start()

    def _tee(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.log_file.write(line)
            self.log_file.flush()
            print(f"[{self.name}] {line}", end="", flush=True)

    def poll(self) -> int | None:
        return self.process.poll()

    def wait(self) -> int:
        status = self.process.wait()
        self.thread.join(timeout=10)
        self.log_file.close()
        return status

    def terminate(self) -> None:
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)

    def kill(self) -> None:
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGKILL)
        self.wait()


def resolved_environment(config_path: Path) -> dict[str, str]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    env_vars = config.get("env_vars", {})
    if not isinstance(env_vars, dict) or not all(
        isinstance(name, str) and isinstance(value, str)
        for name, value in env_vars.items()
    ):
        raise ValueError(f"Invalid env_vars in {config_path}.")
    return env_vars


def component_environment(
    run_id: str,
    label: str,
    *overrides: dict[str, str],
) -> dict[str, str]:
    from prime_rl.utils.process import DEFAULT_COMMON_ENV_VARS

    environment = {
        **os.environ,
        **DEFAULT_COMMON_ENV_VARS,
        "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
        "GIT_LFS_SKIP_SMUDGE": "1",
        "OMP_NUM_THREADS": "1",
        "PYTHONUNBUFFERED": "1",
    }
    for override in overrides:
        environment.update(override)
    environment.update(
        {
            "PRL_RUN_ID": run_id,
            "PRL_RUN_NAME": "snowflake-prime-rl",
            "WANDB_RUN_ID": run_id,
            "WANDB_SHARED_MODE": "1",
            "WANDB_SHARED_LABEL": label,
            "WANDB_MODE": "disabled",
        }
    )
    return environment


def open_control_server(port: int) -> socket.socket:
    inherited_fd = os.environ.get(CONTROL_FD_ENV)
    if inherited_fd is not None:
        server = socket.socket(fileno=int(inherited_fd))
        server.settimeout(2)
        return server

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen()
    server.settimeout(2)
    return server


def send_completion(host: str, port: int, status: int, timeout: float = 600) -> None:
    deadline = time.monotonic() + timeout
    payload = json.dumps({"status": status}).encode("utf-8")
    while True:
        try:
            with socket.create_connection((host, port), timeout=5) as connection:
                connection.sendall(payload)
            print(f"[control] sent status={status} to peer role", flush=True)
            return
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)


def wait_for_inference(
    inference_ip: str,
    inference_port: int,
    control_server: socket.socket,
    timeout: float = 4200,
) -> None:
    urls = (
        f"http://{inference_ip}:{inference_port}/health",
        f"http://{inference_ip}:{inference_port}/v1/models",
    )
    deadline = time.monotonic() + timeout
    last_errors: dict[str, str] = {}
    while True:
        ready = True
        for url in urls:
            try:
                with urllib.request.urlopen(url, timeout=5) as response:
                    if not 200 <= response.status < 300:
                        raise RuntimeError(f"HTTP {response.status}")
            except Exception as error:
                last_errors[url] = str(error)
                ready = False
        if ready:
            print(f"[readiness] inference is ready at {inference_ip}", flush=True)
            return

        try:
            connection, address = control_server.accept()
        except socket.timeout:
            pass
        else:
            with connection:
                message = json.loads(connection.recv(1024).decode("utf-8"))
            status = int(message["status"]) or 1
            raise RuntimeError(
                f"Inference instance {address[0]} failed before readiness "
                f"with status {status}."
            )

        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Inference did not become ready within {timeout:.0f} seconds: "
                f"{last_errors}"
            )
        time.sleep(3)


def stop_processes(processes: list[ManagedProcess]) -> None:
    for process in processes:
        process.terminate()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and any(p.poll() is None for p in processes):
        time.sleep(1)
    for process in processes:
        if process.poll() is None:
            process.kill()
        else:
            process.wait()


def copy_role_output(role: str) -> None:
    destination = OUTPUT_ROOT / role
    destination.mkdir(parents=True, exist_ok=True)
    if LOCAL_ROOT.exists():
        shutil.copytree(LOCAL_ROOT, destination, dirs_exist_ok=True)


def run_inference(
    inference_config: Path,
    run_id: str,
    control_server: socket.socket,
    trainer_ip: str,
    control_port: int,
) -> int:
    from prime_rl.utils.process import DEFAULT_INFERENCE_ENV_VARS

    log_dir = LOCAL_ROOT / "component_logs"
    environment = component_environment(
        run_id,
        "inference",
        DEFAULT_INFERENCE_ENV_VARS,
        resolved_environment(inference_config),
        {"CUDA_VISIBLE_DEVICES": ROLE_CUDA_VISIBLE_DEVICES},
    )
    inference = ManagedProcess(
        "inference",
        ["inference", "@", str(inference_config)],
        environment,
        log_dir,
    )
    status = 1
    try:
        while True:
            process_status = inference.poll()
            if process_status is not None:
                print(
                    f"[dispatch] inference exited before training completed: {process_status}",
                    flush=True,
                )
                status = process_status or 1
                send_completion(trainer_ip, control_port, status)
                break
            try:
                connection, address = control_server.accept()
            except socket.timeout:
                continue
            with connection:
                message = json.loads(connection.recv(1024).decode("utf-8"))
            status = int(message["status"])
            print(f"[control] completion from {address[0]}: status={status}", flush=True)
            break
    finally:
        inference.terminate()
        try:
            inference.wait()
        except Exception:
            inference.kill()
        copy_role_output("inference")
    return status


def run_training_roles(
    trainer_config: Path,
    orchestrator_config: Path,
    env_configs: list[Path],
    trainer_ip: str,
    inference_ip: str,
    ports: dict[str, int],
    run_id: str,
    control_server: socket.socket,
) -> int:
    from prime_rl.utils.process import DEFAULT_TRAINER_ENV_VARS

    log_dir = LOCAL_ROOT / "component_logs"
    common_environment = component_environment(run_id, "support")
    processes: list[ManagedProcess] = []
    try:
        wait_for_inference(
            inference_ip,
            ports["inference"],
            control_server,
        )
        for index, env_config in enumerate(env_configs):
            processes.append(
                ManagedProcess(
                    f"env-server-{index}",
                    ["env-server", "@", str(env_config)],
                    common_environment,
                    log_dir,
                )
            )
        orchestrator_environment = component_environment(
            run_id,
            "orchestrator",
            resolved_environment(orchestrator_config),
        )
        orchestrator = ManagedProcess(
            "orchestrator",
            ["orchestrator", "@", str(orchestrator_config)],
            orchestrator_environment,
            log_dir,
        )
        processes.append(orchestrator)

        trainer_environment = component_environment(
            run_id,
            "trainer",
            DEFAULT_TRAINER_ENV_VARS,
            resolved_environment(trainer_config),
            {"CUDA_VISIBLE_DEVICES": ROLE_CUDA_VISIBLE_DEVICES},
        )
        trainer = ManagedProcess(
            "trainer",
            [
                "torchrun",
                "--role=trainer",
                "--nnodes=1",
                f"--nproc-per-node={GPUS_PER_ROLE_NODE}",
                f"--rdzv-endpoint={trainer_ip}:{ports['trainer_rendezvous']}",
                f"--rdzv-id={run_id}",
                "-m",
                "prime_rl.trainer.rl.train",
                "@",
                str(trainer_config),
            ],
            trainer_environment,
            log_dir,
        )
        processes.append(trainer)
    except Exception:
        stop_processes(processes)
        copy_role_output("trainer_orchestrator")
        raise

    terminal = (trainer, orchestrator)
    status = 0
    remote_failure = False
    try:
        while True:
            try:
                connection, address = control_server.accept()
            except socket.timeout:
                pass
            else:
                with connection:
                    message = json.loads(connection.recv(1024).decode("utf-8"))
                status = int(message["status"]) or 1
                remote_failure = True
                print(
                    f"[control] inference failure from {address[0]}: status={status}",
                    flush=True,
                )
                break

            support_failure = next(
                (
                    process
                    for process in processes[:-2]
                    if process.poll() is not None
                ),
                None,
            )
            if support_failure is not None:
                status = support_failure.poll() or 1
                print(f"[dispatch] {support_failure.name} exited early: {status}")
                break

            failed_terminal = next(
                (
                    process
                    for process in terminal
                    if process.poll() not in (None, 0)
                ),
                None,
            )
            if failed_terminal is not None:
                status = failed_terminal.poll() or 1
                print(f"[dispatch] {failed_terminal.name} failed: {status}")
                break
            if all(process.poll() == 0 for process in terminal):
                break
            time.sleep(2)
    finally:
        stop_processes(processes)
        copy_role_output("trainer_orchestrator")
        if not remote_failure:
            send_completion(inference_ip, ports["control"], status)
    return status


def main() -> None:
    args = parse_args()
    instance_index, node_ips, ports = topology()
    inference_ip, trainer_ip = node_ips
    control_server = open_control_server(ports["control"])
    try:
        if ensure_prime_rl_environment(
            install_flash_attn=instance_index == 1,
            control_fd=control_server.fileno(),
        ):
            return
        configure_network_environment(node_ips[instance_index])
        run_id = hashlib.sha256(
            (",".join(node_ips) + f":{ports['weight_broadcast']}").encode("utf-8")
        ).hexdigest()[:24]
        role = "inference" if instance_index == 0 else "trainer-orchestrator"
        print(
            f"[dispatch] instance={instance_index}/2 role={role} "
            f"inference={inference_ip}:{ports['inference']} trainer={trainer_ip}",
            flush=True,
        )

        validate_gpu_topology()
        configs = prepare_resolved_configs(
            args,
            trainer_ip=trainer_ip,
            inference_ip=inference_ip,
            ports=ports,
        )
        trainer_config, orchestrator_config, inference_config, env_configs = configs
        if instance_index == 0:
            status = run_inference(
                inference_config,
                run_id,
                control_server,
                trainer_ip,
                ports["control"],
            )
        else:
            status = run_training_roles(
                trainer_config,
                orchestrator_config,
                env_configs,
                trainer_ip,
                inference_ip,
                ports,
                run_id,
                control_server,
            )
    except Exception as error:
        if not isinstance(error, PrimeRLChildProcessError):
            peer_ip = inference_ip if instance_index == 1 else trainer_ip
            try:
                send_completion(peer_ip, ports["control"], 1)
            except Exception as control_error:
                print(
                    f"[control] could not report failure: {control_error}",
                    file=sys.stderr,
                )
        raise
    finally:
        control_server.close()
    raise SystemExit(status)


def handle_sigterm(_signum, _frame) -> None:
    raise RuntimeError("Received SIGTERM.")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
