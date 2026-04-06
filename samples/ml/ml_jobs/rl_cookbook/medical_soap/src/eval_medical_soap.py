#!/usr/bin/env python3
"""
Medical SOAP Evaluation: Compare models on SOAP note generation using a local Qwen3-8B judge.

Runs inside SPCS. Evaluates multiple model backends (Cortex REST, HuggingFace vLLM,
checkpoint vLLM) against the MEDICAL_SOAP_TEST table using section-level LLM-as-judge
scoring (S, O, A, P evaluated independently).

GPU layout (2x GPU):
  GPU 0: vLLM for generating completions (hf/checkpoint models)
  GPU 1: vLLM Qwen3-8B judge server

Environment variables:
  EVAL_MODELS          — comma-separated model specs, e.g.
                         "cortex:claude-sonnet-4-6,hf:Qwen/Qwen3-1.7B,checkpoint:/tmp/eval_model"
  NUM_SAMPLES          — number of test samples to evaluate (default 100)
  CHECKPOINT_STAGE_PATH— Snowflake stage path for checkpoint model weights
  SNOWFLAKE_HOST       — SPCS host for REST API calls
"""
import ast
import asyncio
import atexit
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request

# ============================================================================
# SPCS Runtime Fixes (same as run_medical_soap.py)
# ============================================================================

# Force unbuffered output for SPCS logs
os.environ["PYTHONUNBUFFERED"] = "1"
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Suppress verbose output
os.environ["HF_DATASETS_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"
os.environ["HF_HOME"] = "/tmp/hf_cache"
os.environ["TRANSFORMERS_CACHE"] = "/tmp/hf_cache"
os.environ["HUGGINGFACE_HUB_CACHE"] = "/tmp/hf_cache"

# Ensure venv bin is on PATH so subprocesses (vLLM server) use the right Python.
_venv = os.environ.get("VIRTUAL_ENV", "/opt/venv/snowbook")
_venv_bin = os.path.join(_venv, "bin")
_path = os.environ.get("PATH", "")
if _venv_bin not in _path:
    os.environ["PATH"] = f"{_venv_bin}:{_path}"
    print(f"  PATH: prepended {_venv_bin}")

# Fix LD_LIBRARY_PATH for CUDA libs installed via pip (cuDNN, cuBLAS, etc.)
import site
import ctypes
import glob as _glob
_sp = site.getsitepackages()[0] if site.getsitepackages() else ""
_nvidia_dirs = []
if _sp:
    for _pkg in ["nvidia/cudnn/lib", "nvidia/cublas/lib", "nvidia/cuda_runtime/lib",
                 "nvidia/cuda_nvrtc/lib", "nvidia/cuda_cupti/lib", "nvidia/cufft/lib",
                 "nvidia/curand/lib", "nvidia/cusolver/lib", "nvidia/cusparse/lib",
                 "nvidia/nccl/lib", "nvidia/nvtx/lib", "nvidia/nvjitlink/lib"]:
        _d = os.path.join(_sp, _pkg)
        if os.path.isdir(_d):
            _nvidia_dirs.append(_d)
if _nvidia_dirs:
    _existing = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = ":".join(_nvidia_dirs) + (":" + _existing if _existing else "")
    for _d in _nvidia_dirs:
        for _so in sorted(_glob.glob(os.path.join(_d, "*.so*"))):
            try:
                ctypes.CDLL(_so, mode=ctypes.RTLD_GLOBAL)
            except OSError:
                pass
    print(f"  CUDA libs: preloaded from {len(_nvidia_dirs)} nvidia pip dirs")

# Fix stale torch files from 2.8.0->2.9.1 upgrade in the image.
import site as _site
for _sp in _site.getsitepackages() + [_site.getusersitepackages()]:
    _stale = os.path.join(_sp, "torch", "_inductor", "kernel", "flex_attention.py")
    if os.path.isfile(_stale):
        os.remove(_stale)
        print(f"  Removed stale {_stale}")
    _stale_c = _stale + "c"
    if os.path.isfile(_stale_c):
        os.remove(_stale_c)

# Install AReaL and vLLM with --no-deps from local wheel to avoid torch reinstall.
print("--- Installing AReaL + vLLM (--no-deps) ---")
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "--no-deps", "--quiet",
    "areal @ git+https://github.com/inclusionAI/AReaL.git@v1.0.1",
    "/mnt/job_stage/app/vllm-0.14.0-cp38-abi3-manylinux_2_31_x86_64.whl",
])
print("  AReaL + vLLM installed")

# ============================================================================
# Configuration
# ============================================================================
import ssl
import gzip
import urllib.error

# Detect GPU count and assign layout
_num_gpus = int(os.environ.get("NVIDIA_VISIBLE_DEVICES", "0,1").count(",")) + 1
try:
    result = subprocess.run(
        ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode == 0:
        _num_gpus = len(result.stdout.strip().split("\n"))
except Exception:
    pass

JUDGE_GPU = _num_gpus - 1  # Last GPU for judge
GEN_GPU = 0                # First GPU for generation
JUDGE_PORT = 38899
JUDGE_MODEL = os.environ.get("LOCAL_JUDGE_MODEL", "Qwen/Qwen3-8B")

DATA_DATABASE = "RL_TRAINING_DB"
DATA_SCHEMA = "RL_SCHEMA"
TEST_TABLE = "MEDICAL_SOAP_TEST"

print("=" * 60)
print("Medical SOAP Evaluation (SPCS Runtime)")
print("=" * 60)
print(f"  GPUs detected:  {_num_gpus}")
print(f"  Judge GPU:      {JUDGE_GPU} (port {JUDGE_PORT})")
print(f"  Generation GPU: {GEN_GPU}")
print(f"  Judge model:    {JUDGE_MODEL}")


# ============================================================================
# SPCS Token / REST API Helpers
# ============================================================================
def _get_spcs_token():
    """Read SPCS OAuth token for REST API calls."""
    with open("/snowflake/session/token") as f:
        return f.read().strip()


def _query_table(table_name, host):
    """Query a Snowflake table via REST API and return all rows."""
    fq_table = f"{DATA_DATABASE}.{DATA_SCHEMA}.{table_name}"
    url = f"https://{host}/api/v2/statements"
    payload = {
        "statement": f"SELECT DIALOGUE, PRED_S, PRED_O, PRED_A, PRED_P FROM {fq_table}",
        "timeout": 120,
        "resultSetMetaData": {"format": "jsonv2"},
        "warehouse": os.environ.get("CORTEX_WAREHOUSE", "ADMIN_WH"),
        "database": DATA_DATABASE,
        "schema": DATA_SCHEMA,
    }
    body = json.dumps(payload).encode("utf-8")
    ctx = ssl.create_default_context()

    def _read_response(resp):
        raw = resp.read()
        if raw[:2] == b'\x1f\x8b':
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))

    token = _get_spcs_token()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {token}")

    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=120)
        result = _read_response(resp)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        print(f"  ERROR querying {fq_table}: HTTP {e.code} — {error_body[:500]}")
        raise

    columns = [col["name"] for col in result["resultSetMetaData"]["rowType"]]
    statement_handle = result.get("statementHandle", "")
    all_row_data = result.get("data", [])

    partition_info = result.get("resultSetMetaData", {}).get("partitionInfo", [])
    if len(partition_info) > 1:
        print(f"  {fq_table}: {len(partition_info)} partitions, fetching all...")
        for i in range(1, len(partition_info)):
            part_url = f"https://{host}/api/v2/statements/{statement_handle}?partition={i}"
            token = _get_spcs_token()
            part_req = urllib.request.Request(part_url, method="GET")
            part_req.add_header("Accept", "application/json")
            part_req.add_header("Authorization", f"Bearer {token}")
            try:
                part_resp = urllib.request.urlopen(part_req, context=ctx, timeout=120)
                part_result = _read_response(part_resp)
                all_row_data.extend(part_result.get("data", []))
            except urllib.error.HTTPError as e:
                error_body = e.read().decode("utf-8", errors="replace")
                print(f"  ERROR fetching partition {i}: HTTP {e.code} — {error_body[:200]}")
                raise

    rows = [dict(zip(columns, row_data)) for row_data in all_row_data]
    print(f"  Queried {fq_table}: {len(rows)} rows ({len(partition_info)} partition(s))")
    return rows


def _execute_sql(sql_text, host):
    """Execute arbitrary SQL via REST API and return result."""
    url = f"https://{host}/api/v2/statements"
    payload = {
        "statement": sql_text,
        "timeout": 120,
        "resultSetMetaData": {"format": "jsonv2"},
        "warehouse": os.environ.get("CORTEX_WAREHOUSE", "ADMIN_WH"),
        "database": DATA_DATABASE,
        "schema": DATA_SCHEMA,
    }
    body = json.dumps(payload).encode("utf-8")
    ctx = ssl.create_default_context()

    token = _get_spcs_token()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {token}")

    resp = urllib.request.urlopen(req, context=ctx, timeout=120)
    raw = resp.read()
    if raw[:2] == b'\x1f\x8b':
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


# ============================================================================
# Checkpoint Download
# ============================================================================
def download_checkpoint(stage_path, host):
    """Download checkpoint from Snowflake stage via presigned URLs."""
    local_model = "/tmp/eval_model"
    if os.path.exists(local_model):
        shutil.rmtree(local_model)
    os.makedirs(local_model, exist_ok=True)

    list_result = _execute_sql(f"LIST {stage_path}", host)
    files = [row[0] for row in list_result.get("data", [])]
    print(f"  Found {len(files)} files on stage")

    stage_parts = stage_path.split("/")
    stage_name = stage_parts[0]
    stage_root = stage_name.lstrip("@").split(".")[-1].lower()

    for stage_file in files:
        if stage_file.lower().startswith(stage_root):
            rel_path = stage_file[len(stage_root) + 1:]
        else:
            rel_path = stage_file

        filename = os.path.basename(rel_path)
        if not filename:
            continue

        local_file = os.path.join(local_model, filename)
        url_result = _execute_sql(
            f"SELECT GET_PRESIGNED_URL({stage_name}, '{rel_path}')", host,
        )
        presigned_url = url_result["data"][0][0]
        urllib.request.urlretrieve(presigned_url, local_file)
        size_mb = os.path.getsize(local_file) / 1e6
        print(f"    {filename}: {size_mb:.1f} MB")

    print(f"  Contents: {os.listdir(local_model)}")
    return local_model


# ============================================================================
# Judge Server
# ============================================================================
_judge_process = None


def start_judge_server():
    """Spawn vLLM OpenAI-compatible server for judging on the last GPU."""
    global _judge_process

    print(f"\n--- Starting vLLM Judge Server (GPU {JUDGE_GPU}, port {JUDGE_PORT}) ---")
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = str(JUDGE_GPU)

    cmd = [
        sys.executable, "-m", "vllm.entrypoints.openai.api_server",
        "--model", JUDGE_MODEL,
        "--port", str(JUDGE_PORT),
        "--dtype", "bfloat16",
        "--max-model-len", "8192",
        "--gpu-memory-utilization", "0.90",
        "--trust-remote-code",
        "--disable-log-requests",
        "--enable-prefix-caching",
    ]

    _judge_process = subprocess.Popen(cmd, env=env, start_new_session=True)
    atexit.register(_kill_judge)

    # Wait for judge to become healthy (up to 10 minutes)
    max_wait = 600
    start_time = time.time()
    last_log = 0

    while time.time() - start_time < max_wait:
        if _judge_process.poll() is not None:
            raise RuntimeError(
                f"vLLM judge exited with code {_judge_process.returncode}"
            )
        try:
            req = urllib.request.Request(
                f"http://localhost:{JUDGE_PORT}/health", method="GET",
            )
            resp = urllib.request.urlopen(req, timeout=5)
            if resp.status == 200:
                print(f"  Judge ready in {time.time() - start_time:.1f}s")
                return
        except Exception:
            pass

        elapsed = time.time() - start_time
        if elapsed - last_log >= 30:
            print(f"  Waiting for judge... ({elapsed:.0f}s)")
            last_log = elapsed
        time.sleep(5)

    _kill_judge()
    raise RuntimeError(f"Judge not healthy within {max_wait}s")


def _kill_judge():
    """Kill the judge server process."""
    global _judge_process
    if _judge_process and _judge_process.poll() is None:
        print(f"  Killing judge (PID={_judge_process.pid})...")
        try:
            os.killpg(os.getpgid(_judge_process.pid), signal.SIGTERM)
        except (ProcessLookupError, OSError):
            try:
                _judge_process.kill()
            except (ProcessLookupError, OSError):
                pass
    _judge_process = None


# ============================================================================
# Generation vLLM Server (for hf/checkpoint models)
# ============================================================================
_gen_process = None
_gen_port = 38900
_gen_model_name = None


def start_gen_server(model_path, tp=1, gpu_ids=None):
    """Start a vLLM server for generating completions.
    
    Args:
        model_path: HuggingFace model name or local path
        tp: tensor parallel size (e.g. 8 for 235B model)
        gpu_ids: comma-separated GPU IDs (e.g. "0,1,2,3,4,5,6,7"), defaults to GEN_GPU
    """
    global _gen_process, _gen_model_name

    # Kill any existing gen server
    stop_gen_server()

    if gpu_ids is None:
        gpu_ids = str(GEN_GPU)

    print(f"\n--- Starting vLLM Generation Server (GPUs {gpu_ids}, tp={tp}, port {_gen_port}) ---")
    print(f"  Model: {model_path}")
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = gpu_ids

    cmd = [
        sys.executable, "-m", "vllm.entrypoints.openai.api_server",
        "--model", model_path,
        "--port", str(_gen_port),
        "--dtype", "bfloat16",
        "--max-model-len", "4096",
        "--gpu-memory-utilization", "0.90",
        "--trust-remote-code",
        "--disable-log-requests",
    ]
    if tp > 1:
        cmd.extend(["--tensor-parallel-size", str(tp)])

    _gen_process = subprocess.Popen(cmd, env=env, start_new_session=True)
    _gen_model_name = model_path

    # Wait for server to become healthy (longer timeout for large TP models)
    max_wait = 1800 if tp > 1 else 600
    start_time = time.time()
    last_log = 0

    while time.time() - start_time < max_wait:
        if _gen_process.poll() is not None:
            raise RuntimeError(
                f"vLLM gen server exited with code {_gen_process.returncode}"
            )
        try:
            req = urllib.request.Request(
                f"http://localhost:{_gen_port}/health", method="GET",
            )
            resp = urllib.request.urlopen(req, timeout=5)
            if resp.status == 200:
                print(f"  Gen server ready in {time.time() - start_time:.1f}s")
                return
        except Exception:
            pass

        elapsed = time.time() - start_time
        if elapsed - last_log >= 30:
            print(f"  Waiting for gen server... ({elapsed:.0f}s)")
            last_log = elapsed
        time.sleep(5)

    stop_gen_server()
    raise RuntimeError(f"Gen server not healthy within {max_wait}s")


def stop_gen_server():
    """Stop the generation vLLM server."""
    global _gen_process, _gen_model_name
    if _gen_process and _gen_process.poll() is None:
        print(f"  Killing gen server (PID={_gen_process.pid})...")
        try:
            os.killpg(os.getpgid(_gen_process.pid), signal.SIGTERM)
        except (ProcessLookupError, OSError):
            try:
                _gen_process.kill()
            except (ProcessLookupError, OSError):
                pass
        _gen_process.wait(timeout=30)
    _gen_process = None
    _gen_model_name = None


# ============================================================================
# Model Completion Backends
# ============================================================================
def _generate_cortex(model_name, system_prompt, user_prompt, host):
    """Generate completion via Snowflake Cortex COMPLETE SQL API."""
    messages = json.dumps([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    options = json.dumps({"temperature": 0, "max_tokens": 2048})

    # Use bindings to avoid SQL injection / escaping issues with medical text
    url = f"https://{host}/api/v2/statements"
    payload = {
        "statement": (
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, PARSE_JSON(?), PARSE_JSON(?)) AS response"
        ),
        "bindings": {
            "1": {"type": "TEXT", "value": model_name},
            "2": {"type": "TEXT", "value": messages},
            "3": {"type": "TEXT", "value": options},
        },
        "timeout": 180,
        "resultSetMetaData": {"format": "jsonv2"},
        "warehouse": os.environ.get("CORTEX_WAREHOUSE", "ADMIN_WH"),
        "database": DATA_DATABASE,
        "schema": DATA_SCHEMA,
    }
    body = json.dumps(payload).encode("utf-8")

    import ssl
    ctx = ssl.create_default_context()
    token = _get_spcs_token()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {token}")

    resp = urllib.request.urlopen(req, context=ctx, timeout=180)
    raw = resp.read()
    if raw[:2] == b'\x1f\x8b':
        import gzip
        raw = gzip.decompress(raw)
    result = json.loads(raw.decode("utf-8"))

    raw_text = result["data"][0][0]
    try:
        parsed = json.loads(raw_text)
        return parsed["choices"][0]["messages"]
    except (json.JSONDecodeError, KeyError, IndexError):
        return raw_text


def _generate_vllm(model_name, system_prompt, user_prompt):
    """Generate completion via local vLLM server on GPU 0."""
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 2048,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"http://localhost:{_gen_port}/v1/chat/completions",
        data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req, timeout=180)
    result = json.loads(resp.read().decode("utf-8"))
    return result["choices"][0]["message"]["content"]


# ============================================================================
# Judge Helpers
# ============================================================================
def _strip_think_tags(text):
    """Strip <think>...</think> tags from model response."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL)
    return text.strip()


def parse_judge_result(raw_response):
    """Parse judge response and return score (1.0 for pass, 0.0 for fail)."""
    response = _strip_think_tags(raw_response)
    result = None
    response_lower = response.lower()

    # Try to extract JSON from code fence
    for parse_fn in [json.loads, ast.literal_eval]:
        try:
            result = parse_fn(
                response.split("```json")[-1].split("```")[0].strip()
            )
            break
        except Exception:
            pass

    # Try to find raw JSON object with verdict key
    if result is None:
        json_match = re.search(r'\{[^{}]*"verdict"[^{}]*\}', response, re.IGNORECASE)
        if json_match:
            try:
                result = json.loads(json_match.group())
            except Exception:
                pass

    # Fallback: look for verdict string
    if result is None and '"verdict": "fail"' in response_lower:
        result = {"verdict": "fail"}
    if result is None and '"verdict": "pass"' in response_lower:
        result = {"verdict": "pass"}
    if result is None and 'verdict": "fail"' in response_lower:
        result = {"verdict": "fail"}
    if result is None and 'verdict": "pass"' in response_lower:
        result = {"verdict": "pass"}

    if result is None:
        return 0.0

    verdict = result.get("verdict", "").lower()
    return float(verdict == "pass")


async def _call_judge_async(session, system_prompt, user_prompt):
    """Async call to local vLLM judge server via aiohttp."""
    payload = {
        "model": JUDGE_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 2048,
    }

    try:
        async with session.post(
            f"http://localhost:{JUDGE_PORT}/v1/chat/completions",
            json=payload,
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Judge HTTP {resp.status}")
            result = await resp.json()
        return result["choices"][0]["message"]["content"]
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Judge error: {type(e).__name__}: {e}") from None


# ============================================================================
# Evaluation Logic
# ============================================================================
async def evaluate_single_sample(session, sample, completion, judge_sys_prompt,
                                 create_section_judge_prompt):
    """Judge all 4 SOAP sections of a single sample in parallel.

    Returns dict with keys: format_ok, S, O, A, P, total.
    """
    from medical_soap.reward import extract_json_from_response, validate_soap_json

    clean = _strip_think_tags(completion)
    parsed = extract_json_from_response(clean)

    if parsed is None or not validate_soap_json(parsed):
        return {"format_ok": False, "S": 0.0, "O": 0.0, "A": 0.0, "P": 0.0, "total": 0.0}

    dialogue = sample["DIALOGUE"]
    gt = {
        "S": sample.get("PRED_S", ""),
        "O": sample.get("PRED_O", ""),
        "A": sample.get("PRED_A", ""),
        "P": sample.get("PRED_P", ""),
    }

    async def _judge_section(key):
        user_prompt = create_section_judge_prompt(dialogue, key, gt[key], parsed[key])
        try:
            raw = await _call_judge_async(session, judge_sys_prompt, user_prompt)
            return parse_judge_result(raw)
        except Exception:
            return 0.0

    scores = await asyncio.gather(
        _judge_section("S"),
        _judge_section("O"),
        _judge_section("A"),
        _judge_section("P"),
    )

    return {
        "format_ok": True,
        "S": scores[0],
        "O": scores[1],
        "A": scores[2],
        "P": scores[3],
        "total": 1.0 + sum(scores),  # 1.0 format + section scores
    }


async def evaluate_model(model_spec, samples, host):
    """Evaluate a single model on all samples.

    Args:
        model_spec: Model specification string (e.g. "cortex:claude-sonnet-4-6")
        samples: List of test sample dicts
        host: Snowflake host for REST API

    Returns:
        dict with per-section averages and overall scores.
    """
    import aiohttp

    # Copy reward module to importable location
    REWARD_MODULE_DIR = "/tmp/reward_modules"
    stage_src = "/mnt/job_stage/app"
    local_dst = os.path.join(REWARD_MODULE_DIR, "medical_soap")
    if os.path.exists(stage_src) and not os.path.exists(local_dst):
        os.makedirs(REWARD_MODULE_DIR, exist_ok=True)
        shutil.copytree(stage_src, local_dst)
    if REWARD_MODULE_DIR not in sys.path:
        sys.path.insert(0, REWARD_MODULE_DIR)

    from medical_soap.prompt_utils import (
        SYSTEM_PROMPT,
        USER_PROMPT_PREFIX,
        JUDGE_SECTION_SYSTEM_PROMPT,
        create_section_judge_prompt,
    )

    # Parse model spec — supports "backend:model" or "backend:model:tp=N"
    parts = model_spec.split(":", 1)
    backend = parts[0]
    model_name = parts[1] if len(parts) > 1 else parts[0]
    tp = 1
    if ":tp=" in model_spec:
        # e.g. "hf:Qwen/Qwen3-235B-A22B:tp=8"
        segments = model_spec.split(":")
        backend = segments[0]
        tp_parts = [s for s in segments if s.startswith("tp=")]
        if tp_parts:
            tp = int(tp_parts[0].split("=")[1])
        model_name = ":".join(s for s in segments[1:] if not s.startswith("tp="))

    print(f"\n--- Evaluating: {model_spec} (tp={tp}) ---")

    # Determine GPU IDs for this model
    if tp > 1:
        gpu_ids = ",".join(str(i) for i in range(_num_gpus))
    else:
        gpu_ids = None  # defaults to GEN_GPU

    # Start generation server if needed
    if backend in ("hf", "checkpoint"):
        model_path = model_name
        start_gen_server(model_path, tp=tp, gpu_ids=gpu_ids)

    # Generate completions
    completions = []
    gen_errors = 0
    for i, sample in enumerate(samples):
        user_prompt = f"{USER_PROMPT_PREFIX}\n\n{sample['DIALOGUE']}"
        try:
            if backend == "cortex":
                completion = _generate_cortex(model_name, SYSTEM_PROMPT, user_prompt, host)
            elif backend in ("hf", "checkpoint"):
                completion = _generate_vllm(_gen_model_name, SYSTEM_PROMPT, user_prompt)
            else:
                raise ValueError(f"Unknown backend: {backend}")
            completions.append(completion)
        except Exception as e:
            print(f"  [gen error] Sample {i}: {type(e).__name__}: {e}")
            completions.append("")
            gen_errors += 1

        if (i + 1) % 50 == 0:
            print(f"  Generated {i + 1}/{len(samples)} completions ({gen_errors} errors)")

    print(f"  Generation complete: {len(completions)} completions, {gen_errors} errors")

    # Stop gen server after generating all completions
    if backend in ("hf", "checkpoint"):
        stop_gen_server()

    # Judge completions in parallel
    timeout = aiohttp.ClientTimeout(total=120, sock_connect=5, sock_read=120)
    async with aiohttp.ClientSession(timeout=timeout) as aio_session:
        judge_errors = 0
        results = []

        # Process in batches to avoid overwhelming the judge
        batch_size = 20
        for batch_start in range(0, len(samples), batch_size):
            batch_end = min(batch_start + batch_size, len(samples))
            batch_tasks = []
            for j in range(batch_start, batch_end):
                if completions[j]:
                    batch_tasks.append(
                        evaluate_single_sample(
                            aio_session, samples[j], completions[j],
                            JUDGE_SECTION_SYSTEM_PROMPT, create_section_judge_prompt,
                        )
                    )
                else:
                    # Empty completion — score 0
                    async def _zero():
                        return {
                            "format_ok": False, "S": 0.0, "O": 0.0,
                            "A": 0.0, "P": 0.0, "total": 0.0,
                        }
                    batch_tasks.append(_zero())

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, Exception):
                    judge_errors += 1
                    results.append({
                        "format_ok": False, "S": 0.0, "O": 0.0,
                        "A": 0.0, "P": 0.0, "total": 0.0,
                    })
                else:
                    results.append(r)

            if (batch_end) % 50 == 0 or batch_end == len(samples):
                print(f"  Judged {batch_end}/{len(samples)} samples ({judge_errors} errors)")

    # Compute aggregates
    n = len(results)
    format_ok_count = sum(1 for r in results if r["format_ok"])
    avg_S = sum(r["S"] for r in results) / n if n else 0
    avg_O = sum(r["O"] for r in results) / n if n else 0
    avg_A = sum(r["A"] for r in results) / n if n else 0
    avg_P = sum(r["P"] for r in results) / n if n else 0
    avg_total = sum(r["total"] for r in results) / n if n else 0

    return {
        "model": model_spec,
        "n": n,
        "format_ok": format_ok_count,
        "format_pct": format_ok_count / n * 100 if n else 0,
        "S": avg_S,
        "O": avg_O,
        "A": avg_A,
        "P": avg_P,
        "avg_total": avg_total,
        "gen_errors": gen_errors,
        "judge_errors": judge_errors,
    }


# ============================================================================
# Comparison Table
# ============================================================================
def print_comparison_table(results_list):
    """Print a formatted comparison table of model evaluation results."""
    print("\n" + "=" * 90)
    print("EVALUATION RESULTS")
    print("=" * 90)

    header = (
        f"{'Model':<40} {'Format%':>7} {'S':>6} {'O':>6} {'A':>6} {'P':>6} "
        f"{'Avg':>6} {'N':>5}"
    )
    print(header)
    print("-" * 90)

    for r in results_list:
        model_name = r["model"]
        if len(model_name) > 38:
            model_name = model_name[:35] + "..."
        print(
            f"{model_name:<40} {r['format_pct']:>6.1f}% "
            f"{r['S']:>6.3f} {r['O']:>6.3f} {r['A']:>6.3f} {r['P']:>6.3f} "
            f"{r['avg_total']:>6.2f} {r['n']:>5}"
        )

    print("-" * 90)
    print(f"  Score range: 0-5 (1.0 format + 1.0 each for S, O, A, P)")
    print(f"  Section scores: fraction of samples where judge passed (0.0-1.0)")
    print("=" * 90)


# ============================================================================
# Main
# ============================================================================
def main():
    host = os.environ.get("SNOWFLAKE_HOST", "")
    if not host:
        raise RuntimeError("SNOWFLAKE_HOST not set — required for REST API calls")

    eval_models_str = os.environ.get("EVAL_MODELS", "")
    if not eval_models_str:
        raise RuntimeError("EVAL_MODELS not set — provide comma-separated model specs")

    num_samples = int(os.environ.get("NUM_SAMPLES", "100"))
    checkpoint_stage_path = os.environ.get("CHECKPOINT_STAGE_PATH", "")
    model_specs = [m.strip() for m in eval_models_str.split(",") if m.strip()]

    print(f"\n  Models to evaluate: {model_specs}")
    print(f"  Num samples: {num_samples}")
    if checkpoint_stage_path:
        print(f"  Checkpoint stage: {checkpoint_stage_path}")

    # 1. Check if any model needs all GPUs (tp = num_gpus).
    #    If so, defer judge startup until after generation to avoid GPU conflict.
    needs_all_gpus = any(
        f":tp={_num_gpus}" in spec or f":tp= {_num_gpus}" in spec
        for spec in model_specs
    )
    if needs_all_gpus:
        print(f"  Large model detected (tp={_num_gpus}): deferring judge until after generation")
    else:
        start_judge_server()

    # 2. Copy reward module to importable location
    REWARD_MODULE_DIR = "/tmp/reward_modules"
    stage_src = "/mnt/job_stage/app"
    local_dst = os.path.join(REWARD_MODULE_DIR, "medical_soap")
    if os.path.exists(stage_src):
        os.makedirs(REWARD_MODULE_DIR, exist_ok=True)
        if os.path.exists(local_dst):
            shutil.rmtree(local_dst)
        shutil.copytree(stage_src, local_dst)
        print(f"  Copied reward module to {local_dst}")
    if REWARD_MODULE_DIR not in sys.path:
        sys.path.insert(0, REWARD_MODULE_DIR)

    # 3. Load test data
    print("\n--- Loading Test Data ---")
    all_samples = _query_table(TEST_TABLE, host)
    if num_samples < len(all_samples):
        all_samples = all_samples[:num_samples]
    print(f"  Using {len(all_samples)} samples for evaluation")

    # 4. Download checkpoint if any model needs it
    for spec in model_specs:
        if spec.startswith("checkpoint:"):
            if not checkpoint_stage_path:
                raise RuntimeError(
                    f"Model {spec} requires CHECKPOINT_STAGE_PATH but it is not set"
                )
            print("\n--- Downloading Checkpoint ---")
            local_path = download_checkpoint(checkpoint_stage_path, host)
            # Rewrite checkpoint model specs to use local path
            model_specs = [
                s.replace(s.split(":", 1)[1], local_path) if s.startswith("checkpoint:") else s
                for s in model_specs
            ]
            break

    # 5. Evaluate each model
    all_results = []
    if needs_all_gpus:
        # Two-phase: generate all completions first, then judge after starting judge server.
        # This allows large TP models to use all GPUs during generation.
        all_completions = {}  # model_spec -> (completions, gen_errors, backend, model_name)
        for spec in model_specs:
            try:
                parts = spec.split(":", 1)
                backend = parts[0]
                model_name = parts[1] if len(parts) > 1 else parts[0]
                tp = 1
                if ":tp=" in spec:
                    segments = spec.split(":")
                    backend = segments[0]
                    tp_parts = [s for s in segments if s.startswith("tp=")]
                    if tp_parts:
                        tp = int(tp_parts[0].split("=")[1])
                    model_name = ":".join(s for s in segments[1:] if not s.startswith("tp="))

                print(f"\n--- Generating: {spec} (tp={tp}) ---")

                if tp > 1:
                    gpu_ids = ",".join(str(i) for i in range(_num_gpus))
                else:
                    gpu_ids = None

                from medical_soap.prompt_utils import SYSTEM_PROMPT, USER_PROMPT_PREFIX

                completions = []
                gen_errors = 0
                if backend in ("hf", "checkpoint"):
                    start_gen_server(model_name, tp=tp, gpu_ids=gpu_ids)
                for i, sample in enumerate(all_samples):
                    user_prompt = f"{USER_PROMPT_PREFIX}\n\n{sample['DIALOGUE']}"
                    try:
                        if backend == "cortex":
                            completion = _generate_cortex(model_name, SYSTEM_PROMPT, user_prompt, host)
                        elif backend in ("hf", "checkpoint"):
                            completion = _generate_vllm(_gen_model_name, SYSTEM_PROMPT, user_prompt)
                        else:
                            raise ValueError(f"Unknown backend: {backend}")
                        completions.append(completion)
                    except Exception as e:
                        print(f"  [gen error] Sample {i}: {type(e).__name__}: {e}")
                        completions.append("")
                        gen_errors += 1
                    if (i + 1) % 50 == 0:
                        print(f"  Generated {i + 1}/{len(all_samples)} completions ({gen_errors} errors)")
                print(f"  Generation complete: {len(completions)} completions, {gen_errors} errors")
                if backend in ("hf", "checkpoint"):
                    stop_gen_server()
                all_completions[spec] = (completions, gen_errors)
            except Exception as e:
                import traceback
                print(f"\n  ERROR generating for {spec}:")
                print(traceback.format_exc())
                all_completions[spec] = ([""] * len(all_samples), len(all_samples))

        # Now start judge and score everything
        print("\n--- Starting Judge for Scoring Phase ---")
        start_judge_server()

        import aiohttp
        from medical_soap.prompt_utils import (
            JUDGE_SECTION_SYSTEM_PROMPT,
            create_section_judge_prompt,
        )

        for spec in model_specs:
            completions, gen_errors = all_completions[spec]
            print(f"\n--- Judging: {spec} ---")
            try:
                async def _judge_completions():
                    timeout = aiohttp.ClientTimeout(total=120, sock_connect=5, sock_read=120)
                    async with aiohttp.ClientSession(timeout=timeout) as aio_session:
                        judge_errors = 0
                        results = []
                        batch_size = 20
                        for batch_start in range(0, len(all_samples), batch_size):
                            batch_end = min(batch_start + batch_size, len(all_samples))
                            batch_tasks = []
                            for j in range(batch_start, batch_end):
                                if completions[j]:
                                    batch_tasks.append(
                                        evaluate_single_sample(
                                            aio_session, all_samples[j], completions[j],
                                            JUDGE_SECTION_SYSTEM_PROMPT, create_section_judge_prompt,
                                        )
                                    )
                                else:
                                    async def _zero():
                                        return {"format_ok": False, "S": 0.0, "O": 0.0, "A": 0.0, "P": 0.0, "total": 0.0}
                                    batch_tasks.append(_zero())
                            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                            for r in batch_results:
                                if isinstance(r, Exception):
                                    judge_errors += 1
                                    results.append({"format_ok": False, "S": 0.0, "O": 0.0, "A": 0.0, "P": 0.0, "total": 0.0})
                                else:
                                    results.append(r)
                            if batch_end % 50 == 0 or batch_end == len(all_samples):
                                print(f"  Judged {batch_end}/{len(all_samples)} samples ({judge_errors} errors)")
                        return results, judge_errors
                results, judge_errors = asyncio.run(_judge_completions())
                n = len(results)
                all_results.append({
                    "model": spec,
                    "n": n,
                    "format_ok": sum(1 for r in results if r["format_ok"]),
                    "format_pct": sum(1 for r in results if r["format_ok"]) / n * 100 if n else 0,
                    "S": sum(r["S"] for r in results) / n if n else 0,
                    "O": sum(r["O"] for r in results) / n if n else 0,
                    "A": sum(r["A"] for r in results) / n if n else 0,
                    "P": sum(r["P"] for r in results) / n if n else 0,
                    "avg_total": sum(r["total"] for r in results) / n if n else 0,
                    "gen_errors": gen_errors,
                    "judge_errors": judge_errors,
                })
            except Exception as e:
                import traceback
                print(f"\n  ERROR judging {spec}:")
                print(traceback.format_exc())
                all_results.append({
                    "model": spec, "n": len(all_samples), "format_ok": 0,
                    "format_pct": 0, "S": 0, "O": 0, "A": 0, "P": 0,
                    "avg_total": 0, "gen_errors": gen_errors, "judge_errors": len(all_samples),
                })
    else:
        # Original flow: judge is already running, evaluate each model end-to-end
        for spec in model_specs:
            try:
                result = asyncio.run(evaluate_model(spec, all_samples, host))
                all_results.append(result)
            except Exception as e:
                import traceback
                print(f"\n  ERROR evaluating {spec}:")
                print(traceback.format_exc())
                all_results.append({
                    "model": spec, "n": len(all_samples), "format_ok": 0,
                    "format_pct": 0, "S": 0, "O": 0, "A": 0, "P": 0,
                    "avg_total": 0, "gen_errors": len(all_samples), "judge_errors": 0,
                })

    # 6. Print comparison table
    print_comparison_table(all_results)

    # 7. Print detailed per-model stats
    print("\nDetailed Stats:")
    for r in all_results:
        print(f"  {r['model']}:")
        print(f"    Samples: {r['n']}, Format OK: {r['format_ok']} ({r['format_pct']:.1f}%)")
        print(f"    Section pass rates: S={r['S']:.3f} O={r['O']:.3f} A={r['A']:.3f} P={r['P']:.3f}")
        print(f"    Avg total score: {r['avg_total']:.3f}/5.0")
        print(f"    Gen errors: {r['gen_errors']}, Judge errors: {r['judge_errors']}")

    print("\n" + "=" * 60)
    print("Evaluation complete.")
    print("=" * 60)

    # 8. Ensure all output is flushed and captured by SPCS event table
    sys.stdout.flush()
    sys.stderr.flush()
    # Give SPCS log collector time to capture the final output
    time.sleep(10)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("=" * 60)
        print("FATAL ERROR:")
        print(traceback.format_exc())
        print("=" * 60)
        sys.stdout.flush()
        sys.stderr.flush()
        _kill_judge()
        stop_gen_server()
        time.sleep(10)
        sys.exit(1)
    finally:
        # Final cleanup — ensure servers are stopped
        _kill_judge()
        stop_gen_server()
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(5)
