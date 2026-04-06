#!/usr/bin/env python3
"""
Medical SOAP RL Training with Local vLLM Judge on SPCS Runtime Image.

Trains a model to generate SOAP notes from doctor-patient dialogues using
section-level LLM-as-judge reward (S, O, A, P evaluated independently).

This version uses the SPCS managed runtime image (no custom Docker build).
Ray is pre-started by the runtime; packages installed via pip_requirements.

GPU layout (8x A100-40GB):
  GPUs 0-3: AReaL training
  GPUs 4-7: vLLM judge servers (one per GPU)

Reward structure (max 5.0):
  - Format reward: 1.0 if valid JSON with S, O, A, P keys, else 0.0
  - Section rewards: 1.0 each for S, O, A, P (max 4.0) via LLM judge
"""
import ast
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
# The vLLM rollout server is launched via subprocess and needs access to pip-installed
# packages in the venv. Also ensure VIRTUAL_ENV is set for subprocess activation.
_venv = os.environ.get("VIRTUAL_ENV", "/opt/venv/snowbook")
_venv_bin = os.path.join(_venv, "bin")
_path = os.environ.get("PATH", "")
if _venv_bin not in _path:
    os.environ["PATH"] = f"{_venv_bin}:{_path}"
    print(f"  PATH: prepended {_venv_bin}")

# Fix LD_LIBRARY_PATH for CUDA libs installed via pip (cuDNN, cuBLAS, etc.)
# The SPCS runtime installs nvidia-cudnn-cu12 etc. as pip packages which place
# .so files in site-packages, but torch looks for them on LD_LIBRARY_PATH.
# We set LD_LIBRARY_PATH AND preload libcudnn via ctypes (since LD_LIBRARY_PATH
# changes after process start only affect dlopen with full path, not by name).
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
    # Preload critical .so files so dlopen can find them by name
    for _d in _nvidia_dirs:
        for _so in sorted(_glob.glob(os.path.join(_d, "*.so*"))):
            try:
                ctypes.CDLL(_so, mode=ctypes.RTLD_GLOBAL)
            except OSError:
                pass
    print(f"  CUDA libs: preloaded from {len(_nvidia_dirs)} nvidia pip dirs")

# Fix stale torch files from 2.8.0->2.9.1 upgrade in the image.
# The old flex_attention.py wasn't removed, causing "duplicate template name" error.
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

# GPU layout: GPUs 0-3 for AReaL, GPUs 4-7 for judges
JUDGE_GPUS = [6, 7]
NUM_AREAL_GPUS = 6

JUDGE_BASE_PORT = int(os.environ.get("LOCAL_JUDGE_BASE_PORT", "38899"))
JUDGE_PORTS = [JUDGE_BASE_PORT + i for i in range(len(JUDGE_GPUS))]
os.environ["NUM_GPUS"] = str(NUM_AREAL_GPUS)

JUDGE_MODEL = os.environ.get("LOCAL_JUDGE_MODEL", "Qwen/Qwen3-8B")

print("=" * 60)
print("Medical SOAP RL Training (SPCS Runtime)")
print("=" * 60)
print(f"  Judge model:  {JUDGE_MODEL}")
print(f"  Judge GPUs:   {JUDGE_GPUS} ({len(JUDGE_GPUS)} judges)")
print(f"  Judge ports:  {JUDGE_PORTS}")
print(f"  AReaL GPUs:   0-{NUM_AREAL_GPUS - 1} ({NUM_AREAL_GPUS} total)")


# ============================================================================
# Local vLLM Judge Server
# ============================================================================
_judge_processes = []


def start_judge_servers():
    """Spawn vLLM OpenAI-compatible servers on dedicated GPUs."""
    global _judge_processes

    print(f"\n--- Starting {len(JUDGE_GPUS)} vLLM Judge Servers ---")

    for gpu, port in zip(JUDGE_GPUS, JUDGE_PORTS):
        print(f"  Launching judge on GPU {gpu}, port {port}...")
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = str(gpu)

        cmd = [
            sys.executable, "-m", "vllm.entrypoints.openai.api_server",
            "--model", JUDGE_MODEL,
            "--port", str(port),
            "--dtype", "bfloat16",
            "--max-model-len", "8192",
            "--gpu-memory-utilization", "0.90",
            "--trust-remote-code",
            "--disable-log-requests",
            "--enable-prefix-caching",
        ]

        proc = subprocess.Popen(cmd, env=env)
        _judge_processes.append((gpu, port, proc))

    atexit.register(_kill_judges)

    # Wait for all judges to become healthy (up to 10 minutes)
    max_wait = 600
    start_time = time.time()
    last_log = 0
    healthy = set()

    while time.time() - start_time < max_wait and len(healthy) < len(JUDGE_GPUS):
        for i, (gpu, port, proc) in enumerate(_judge_processes):
            if i in healthy:
                continue
            if proc.poll() is not None:
                raise RuntimeError(
                    f"vLLM judge on GPU {gpu} port {port} exited with code {proc.returncode}"
                )
            try:
                req = urllib.request.Request(f"http://localhost:{port}/health", method="GET")
                resp = urllib.request.urlopen(req, timeout=5)
                if resp.status == 200:
                    healthy.add(i)
                    print(f"  Judge GPU {gpu} port {port} ready ({len(healthy)}/{len(JUDGE_GPUS)})")
            except Exception:
                pass

        elapsed = time.time() - start_time
        if elapsed - last_log >= 30:
            print(f"  Waiting for judges... {len(healthy)}/{len(JUDGE_GPUS)} ready ({elapsed:.0f}s)")
            last_log = elapsed
        if len(healthy) < len(JUDGE_GPUS):
            time.sleep(5)

    if len(healthy) < len(JUDGE_GPUS):
        _kill_judges()
        raise RuntimeError(f"Only {len(healthy)}/{len(JUDGE_GPUS)} judges healthy within {max_wait}s")

    print(f"  All {len(JUDGE_GPUS)} judges ready in {time.time() - start_time:.1f}s")


def _kill_judges():
    """Kill all judge server processes."""
    global _judge_processes
    for gpu, port, proc in _judge_processes:
        if proc.poll() is None:
            print(f"  Killing judge on GPU {gpu} port {port} (PID={proc.pid})...")
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, OSError):
                try:
                    proc.kill()
                except (ProcessLookupError, OSError):
                    pass
    _judge_processes = []


# ============================================================================
# Data Preparation
# ============================================================================
TRAIN_TABLE = "MEDICAL_SOAP_TRAIN"
EVAL_TABLE = "MEDICAL_SOAP_TEST"
DATA_DATABASE = "RL_TRAINING_DB"
DATA_SCHEMA = "RL_SCHEMA"


def _get_spcs_token():
    """Read SPCS OAuth token for REST API calls."""
    with open("/snowflake/session/token") as f:
        return f.read().strip()


def _query_table(table_name, host):
    """Query a Snowflake table via REST API and return all rows."""
    import ssl
    import gzip
    import urllib.error

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


# ============================================================================
# Dataset Loading
# ============================================================================
def load_medical_soap_dataset(dataset_config, tokenizer):
    """Load medical SOAP dataset from a Snowflake table.

    Returns HF Dataset with columns:
      question, answer, messages, dialogue, pred_S, pred_O, pred_A, pred_P
    """
    from datasets import Dataset
    from medical_soap.prompt_utils import SYSTEM_PROMPT, USER_PROMPT_PREFIX

    table_name = dataset_config.path
    host = os.environ.get("SNOWFLAKE_HOST", "")
    if not host:
        raise RuntimeError("SNOWFLAKE_HOST not set — required for table queries")

    rows = _query_table(table_name, host)

    records = []
    for row in rows:
        dialogue = row["DIALOGUE"]
        pred_s = row.get("PRED_S", "")
        pred_o = row.get("PRED_O", "")
        pred_a = row.get("PRED_A", "")
        pred_p = row.get("PRED_P", "")

        user_prompt = f"{USER_PROMPT_PREFIX}\n\n{dialogue}"

        records.append({
            "question": user_prompt,
            "answer": json.dumps({"S": pred_s, "O": pred_o, "A": pred_a, "P": pred_p}),
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "dialogue": dialogue,
            "pred_S": pred_s,
            "pred_O": pred_o,
            "pred_A": pred_a,
            "pred_P": pred_p,
        })

    dataset = Dataset.from_list(records)
    print(f"  Loaded {len(records)} records from {table_name}")
    return dataset


# ============================================================================
# Judge Helpers
# ============================================================================
_JUDGE_URLS = [f"http://localhost:{p}/v1/chat/completions" for p in JUDGE_PORTS]
_aiohttp_session = None
_call_counter = None


def _strip_think_tags(text):
    """Strip <think>...</think> tags from model response."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL)
    return text.strip()


def parse_judge_result(raw_response):
    """Parse judge response and return score (1.0 for pass, 0.0 for fail).

    Handles Qwen3's <think>...</think> reasoning tags by stripping them first.
    """
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


def _call_local_judge_sync(system_prompt, user_prompt, _max_retries=3):
    """Synchronous call to a local vLLM judge server.

    Returns raw response content string.
    """
    global _call_counter
    if _call_counter is None:
        _call_counter = os.getpid() % len(_JUDGE_URLS)

    judge_url = _JUDGE_URLS[_call_counter % len(_JUDGE_URLS)]
    _call_counter += 1

    payload = {
        "model": JUDGE_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 2048,
    }
    body = json.dumps(payload).encode("utf-8")

    last_err = None
    for attempt in range(_max_retries):
        try:
            req = urllib.request.Request(
                judge_url, data=body, method="POST",
                headers={"Content-Type": "application/json"},
            )
            resp = urllib.request.urlopen(req, timeout=180)
            result = json.loads(resp.read().decode("utf-8"))
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            last_err = e
            if attempt < _max_retries - 1:
                time.sleep(1)
                continue
            raise RuntimeError(
                f"Local judge failed after {_max_retries} attempts: {e}"
            ) from None


async def _call_local_judge_async(system_prompt, user_prompt):
    """Async call to a local vLLM judge server via aiohttp.

    Returns raw response content string. No retries — fail fast.
    """
    global _aiohttp_session
    import aiohttp

    if _aiohttp_session is None or _aiohttp_session.closed:
        timeout = aiohttp.ClientTimeout(total=180, sock_connect=5, sock_read=180)
        _aiohttp_session = aiohttp.ClientSession(timeout=timeout)

    payload = {
        "model": JUDGE_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 2048,
    }
    body = json.dumps(payload)

    # Route by user_prompt hash for prefix cache reuse
    judge_idx = hash(user_prompt) % len(_JUDGE_URLS)
    judge_url = _JUDGE_URLS[judge_idx]

    try:
        async with _aiohttp_session.post(
            judge_url,
            data=body,
            headers={"Content-Type": "application/json"},
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Local judge HTTP {resp.status} from {judge_url}")
            result = await resp.json()
        return result["choices"][0]["message"]["content"]
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Local judge error: {type(e).__name__}: {e}") from None


# ============================================================================
# Reward Functions
# ============================================================================
_CALL_COUNT = 0
_JUDGE_SUCCESS = 0
_JUDGE_FAIL = 0
_LOG_INTERVAL = 20


def medical_soap_combined_reward_fn(
    prompt, response, prompt_ids=None, completion_ids=None,
    ground_truth=None, data=None, **kwargs,
):
    """Synchronous reward function for medical SOAP RL training.

    Compatible with AReaL's RLVRWorkflow reward_fn signature.

    Reward structure (0-5):
      - Format reward: 1.0 if valid JSON with S, O, A, P keys, else 0.0
      - Section rewards: 1.0 each for S, O, A, P via LLM judge (0-4)

    Args:
        prompt: The input prompt string.
        response: The model's completion/response string.
        data: Full dataset record dict with dialogue, pred_S/O/A/P.

    Returns:
        float: Reward value in [0.0, 5.0].
    """
    global _CALL_COUNT, _JUDGE_SUCCESS, _JUDGE_FAIL
    _CALL_COUNT += 1

    from medical_soap.reward import extract_json_from_response, validate_soap_json
    from medical_soap.prompt_utils import (
        JUDGE_SECTION_SYSTEM_PROMPT,
        create_section_judge_prompt,
    )

    # Strip <think> tags from model response
    clean_response = _strip_think_tags(response)

    # Stage 1: JSON format check
    parsed = extract_json_from_response(clean_response)
    if parsed is None or not validate_soap_json(parsed):
        if _CALL_COUNT <= 3:
            print(f"[medical_soap_reward] Call #{_CALL_COUNT}: format invalid, reward=0.0")
        return 0.0

    format_reward = 1.0

    # Stage 2: Section-level judge evaluation
    # AReaL passes dataset fields as **kwargs (not as data=dict)
    dialogue = kwargs.get("dialogue", "")
    ground_truth_sections = {
        "S": kwargs.get("pred_S", ""),
        "O": kwargs.get("pred_O", ""),
        "A": kwargs.get("pred_A", ""),
        "P": kwargs.get("pred_P", ""),
    }

    # Fallback: check data dict if kwargs are empty (for direct calls)
    if not dialogue and data and isinstance(data, dict):
        dialogue = data.get("dialogue", "")
        ground_truth_sections = {
            "S": data.get("pred_S", ""),
            "O": data.get("pred_O", ""),
            "A": data.get("pred_A", ""),
            "P": data.get("pred_P", ""),
        }

    section_total = 0.0
    for key in ["S", "O", "A", "P"]:
        user_prompt = create_section_judge_prompt(
            dialogue, key, ground_truth_sections[key], parsed[key],
        )
        try:
            raw_response = _call_local_judge_sync(JUDGE_SECTION_SYSTEM_PROMPT, user_prompt)
            score = parse_judge_result(raw_response)
            _JUDGE_SUCCESS += 1
        except Exception:
            _JUDGE_FAIL += 1
            score = 0.0
        section_total += score

    combined = format_reward + section_total

    if _CALL_COUNT <= 3:
        print(f"[medical_soap_reward] Call #{_CALL_COUNT}: "
              f"format={format_reward:.1f} sections={section_total:.1f} total={combined:.1f}")

    if _CALL_COUNT % _LOG_INTERVAL == 0:
        total = _JUDGE_SUCCESS + _JUDGE_FAIL
        pct = (_JUDGE_FAIL / total * 100) if total else 0
        print(f"[medical_soap_reward] Stats @ call {_CALL_COUNT}: "
              f"judge {_JUDGE_SUCCESS}/{total} ok ({pct:.1f}% fail)")

    return combined


async def async_medical_soap_reward_fn(
    prompt, response, prompt_ids=None, completion_ids=None,
    ground_truth=None, data=None, **kwargs,
):
    """Async reward function for medical SOAP RL training.

    Native asyncio version that uses aiohttp to call local vLLM judges.
    Returns (reward, sub_scores_dict) for W&B logging.

    Args:
        prompt: The input prompt string.
        response: The model's completion/response string.
        data: Full dataset record dict with dialogue, pred_S/O/A/P.

    Returns:
        tuple: (float reward in [0.0, 5.0], dict of sub-scores)
    """
    import asyncio

    global _CALL_COUNT, _JUDGE_SUCCESS, _JUDGE_FAIL
    _CALL_COUNT += 1

    from medical_soap.reward import extract_json_from_response, validate_soap_json
    from medical_soap.prompt_utils import (
        JUDGE_SECTION_SYSTEM_PROMPT,
        create_section_judge_prompt,
    )

    # Strip <think> tags from model response
    clean_response = _strip_think_tags(response)

    # Stage 1: JSON format check
    parsed = extract_json_from_response(clean_response)
    if parsed is None or not validate_soap_json(parsed):
        if _CALL_COUNT <= 3:
            print(f"[async_medical_soap_reward] Call #{_CALL_COUNT}: format invalid, reward=0.0")
        return 0.0, {"format_reward": 0.0, "S": 0.0, "O": 0.0, "A": 0.0, "P": 0.0}

    format_reward = 1.0

    # Stage 2: Section-level judge evaluation (all 4 in parallel)
    # AReaL passes dataset fields as **kwargs (not as data=dict)
    dialogue = kwargs.get("dialogue", "")
    ground_truth_sections = {
        "S": kwargs.get("pred_S", ""),
        "O": kwargs.get("pred_O", ""),
        "A": kwargs.get("pred_A", ""),
        "P": kwargs.get("pred_P", ""),
    }

    # Fallback: check data dict if kwargs are empty (for direct calls)
    if not dialogue and data and isinstance(data, dict):
        dialogue = data.get("dialogue", "")
        ground_truth_sections = {
            "S": data.get("pred_S", ""),
            "O": data.get("pred_O", ""),
            "A": data.get("pred_A", ""),
            "P": data.get("pred_P", ""),
        }

    async def _judge_section(key):
        user_prompt = create_section_judge_prompt(
            dialogue, key, ground_truth_sections[key], parsed[key],
        )
        try:
            raw_response = await _call_local_judge_async(
                JUDGE_SECTION_SYSTEM_PROMPT, user_prompt,
            )
            score = parse_judge_result(raw_response)
            return score
        except Exception:
            return 0.0

    results = await asyncio.gather(
        _judge_section("S"),
        _judge_section("O"),
        _judge_section("A"),
        _judge_section("P"),
    )
    score_S, score_O, score_A, score_P = results

    # Track judge stats (approximate — parallel calls counted together)
    _JUDGE_SUCCESS += 4
    section_total = score_S + score_O + score_A + score_P
    combined = format_reward + section_total

    sub_scores = {
        "format_reward": format_reward,
        "S": score_S,
        "O": score_O,
        "A": score_A,
        "P": score_P,
        "sections_total": section_total,
    }

    if _CALL_COUNT <= 5:
        print(f"[async_medical_soap_reward] Call #{_CALL_COUNT}: "
              f"dialogue_len={len(dialogue)} "
              f"gt_S_len={len(ground_truth_sections['S'])} "
              f"gt_O_len={len(ground_truth_sections['O'])} "
              f"gt_A_len={len(ground_truth_sections['A'])} "
              f"gt_P_len={len(ground_truth_sections['P'])} "
              f"format={format_reward:.0f} S={score_S:.0f} O={score_O:.0f} "
              f"A={score_A:.0f} P={score_P:.0f} total={combined:.1f}")
        sys.stdout.flush()

    if _CALL_COUNT % _LOG_INTERVAL == 0:
        total = _JUDGE_SUCCESS + _JUDGE_FAIL
        pct = (_JUDGE_FAIL / total * 100) if total else 0
        print(f"[async_medical_soap_reward] Stats @ call {_CALL_COUNT}: "
              f"judge {_JUDGE_SUCCESS}/{total} ok ({pct:.1f}% fail)")

    return combined, sub_scores


# ============================================================================
# AReaL Reward API Patch
# ============================================================================
_NEW_CALL_METHOD = '''\
    _async_reward_fn_resolved = None

    async def __call__(self, *args, **kwargs) -> float:
        """Native async reward — bypasses ProcessPoolExecutor.
        Patched by run_medical_soap.py.
        """
        cls = type(self)
        if cls._async_reward_fn_resolved is None:
            try:
                from medical_soap.run_medical_soap import async_medical_soap_reward_fn
                cls._async_reward_fn_resolved = async_medical_soap_reward_fn
                logger.info("AsyncRewardWrapper: using native async reward path")
            except ImportError:
                logger.warning("async_medical_soap_reward_fn not found, falling back to sync path")
                cls._async_reward_fn_resolved = False

        if cls._async_reward_fn_resolved and cls._async_reward_fn_resolved is not False:
            result = await asyncio.wait_for(
                cls._async_reward_fn_resolved(*args, **kwargs),
                timeout=self.timeout_seconds,
            )
            if isinstance(result, tuple):
                reward, sub_scores = result
                from areal.utils import stats_tracker
                from areal.infra import workflow_context
                scope = workflow_context.stat_scope()
                stats_tracker.get(scope).scalar(**{
                    f"reward/{k}": v for k, v in sub_scores.items()
                })
                return reward
            return result

        # Fallback: original ProcessPoolExecutor path
        with self._lock:
            executor = self._executors.get(self._executor_key)
        if executor is None:
            raise RuntimeError("ProcessPoolExecutor has been shut down")
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(
            executor, partial(self.reward_fn, *args, **kwargs),
        )
        return await asyncio.wait_for(future, timeout=self.timeout_seconds)'''


def _patch_reward_api():
    """Patch AReaL's AsyncRewardWrapper for native async + longer timeout."""
    for path in ["/opt/venv/snowbook/lib/python3.12/site-packages/areal/api/reward_api.py",
                 "/opt/venv/snowbook/lib/python3.11/site-packages/areal/api/reward_api.py",
                 "/AReaL/src/areal/api/reward_api.py",
                 "/AReaL/areal/api/reward_api.py"]:
        if not os.path.exists(path):
            continue

        with open(path, "r") as f:
            content = f.read()
        original = content

        # Patch 1: timeout 15s -> 300s
        content = re.sub(
            r'timeout_seconds:\s*float\s*=\s*15\b',
            'timeout_seconds: float = 300',
            content,
        )

        # Patch 2: replace __call__ with native async version
        pattern = r'(    async def __call__\(self.*?\n)(.*?)(?=\n    (?:def |async def |@)|$)'
        match = re.search(pattern, content, re.DOTALL)
        if match:
            content = content[:match.start()] + _NEW_CALL_METHOD + content[match.end():]

        if content != original:
            with open(path, "w") as f:
                f.write(content)
            print(f"  Patched {path}: timeout=300s, native async __call__")
        else:
            print(f"  WARNING: No patches matched in {path}")
        return

    print("  WARNING: reward_api.py not found")


# ============================================================================
# Checkpoint Download (warm-start)
# ============================================================================
def download_checkpoint(stage_path):
    """Download checkpoint from Snowflake stage via presigned URLs."""
    import ssl
    import urllib.error

    local_model = "/tmp/init_model"
    if os.path.exists(local_model):
        shutil.rmtree(local_model)
    os.makedirs(local_model, exist_ok=True)

    host = os.environ.get("SNOWFLAKE_HOST", "")
    ctx = ssl.create_default_context()
    api_url = f"https://{host}/api/v2/statements"

    def _run_sql(sql_text):
        token = _get_spcs_token()
        payload = json.dumps({
            "statement": sql_text,
            "timeout": 120,
            "resultSetMetaData": {"format": "jsonv2"},
            "warehouse": os.environ.get("CORTEX_WAREHOUSE", "ADMIN_WH"),
            "database": DATA_DATABASE,
            "schema": DATA_SCHEMA,
        }).encode("utf-8")
        req = urllib.request.Request(api_url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        req.add_header("Authorization", f"Bearer {token}")
        try:
            resp = urllib.request.urlopen(req, context=ctx, timeout=120)
            return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  SQL REST API error: HTTP {e.code} — {body[:500]}")
            raise

    # List and download files
    list_result = _run_sql(f"LIST {stage_path}")
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
        url_result = _run_sql(f"SELECT GET_PRESIGNED_URL({stage_name}, '{rel_path}')")
        presigned_url = url_result["data"][0][0]
        urllib.request.urlretrieve(presigned_url, local_file)
        size_mb = os.path.getsize(local_file) / 1e6
        print(f"    {filename}: {size_mb:.1f} MB")

    print(f"  Contents: {os.listdir(local_model)}")
    return local_model


# ============================================================================
# Main
# ============================================================================
def main():
    import ray

    # 1. Start local vLLM judges
    start_judge_servers()

    # 2. Patch AReaL reward API
    print("\n--- Patching reward_api.py ---")
    _patch_reward_api()

    # 3. Copy reward module to local filesystem
    REWARD_MODULE_DIR = "/tmp/reward_modules"
    stage_src = "/mnt/job_stage/app"
    local_dst = os.path.join(REWARD_MODULE_DIR, "medical_soap")
    if os.path.exists(stage_src):
        os.makedirs(REWARD_MODULE_DIR, exist_ok=True)
        if os.path.exists(local_dst):
            shutil.rmtree(local_dst)
        shutil.copytree(stage_src, local_dst)
        print(f"  Copied reward module to {local_dst}")

    sys.path.insert(0, REWARD_MODULE_DIR)
    existing_pypath = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = (
        f"{REWARD_MODULE_DIR}:{existing_pypath}" if existing_pypath
        else REWARD_MODULE_DIR
    )
    print(f"  PYTHONPATH={os.environ['PYTHONPATH']}")

    # 4. Connect to Ray (pre-started by SPCS runtime)
    print("\n--- Ray Init ---")
    judge_ports_str = ",".join(str(p) for p in JUDGE_PORTS)
    env_vars = {
        "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
        "PATH": os.environ.get("PATH", ""),
        "LD_LIBRARY_PATH": os.environ.get("LD_LIBRARY_PATH", ""),
        "VIRTUAL_ENV": os.environ.get("VIRTUAL_ENV", ""),
        "HF_HOME": "/tmp/hf_cache",
        "TRANSFORMERS_CACHE": "/tmp/hf_cache",
        "LOCAL_JUDGE_PORTS": judge_ports_str,
        "LOCAL_JUDGE_MODEL": JUDGE_MODEL,
    }

    ray.init(address="auto", ignore_reinit_error=True,
             runtime_env={"env_vars": env_vars})
    print(f"  Connected to Ray: {ray.cluster_resources().get('GPU', 0)} GPUs")

    # 5. Import AReaL
    from areal import PPOTrainer
    from areal.api.cli_args import GRPOConfig, load_expr_config
    from areal.utils.hf_utils import load_hf_tokenizer

    # 6. Load config
    config, _ = load_expr_config(sys.argv[1:], GRPOConfig)

    # 6b. Warm-start from checkpoint (if configured)
    init_stage_path = os.environ.get("INIT_MODEL_STAGE_PATH", "")
    if init_stage_path:
        print(f"\n--- Loading Init Checkpoint ---")
        print(f"  Stage path: {init_stage_path}")
        local_model = download_checkpoint(init_stage_path)
        config.actor.path = local_model
        config.ref.path = local_model
        config.vllm.model = local_model
        config.tokenizer_path = local_model
        print(f"  actor.path -> {local_model}")

    tokenizer = load_hf_tokenizer(config.tokenizer_path)

    # 7. Load datasets
    print("\n--- Dataset Loading ---")
    train_dataset = load_medical_soap_dataset(config.train_dataset, tokenizer)
    valid_dataset = load_medical_soap_dataset(config.valid_dataset, tokenizer)

    # 8. Configure workflow
    reward_fn_path = "medical_soap.run_medical_soap.medical_soap_combined_reward_fn"
    workflow_kwargs = dict(
        reward_fn=reward_fn_path,
        gconfig=config.gconfig,
        tokenizer=config.tokenizer_path,
        enable_thinking=False,
    )
    eval_workflow_kwargs = workflow_kwargs.copy()
    eval_workflow_kwargs["gconfig"] = config.gconfig.new(temperature=0.6)

    # 9. Train
    print("\n--- Training ---")
    print(f"  Reward function: {reward_fn_path}")
    print(f"  Judges: {len(JUDGE_GPUS)}x local vLLM @ ports {JUDGE_PORTS}")
    print(f"  Checkpoints saved directly to: {config.saver.fileroot}")

    with PPOTrainer(
        config,
        train_dataset=train_dataset,
        valid_dataset=valid_dataset,
    ) as trainer:
        trainer.train(
            workflow="areal.workflow.rlvr.RLVRWorkflow",
            workflow_kwargs=workflow_kwargs,
            eval_workflow="areal.workflow.rlvr.RLVRWorkflow",
            eval_workflow_kwargs=eval_workflow_kwargs,
        )

    print("\n" + "=" * 60)
    print("Training complete. Model exported to stage.")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("=" * 60)
        print("FATAL ERROR:")
        print(traceback.format_exc())
        print("=" * 60)
        _kill_judges()
        sys.exit(1)
