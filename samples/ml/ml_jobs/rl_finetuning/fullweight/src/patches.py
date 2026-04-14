"""Runtime patches for AReaL v1.0.1 LoRA + SGLang compatibility on SPCS.

These patches fix bugs in AReaL v1.0.1 that prevent LoRA training with SGLang:
  - Fix 3: Version 0 LoRA guard (skip lora_path on initial rollout)
  - Fix 4: Reorder disk weight sync + bypass FSDP2 for LoRA saves
  - Fix 5: Add lora_target_modules to SGLangConfig
  - Fix 6: Bypass model.save_pretrained for LoRA checkpoints
  - Reward API: Fix kwargs unpacking + native async support
"""

import glob
import os
import re


def _patch_sglang_lora():
    """Patch AReaL for SGLang + LoRA compatibility.

    Fixes from local testing:
    3. Version 0 LoRA guard in sglang_remote.py — skip LoRA path at version 0
    4. Save-first reorder + skip tokenizer for LoRA in fsdp_engine.py
    5. Add lora_target_modules field to SGLangConfig in cli_args.py
    6. LoRA-aware _save_model_to_hf — bypass save_pretrained for LoRA
    """
    import glob as _g

    areal_dirs = _g.glob("/opt/venv/snowbook/lib/python3.*/site-packages/areal/")
    if not areal_dirs:
        print("  WARNING: areal package not found for SGLang patches")
        return
    areal_dir = areal_dirs[0]

    patched = []

    # Fix 3: Version 0 LoRA guard in sglang_remote.py
    sglang_remote = os.path.join(areal_dir, "engine", "sglang_remote.py")
    if os.path.exists(sglang_remote):
        with open(sglang_remote, "r") as f:
            content = f.read()
        if "# PATCHED: version 0 guard" not in content:
            old = 'payload["lora_path"] = get_versioned_lora_name(lora_name, version)'
            new = (
                '# PATCHED: version 0 guard — no LoRA loaded yet at version 0\n'
                '            if version > 0:\n'
                '                payload["lora_path"] = get_versioned_lora_name(lora_name, version)'
            )
            if old in content:
                content = content.replace(old, new, 1)
                with open(sglang_remote, "w") as f:
                    f.write(content)
                patched.append("sglang_remote.py: version 0 LoRA guard")
            else:
                print(f"  WARNING: Fix 3 patch target not found in {sglang_remote}")

    # Fix 4: Reorder _update_weights_from_disk + skip tokenizer for LoRA.
    # v1.0.1 has two bugs:
    #   Bug 1 (race): calls rollout_engine.update_weights_from_disk BEFORE saving
    #     weights to disk → SGLang tries to load a path that doesn't exist → 400
    #   Bug 2 (tokenizer): _save_model_to_hf saves tokenizer alongside LoRA weights,
    #     including added_tokens.json → SGLang LoRAConfig sets lora_added_tokens_size=26
    #     → fails memory pool check → 400
    # Fix: save first (skip tokenizer for LoRA), then tell SGLang to load.
    fsdp_engine = os.path.join(areal_dir, "engine", "fsdp_engine.py")
    if os.path.exists(fsdp_engine):
        with open(fsdp_engine, "r") as f:
            content = f.read()
        if "# PATCHED: save first, skip tokenizer for LoRA" not in content:
            old = (
                "    def _update_weights_from_disk(self, meta: WeightUpdateMeta):\n"
                "        fut = Future()\n"
                "\n"
                "        if dist.get_rank() == 0:\n"
                "            fut = self.rollout_engine.update_weights_from_disk(meta)\n"
                "\n"
                "        assert meta.path is not None\n"
                "        self._save_model_to_hf(meta.path, self.tokenizer, self.processor)\n"
                "        # dist.barrier() are called when _save_model_to_hf finished"
            )
            new = (
                "    def _update_weights_from_disk(self, meta: WeightUpdateMeta):\n"
                "        # PATCHED: save first, skip tokenizer for LoRA, bypass FSDP2 collective\n"
                "        assert meta.path is not None\n"
                "\n"
                "        if meta.use_lora:\n"
                "            # LoRA: save adapter via get_model_state_dict + safetensors\n"
                "            # model.save_pretrained crashes due to safetensors _find_shared_tensors\n"
                "            # accessing FSDP2-offloaded storage. Use state_dict directly.\n"
                "            from torch.distributed.checkpoint.state_dict import StateDictOptions, get_model_state_dict\n"
                "            _opts = StateDictOptions(full_state_dict=True, cpu_offload=True)\n"
                "            _sd = get_model_state_dict(self.model, options=_opts)\n"
                "            _lora_sd = {k.replace('.default.', '.'): v for k, v in _sd.items() if 'lora_' in k}\n"
                "            if dist.get_rank() == 0:\n"
                "                import os as _os\n"
                "                from safetensors.torch import save_file as _sf\n"
                "                _os.makedirs(meta.path, exist_ok=True)\n"
                "                _sf(_lora_sd, _os.path.join(meta.path, 'adapter_model.safetensors'))\n"
                "                self.model.peft_config['default'].save_pretrained(meta.path)\n"
                "                self.model_config.save_pretrained(meta.path)\n"
                "            dist.barrier(group=self.cpu_group)\n"
                "        else:\n"
                "            self._save_model_to_hf(meta.path, self.tokenizer, self.processor)\n"
                "\n"
                "        fut = Future()\n"
                "\n"
                "        if dist.get_rank() == 0:\n"
                "            fut = self.rollout_engine.update_weights_from_disk(meta)"
            )
            if old in content:
                content = content.replace(old, new, 1)
                with open(fsdp_engine, "w") as f:
                    f.write(content)
                patched.append("fsdp_engine.py: fixed disk weight sync (save-first + skip tokenizer)")
            else:
                print(f"  WARNING: Fix 4 patch target not found in {fsdp_engine}")

    # Fix 6: Patch _save_model_to_hf to avoid safetensors storage_ptr crash for LoRA.
    # FSDP2 get_model_state_dict returns valid CPU tensors, but model.save_pretrained
    # calls safetensors._find_shared_tensors which accesses the MODEL's parameters
    # (not the returned state_dict), hitting invalid storage from FSDP2 cpu_offload.
    # Fix: use get_model_state_dict + filter LoRA keys + save_file directly.
    if os.path.exists(fsdp_engine):
        with open(fsdp_engine, "r") as f:
            content = f.read()
        if "# PATCHED: LoRA-aware save" not in content:
            old_save = (
                '        # save huggingface model on rank 0\n'
                '        if dist.get_rank() == 0:\n'
                '            os.makedirs(path, exist_ok=True)\n'
                '            self.model.save_pretrained(path, state_dict=state_dict)\n'
                '            self.model_config.save_pretrained(path)'
            )
            new_save = (
                '        # PATCHED: LoRA-aware save — bypass model.save_pretrained for LoRA\n'
                '        # save_pretrained calls safetensors._find_shared_tensors which accesses\n'
                '        # FSDP2-offloaded model params → invalid storage crash.\n'
                '        # Instead: save state_dict directly with safetensors.save_file.\n'
                '        if dist.get_rank() == 0:\n'
                '            os.makedirs(path, exist_ok=True)\n'
                '            _is_lora = hasattr(self.model, "peft_config")\n'
                '            if _is_lora:\n'
                '                # Filter for LoRA adapter keys only\n'
                '                _lora_sd = {k.replace(".default.", "."): v for k, v in state_dict.items() if "lora_" in k}\n'
                '                from safetensors.torch import save_file as _sf\n'
                '                _sf(_lora_sd, os.path.join(path, "adapter_model.safetensors"))\n'
                '                self.model.peft_config["default"].save_pretrained(path)\n'
                '            else:\n'
                '                self.model.save_pretrained(path, state_dict=state_dict)\n'
                '            self.model_config.save_pretrained(path)'
            )
            if old_save in content:
                content = content.replace(old_save, new_save, 1)
                with open(fsdp_engine, "w") as f:
                    f.write(content)
                patched.append("fsdp_engine.py: LoRA-aware _save_model_to_hf (bypass save_pretrained)")
            else:
                print(f"  WARNING: Fix 6 patch target not found in fsdp_engine.py")

    # Fix 5: Add lora_target_modules field to SGLangConfig in cli_args.py
    # Required by SGLang when enable_lora=True — without it, SGLang can't properly
    # initialize the LoRA memory pool, causing silent hangs on first LoRA inference.
    # Must be patched BEFORE AReaL imports cli_args (happens in PPOTrainer.__init__).
    cli_args = os.path.join(areal_dir, "api", "cli_args.py")
    if os.path.exists(cli_args):
        with open(cli_args, "r") as f:
            content = f.read()
        if "lora_target_modules" not in content:
            old = "    max_lora_rank: int | None = None"
            new = (
                "    max_lora_rank: int | None = None\n"
                "    lora_target_modules: list[str] | None = None  # PATCHED: Fix 5"
            )
            if old in content:
                content = content.replace(old, new, 1)
                with open(cli_args, "w") as f:
                    f.write(content)
                patched.append("cli_args.py: added lora_target_modules to SGLangConfig")
            else:
                print(f"  WARNING: Fix 5 patch target not found in {cli_args}")

    if patched:
        for p in patched:
            print(f"  Patched {p}")
    else:
        print("  SGLang LoRA patches: no patches applied (already applied or targets not found)")


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

