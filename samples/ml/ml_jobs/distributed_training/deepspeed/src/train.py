"""Full-parameter instruction tuning with ordinary DeepSpeed ZeRO-3.

This file contains no Snowflake-specific imports. The DeepSpeed launcher
supplies the distributed environment exactly as it would on another cluster.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import tempfile
import time
from datetime import timedelta
from pathlib import Path

import deepspeed
import torch
import torch.distributed as dist
from datasets import load_dataset
from torch.utils.data import DataLoader, DistributedSampler
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    DataCollatorForLanguageModeling,
)


# Pinned reference workload; change these to point the sample at another model or dataset.
MODEL = "Qwen/Qwen3-1.7B"
MODEL_REVISION = "70d244cc86ccca08cf5af4e1e306ecf908b1ad5e"
DATASET = "databricks/databricks-dolly-15k"
DATASET_REVISION = "bdd27f4d94b9c1f951818a7da7fd7aeea5dbff1a"
DATASET_SPLIT = "train"
SEED = 42
LOG_EVERY = 5
OUTPUT_DIR = "output/deepspeed"
ZERO3_CONFIG = "zero3.json"
CACHE_DIR = os.environ.get("HF_HOME", "/tmp/huggingface")
COPY_BUFFER_BYTES = 16 * 1024 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local_rank", type=int, default=-1)
    parser.add_argument("--max-samples", type=int, default=2048)
    parser.add_argument("--sequence-length", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--max-steps", type=int, default=20)
    parser.add_argument("--learning-rate", type=float, default=2e-5)
    return parser.parse_args()


def format_example(example: dict[str, str], tokenizer) -> str:
    instruction = example["instruction"].strip()
    context = example.get("context", "").strip()
    if context:
        instruction = f"{instruction}\n\nContext:\n{context}"
    messages = [
        {"role": "user", "content": instruction},
        {"role": "assistant", "content": example["response"].strip()},
    ]
    return tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=False,
    )


def prepare_dataloader(
    args: argparse.Namespace,
    tokenizer,
    rank: int,
    world_size: int,
) -> tuple[DataLoader, DistributedSampler]:
    dataset = load_dataset(
        DATASET,
        split=DATASET_SPLIT,
        revision=DATASET_REVISION,
        cache_dir=CACHE_DIR,
    )
    if args.max_samples > 0:
        dataset = dataset.select(range(min(args.max_samples, len(dataset))))

    def tokenize(batch):
        texts = [
            format_example(
                {
                    "instruction": instruction,
                    "context": context,
                    "response": response,
                },
                tokenizer,
            )
            for instruction, context, response in zip(
                batch["instruction"],
                batch["context"],
                batch["response"],
            )
        ]
        return tokenizer(
            texts,
            max_length=args.sequence_length,
            padding="max_length",
            truncation=True,
            return_special_tokens_mask=True,
        )

    tokenized = dataset.map(
        tokenize,
        batched=True,
        remove_columns=dataset.column_names,
        desc="Tokenizing Dolly-15k",
    )
    sampler = DistributedSampler(
        tokenized,
        num_replicas=world_size,
        rank=rank,
        shuffle=True,
        seed=SEED,
        drop_last=True,
    )
    collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False)
    loader = DataLoader(
        tokenized,
        batch_size=args.batch_size,
        sampler=sampler,
        collate_fn=collator,
        num_workers=0,
        pin_memory=True,
        drop_last=True,
    )
    if not loader:
        raise ValueError(
            "The dataset is too small for the requested world size and batch size."
        )
    return loader, sampler


def consolidate_model(engine, rank: int) -> dict[str, torch.Tensor] | None:
    parameters = list(engine.module.parameters())
    with deepspeed.zero.GatheredParameters(parameters, modifier_rank=0):
        if rank != 0:
            return None
        return {
            name: value.detach().cpu().clone()
            for name, value in engine.module.state_dict().items()
        }


def publish_checkpoint(
    model,
    tokenizer,
    state_dict: dict[str, torch.Tensor],
    model_dir: Path,
) -> None:
    # Save to node-local /tmp then copy to the stage; writing safetensors
    # straight to the stage mount leaves a .tmp name, not model.safetensors.
    with tempfile.TemporaryDirectory(prefix="deepspeed-checkpoint-", dir="/tmp") as temp_dir:
        local_model_dir = Path(temp_dir) / "model"
        model.save_pretrained(local_model_dir, state_dict=state_dict, safe_serialization=True)
        tokenizer.save_pretrained(local_model_dir)
        model_dir.mkdir(parents=True, exist_ok=True)
        for source_path in sorted(local_model_dir.rglob("*")):
            destination_path = model_dir / source_path.relative_to(local_model_dir)
            if source_path.is_dir():
                destination_path.mkdir(parents=True, exist_ok=True)
                continue
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            with source_path.open("rb") as src, destination_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=COPY_BUFFER_BYTES)


def main() -> None:
    args = parse_args()
    deepspeed.init_distributed(timeout=timedelta(minutes=30))
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    local_rank = int(os.environ.get("LOCAL_RANK", args.local_rank))
    torch.cuda.set_device(local_rank)
    device = torch.device("cuda", local_rank)

    torch.manual_seed(SEED)
    torch.cuda.manual_seed_all(SEED)
    use_bf16 = torch.cuda.is_bf16_supported()
    model_dtype = torch.bfloat16 if use_bf16 else torch.float32

    if rank == 0:
        print(
            "[train] "
            f"world_size={world_size} model={MODEL}@{MODEL_REVISION} "
            f"dataset={DATASET}@{DATASET_REVISION}",
            flush=True,
        )

    tokenizer = AutoTokenizer.from_pretrained(
        MODEL,
        revision=MODEL_REVISION,
        cache_dir=CACHE_DIR,
    )
    if tokenizer.pad_token_id is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL,
        revision=MODEL_REVISION,
        cache_dir=CACHE_DIR,
        dtype=model_dtype,
    )
    model.config.use_cache = False
    model.config.pad_token_id = tokenizer.pad_token_id
    model.gradient_checkpointing_enable(
        gradient_checkpointing_kwargs={"use_reentrant": False}
    )

    optimizer = torch.optim.AdamW(model.parameters(), lr=args.learning_rate)
    config = json.loads(Path(ZERO3_CONFIG).read_text(encoding="utf-8"))
    config["bf16"]["enabled"] = use_bf16
    config["train_micro_batch_size_per_gpu"] = args.batch_size

    engine, optimizer, _, _ = deepspeed.initialize(
        model=model,
        optimizer=optimizer,
        config=config,
        dist_init_required=False,
    )
    loader, sampler = prepare_dataloader(args, tokenizer, rank, world_size)

    global_step = 0
    local_loss_sum = 0.0
    local_tokens = 0
    epoch = 0
    engine.train()
    dist.barrier()
    started_at = time.monotonic()

    while global_step < args.max_steps:
        sampler.set_epoch(epoch)
        for batch in loader:
            batch = {
                name: value.to(device, non_blocking=True)
                for name, value in batch.items()
            }
            outputs = engine(**batch)
            loss = outputs.loss
            engine.backward(loss)
            engine.step()

            global_step += 1
            local_loss_sum += loss.detach().float().item()
            local_tokens += int(batch["attention_mask"].sum().item())

            if global_step % LOG_EVERY == 0 or global_step == args.max_steps:
                current = torch.tensor(
                    [loss.detach().float().item(), 1.0],
                    dtype=torch.float64,
                    device=device,
                )
                dist.all_reduce(current)
                if rank == 0:
                    print(
                        f"[train] step={global_step}/{args.max_steps} "
                        f"loss={current[0].item() / current[1].item():.4f}",
                        flush=True,
                    )
            if global_step >= args.max_steps:
                break
        epoch += 1

    dist.barrier()
    elapsed = torch.tensor(
        time.monotonic() - started_at,
        dtype=torch.float64,
        device=device,
    )
    totals = torch.tensor(
        [local_loss_sum, float(global_step), float(local_tokens)],
        dtype=torch.float64,
        device=device,
    )
    dist.all_reduce(totals)
    dist.all_reduce(elapsed, op=dist.ReduceOp.MAX)
    loss = totals[0].item() / max(totals[1].item(), 1.0)
    global_tokens = int(totals[2].item())

    state_dict = consolidate_model(engine, rank)
    if rank == 0:
        assert state_dict is not None
        output_dir = Path(OUTPUT_DIR)
        model_dir = output_dir / "model"
        publish_checkpoint(engine.module, tokenizer, state_dict, model_dir)
        metrics = {
            "dataset": DATASET,
            "dataset_revision": DATASET_REVISION,
            "global_tokens": global_tokens,
            "loss": loss,
            "max_steps_per_rank": global_step,
            "model": MODEL,
            "model_revision": MODEL_REVISION,
            "perplexity": math.exp(loss) if loss < 100 else None,
            "tokens_per_second": global_tokens / max(elapsed.item(), 1e-9),
            "world_size": world_size,
            "zero_stage": 3,
        }
        (output_dir / "metrics.json").write_text(
            json.dumps(metrics, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print("[train] metrics=" + json.dumps(metrics, sort_keys=True), flush=True)
        print(f"[train] checkpoint={model_dir}", flush=True)

    dist.barrier()
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
