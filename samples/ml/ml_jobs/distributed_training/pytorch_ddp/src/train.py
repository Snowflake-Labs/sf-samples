"""Continue training a causal language model with ordinary PyTorch DDP.

This file deliberately contains no Snowflake-specific imports or topology
handling. torchrun supplies RANK, WORLD_SIZE, LOCAL_RANK, MASTER_ADDR, and
MASTER_PORT, exactly as it would outside Snowflake.
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

import torch
import torch.distributed as dist
from datasets import load_dataset
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    DataCollatorForLanguageModeling,
)


# Pinned reference workload; change these to point the sample at another model or dataset.
MODEL = "Qwen/Qwen3-0.6B"
MODEL_REVISION = "c1899de289a04d12100db370d81485cdf75e47ca"
DATASET = "Salesforce/wikitext"
DATASET_CONFIG = "wikitext-103-raw-v1"
DATASET_REVISION = "b08601e04326c79dfdd32d625aee71d232d685c3"
DATASET_SPLIT = "train"
TEXT_COLUMN = "text"
SEED = 42
LOG_EVERY = 5
OUTPUT_DIR = "output/pytorch_ddp"
CACHE_DIR = os.environ.get("HF_HOME", "/tmp/huggingface")
COPY_BUFFER_BYTES = 16 * 1024 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continue training a causal language model with PyTorch DDP."
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=4096,
        help="Non-empty examples to train on; 0 uses the full split.",
    )
    parser.add_argument(
        "--sequence-length",
        type=int,
        default=256,
        help="Tokens per example after padding and truncation.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Per-process batch size.",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=20,
        help="Optimizer steps to run.",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=5e-5,
        help="AdamW learning rate.",
    )
    return parser.parse_args()


def distributed_coordinates() -> tuple[int, int, int]:
    required = ("RANK", "WORLD_SIZE", "LOCAL_RANK")
    missing = [name for name in required if name not in os.environ]
    if missing:
        raise RuntimeError(
            "This program must be launched with torchrun; missing "
            + ", ".join(missing)
        )
    return (
        int(os.environ["RANK"]),
        int(os.environ["WORLD_SIZE"]),
        int(os.environ["LOCAL_RANK"]),
    )


def prepare_dataset(
    args: argparse.Namespace,
    tokenizer,
    rank: int,
    world_size: int,
) -> tuple[DataLoader, DistributedSampler]:
    dataset = load_dataset(
        DATASET,
        DATASET_CONFIG,
        split=DATASET_SPLIT,
        revision=DATASET_REVISION,
        cache_dir=CACHE_DIR,
    )
    if TEXT_COLUMN not in dataset.column_names:
        raise ValueError(
            f"Text column {TEXT_COLUMN!r} not found in {dataset.column_names}"
        )

    dataset = dataset.filter(
        lambda row: bool(row[TEXT_COLUMN] and row[TEXT_COLUMN].strip())
    )
    if args.max_samples > 0:
        dataset = dataset.select(range(min(args.max_samples, len(dataset))))

    def tokenize(batch):
        return tokenizer(
            batch[TEXT_COLUMN],
            max_length=args.sequence_length,
            padding="max_length",
            truncation=True,
            return_special_tokens_mask=True,
        )

    tokenized = dataset.map(
        tokenize,
        batched=True,
        remove_columns=dataset.column_names,
        desc="Tokenizing",
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
        pin_memory=torch.cuda.is_available(),
        drop_last=True,
    )
    if not loader:
        raise ValueError(
            "The dataset is too small for the requested world size and batch size."
        )
    return loader, sampler


def reduce_metrics(
    loss_sum: float,
    steps: int,
    tokens: int,
    elapsed_seconds: float,
    device: torch.device,
) -> tuple[float, int, float]:
    totals = torch.tensor(
        [loss_sum, float(steps), float(tokens)],
        dtype=torch.float64,
        device=device,
    )
    dist.all_reduce(totals, op=dist.ReduceOp.SUM)

    elapsed = torch.tensor(elapsed_seconds, dtype=torch.float64, device=device)
    dist.all_reduce(elapsed, op=dist.ReduceOp.MAX)

    global_loss = totals[0].item() / max(totals[1].item(), 1.0)
    global_tokens = int(totals[2].item())
    tokens_per_second = global_tokens / max(elapsed.item(), 1e-9)
    return global_loss, global_tokens, tokens_per_second


def publish_checkpoint(model, tokenizer, model_dir: Path) -> None:
    # Save to node-local /tmp then copy to the stage; writing safetensors
    # straight to the stage mount leaves a .tmp name, not model.safetensors.
    with tempfile.TemporaryDirectory(prefix="pytorch-ddp-checkpoint-", dir="/tmp") as temp_dir:
        local_model_dir = Path(temp_dir) / "model"
        model.save_pretrained(local_model_dir, safe_serialization=True)
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
    rank, world_size, local_rank = distributed_coordinates()

    use_cuda = torch.cuda.is_available()
    if use_cuda:
        torch.cuda.set_device(local_rank)
        device = torch.device("cuda", local_rank)
        backend = "nccl"
    else:
        device = torch.device("cpu")
        backend = "gloo"

    dist.init_process_group(
        backend=backend,
        init_method="env://",
        timeout=timedelta(minutes=30),
    )
    try:
        torch.manual_seed(SEED)
        if use_cuda:
            torch.cuda.manual_seed_all(SEED)

        use_bf16 = use_cuda and torch.cuda.is_bf16_supported()
        model_dtype = torch.bfloat16 if use_bf16 else torch.float32

        if rank == 0:
            print(
                "[train] "
                f"world_size={world_size} backend={backend} "
                f"model={MODEL}@{MODEL_REVISION} "
                f"dataset={DATASET}/{DATASET_CONFIG}"
                f"@{DATASET_REVISION}",
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
        model.to(device)

        distributed_model = DistributedDataParallel(
            model,
            device_ids=[local_rank] if use_cuda else None,
            output_device=local_rank if use_cuda else None,
            broadcast_buffers=False,
        )
        optimizer = torch.optim.AdamW(
            distributed_model.parameters(),
            lr=args.learning_rate,
        )
        loader, sampler = prepare_dataset(args, tokenizer, rank, world_size)

        global_step = 0
        local_loss_sum = 0.0
        local_tokens = 0
        epoch = 0

        dist.barrier()
        started_at = time.monotonic()
        distributed_model.train()

        while global_step < args.max_steps:
            sampler.set_epoch(epoch)
            for batch in loader:
                batch = {
                    name: value.to(device, non_blocking=use_cuda)
                    for name, value in batch.items()
                }
                optimizer.zero_grad(set_to_none=True)
                outputs = distributed_model(**batch)
                loss = outputs.loss
                loss.backward()
                optimizer.step()

                global_step += 1
                local_loss_sum += loss.detach().float().item()
                local_tokens += int(batch["attention_mask"].sum().item())

                if global_step % LOG_EVERY == 0 or global_step == args.max_steps:
                    window_loss = torch.tensor(
                        [loss.detach().float().item(), 1.0],
                        dtype=torch.float64,
                        device=device,
                    )
                    dist.all_reduce(window_loss, op=dist.ReduceOp.SUM)
                    if rank == 0:
                        print(
                            f"[train] step={global_step}/{args.max_steps} "
                            f"loss={window_loss[0].item() / window_loss[1].item():.4f}",
                            flush=True,
                        )

                if global_step >= args.max_steps:
                    break
            epoch += 1

        dist.barrier()
        elapsed_seconds = time.monotonic() - started_at
        global_loss, global_tokens, tokens_per_second = reduce_metrics(
            local_loss_sum,
            global_step,
            local_tokens,
            elapsed_seconds,
            device,
        )

        try:
            perplexity = math.exp(global_loss)
        except OverflowError:
            perplexity = None

        metrics = {
            "backend": backend,
            "dataset": DATASET,
            "dataset_config": DATASET_CONFIG,
            "dataset_revision": DATASET_REVISION,
            "global_tokens": global_tokens,
            "loss": global_loss,
            "max_steps_per_rank": global_step,
            "model": MODEL,
            "model_revision": MODEL_REVISION,
            "perplexity": perplexity,
            "tokens_per_second": tokens_per_second,
            "world_size": world_size,
        }

        if rank == 0:
            output_dir = Path(OUTPUT_DIR)
            model_dir = output_dir / "model"
            publish_checkpoint(distributed_model.module, tokenizer, model_dir)
            (output_dir / "metrics.json").write_text(
                json.dumps(metrics, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            print("[train] metrics=" + json.dumps(metrics, sort_keys=True), flush=True)
            print(f"[train] checkpoint={model_dir}", flush=True)

        dist.barrier()
    finally:
        if dist.is_initialized():
            dist.destroy_process_group()


if __name__ == "__main__":
    main()
