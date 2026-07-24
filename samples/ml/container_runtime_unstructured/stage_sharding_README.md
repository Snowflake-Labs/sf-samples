# Stage Sharding — fast reads for many small unstructured files

---

## TL;DR

If your training data is **millions of small unstructured files on a Snowflake stage**
(images, audio, …), reading them every epoch is slow because **each file is its own
network round-trip**. **Stage sharding** repacks those many small files into a
**handful of large `.arrow` shards, once** — keeping their original byte footprint (no
data bloat) — so every later training run reads a few big files instead of millions of
small ones.

- **One-time step:** `generate_shards(...)` repacks your raw files into large
  `.arrow` shards on the stage.
- **Every training run:** read the shards back with a single Ray datasource.
- **No data bloat:** shards store your bytes **verbatim** (a JPEG stays a JPEG), so the
  sharded copy is ~the same total size as your raw files — sharding cuts *round-trips*,
  not bytes.
- **One knob:** `target_shard_size_mb`.
- **The catch:** creating the shards is **one full pass over your data**, so the win
  only shows up when you read the sharded data **more than once** (multiple epochs,
  re-runs, sweeps) — not on a single one-shot pass.
- **Typical result:** **~4–7× faster reads per epoch**, paying for itself in under
  ~2 epochs.

---

## The flow at a glance

Two phases, three API calls. You pay a **one-time** repack, then every training run
reads the shards fast:

```text
  ── ONE-TIME, offline (one full pass over the data) ─────────────────

    @MY_STAGE/raw_images/          millions of small files (img0.jpg, …)
         │
         │   ①  generate_shards(src, dest, file_pattern="*.jpg",
         ▼                       target_shard_size_mb=256)
    @MY_STAGE/shards_images/       a few big shards (shard_0.arrow, …)


  ── EVERY TRAINING RUN (this is the fast part) ──────────────────────

    @MY_STAGE/shards_images/
         │
         │   ②  ray.data.read_datasource(
         ▼          SFStageShardDataSource(stage_location="@…/shards_images/"))
    Ray Dataset  (raw `bytes` column)
         │
         │   ③  ds.map_batches(decode_fn)        # you own the decode
         ▼
    Decoded dataset  ──▶  PyTorch / XGBoost / LightGBM / Ray Train
```

**The three calls, in order:**

1. **`generate_shards(raw, shards, file_pattern="*.jpg", target_shard_size_mb=256)`**
   — run **once**, offline, to repack the small files into shards.
2. **`ray.data.read_datasource(SFStageShardDataSource(stage_location="@…/shards_images/"))`**
   — run **every training run** to read the shards back.
3. **`ds.map_batches(decode_fn)`** — decode the raw `bytes` into tensors/features.

> **Why the payoff needs multiple epochs.** Step ① reads *every* source file exactly
> once and writes the shards — so the repack costs roughly one slow-path epoch up
> front. You only come out ahead once you've read the sharded data enough times for
> the per-epoch savings to outweigh that one-time pass. In practice that's under ~2
> epochs; a single one-shot read won't benefit. If you'll only ever read the data
> once, don't shard it.

---

## The problem it solves

Reading a file from a Snowflake stage isn't free: for every single file, the reader
has to look up a URL for it and then fetch it over the network. That per-file
overhead is invisible when you have a few big files — but with **1 million small
files and hundreds of parallel workers**, that overhead *is* the bottleneck. The
network spends most of its time on handshakes, not on moving your data.

Small-file datasets are exactly where distributed ML lives: image classification,
audio, video, and document corpora all tend to be "lots of tiny objects." (Already-
columnar data like a few big Parquet files doesn't have this problem — see below.)

## What stage sharding does

Stage sharding **repacks** many small files into a few large **Arrow files**
("shards"), stored back on the same stage.

- Your original bytes are stored **verbatim** — no
  re-compression, bloated bytes. Nothing is decoded or transformed at repack time,
  so the sharded copy is **~the same total size as your raw data — no bloat**.
- Reads afterward touch **a few big files instead of millions of small ones**, so the
  per-file network overhead nearly disappears.
- The amount of data on the wire is unchanged (bytes stay compressed); what shrinks
  is the *number of round-trips*.

Think of it as **"zip a folder of tiny files into a few big archives, once"** — but in
a format your training loop can stream directly, with no unzip step.

## When to use it — and when not to

**Use it when:**
- You have **many small files** (thousands to millions), and/or
- You read the same dataset **more than once** — multiple epochs, repeated
  experiments, hyperparameter sweeps.

**Skip it when:**
- Your data is already **a few large files** (e.g. a handful of big Parquet files) —
  there's little per-file overhead to remove.
- You'll read the data **once** and never again, *and* it's small — the one-time
  repack may not pay for itself. (Break-even is typically under ~2 epochs, so most
  training jobs come out ahead.)

## How to use it — two steps

1. **Generate shards — once, offline.** `generate_shards(src, dest, file_pattern="*.jpg")`
   repacks your raw files into a few `shard_*.arrow` files on the stage. Re-run only
   when the raw data changes.
2. **Read the shards — every training run.**
   `ray.data.read_datasource(SFStageShardDataSource(stage_location="@…/shards_images/"))`
   returns a Ray dataset with a raw `bytes` column; `ds.map_batches(decode_fn)` turns
   those bytes into tensors/features. Because the bytes are stored raw, **you own the
   decoding** — any file type and any decode pipeline works — and the dataset plugs
   straight into the PyTorch / XGBoost / LightGBM distributors and Ray Train.

The **[`stage_sharding_quickstart.ipynb`](stage_sharding_quickstart.ipynb)** notebook is
the full runnable walkthrough — both calls with real code (including the decode
function and the trailing-slash detail), plus a `ShardedDataConnector` +
`PyTorchDistributor` GPU training example. This README stays conceptual so it doesn't
duplicate the notebook.

## The one knob: `target_shard_size_mb`

Shard size is the only sizing control, and it's a simple trade-off:

| Larger shards (e.g. 256 MB) | Smaller shards (e.g. 32–64 MB) |
| --- | --- |
| Fewer files → less per-file overhead | More files → more parallel readers |
| Fewer parallel read/decode units | More parallel read/decode units |

Each shard is read by exactly one worker at a time, so **the number of shards is your
read/decode parallelism**. Rule of thumb: **pick a size that produces at least as many
shards as you have workers.** For a ~2 GB dataset, `64 MB` → ~30 shards (lots of
parallelism); `256 MB` → ~8 shards (fine for a single node, thin for a big cluster).
The default is `256 MB`.

## What to expect

Internal benchmarks reading real JPEG datasets from a Snowflake stage (Ray + the
shard datasource), comparing per-file reads vs. sharded reads:

| Dataset | Per-file read | Sharded read | Per-epoch speedup | Pays for itself after |
| --- | --- | --- | --- | --- |
| 10k images (~185 MB) | ~49 s / epoch | ~7.3 s / epoch | **~6.7×** | ~1.8 epochs |
| 100k images (~1.85 GB) | ~510–630 s / epoch | ~115 s / epoch | **~4.4–5.5×** | ~1.2–1.5 epochs |

The one-time `generate_shards` step is counted in "pays for itself." Larger datasets
tend to benefit *more*, because they pack into enough shards to read fully in
parallel. Numbers vary with data, cluster size, and stage type; treat these as
representative, not guarantees.

**Benchmark setup (100k):**
- **Dataset**: 100,000 JPEGs (~1.85 GB total, ~19 KB avg) on a Snowflake internal stage.
- **Environment**: Ray 2.55 local mode on a Mac laptop (10 CPU cores, 2 GB Ray object store), reading from a Snowflake stage via presigned URLs.
- **Shard config**: `target_shard_size_mb=256` (default) → 8 shards (~276 MB each).
- **Decode**: Both paths decode to 224×224 RGB. Baseline (`SFStageImageDataSource`) decodes in-read; shard path decodes post-read via `map_batches`.
- **Batch size**: 2048 images per batch.
- **Method**: Average of 2 epochs per path, measured across two independent runs. Build time (~600s, counted in break-even) measured from shard timestamps.

**What the two runs showed:**

| Path | Run 1 | Run 2 | Notes |
| --- | --- | --- | --- |
| Baseline (`SFStageImageDataSource`) | 506 s/epoch | 631 s/epoch | Network-bound — varies with stage-read latency |
| Sharded (`SFStageShardDataSource`) | 114.6 s/epoch | 115.0 s/epoch | **Highly reproducible** |
| Speedup | 4.4× | 5.5× | |

The sharded path is **stable to within ~0.4%** across runs; the baseline swings with
stage-read network conditions, which is exactly the per-file overhead sharding
removes. The 4.4× speedup comes from **cutting 100k presigned-URL lookups + S3 GETs
down to 8**. The bytes on the wire are the same (JPEGs stay compressed); what shrinks
is the number of round-trips.

### End-to-end distributed training

The read numbers above translate directly into training time. We ran the **actual
`PyTorchDistributor`** (4 workers, DDP) over the same 100k images, feeding it two ways
— raw files via `SFStageImageDataSource` vs. shards via `SFStageShardDataSource` — with
everything else identical (batch 2048, 64×64 RGB, same model):

| Data source | Per-epoch | 5-epoch total |
| --- | --- | --- |
| `SFStageImageDataSource` (raw files) | ~717 s | **~3585 s (~60 min)** |
| `SFStageShardDataSource` (shards) | ~127 s | **~635 s (~11 min)** |
| **Speedup** | | **~5.6×** |

Per-epoch is the measured average from a 2-epoch run (image: 648 s + 786 s → ~717 s;
shard: 130 s + 124 s → ~127 s, incl. distributor setup); the 5-epoch totals scale that
linearly. Two takeaways:

- **Training is ~5.6× faster end-to-end with shards** — and the entire difference is data
  loading; the model and gradient math are identical. The GPU/CPU spends its time
  training instead of waiting on the network.
- **Adding workers doesn't rescue the raw-file path.** With 4 workers each pulling
  ~25k images, the image-datasource epoch (~650–790 s) is *no faster* than a
  single-process read — every worker still waits on the same per-file stage-read
  throughput. Shards remove that bottleneck (8 large reads instead of 100k small
  ones), so the workers stay fed.

## Good to know

- **Order is preserved.** Sharding doesn't shuffle. Shuffle at read time the usual Ray
  way — `ds.random_shuffle()` or `iter_batches(local_shuffle_buffer_size=...)`.
- **Named stages only.** Point at a named stage (`@MY_DB.MY_SCHEMA.MY_STAGE/...`); the
  user stage (`@~`) isn't supported.
- **Use a `file_pattern`.** Selecting `"*.jpg"` (etc.) keeps unrelated files out of the
  shards.
- **Re-running.** Writing again overwrites same-named shards; pass `force_cleanup=True`
  to first clear old `.arrow` shards in the destination folder for a clean replace.
- **Trailing slash on read.** `"@…/shards_images/"` anchors the read to exactly that
  folder; without it, listing is prefix-based and could also pick up a sibling like
  `shards_images_v2`.
- **Pairs well with the file cache.** If you enable the runtime's file-level disk cache
  (`use_file_cache=True`), the big shards are also cached on node-local disk after the
  first read, making later epochs faster still.

## API at a glance

**`generate_shards(src, dest, *, file_pattern=None, target_shard_size_mb=256, include_paths=True, force_cleanup=False, file_prefix="shard", overwrite=True, database=None, schema=None)`**
Repack raw files under `src` into large `.arrow` shards under `dest`. Run once.

**`SFStageShardDataSource(*, stage_location, file_pattern="*.arrow", columns=None, use_file_cache=None, database=None, schema=None)`**
Ray `Datasource` for reading shards. Use with `ray.data.read_datasource(...)`. Each row
has a `bytes` column (raw file bytes) and, if `include_paths=True` at generate time, a
`path` column (the original file path).
