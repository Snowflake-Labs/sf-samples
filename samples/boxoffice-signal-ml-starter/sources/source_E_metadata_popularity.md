# Source E — Title metadata & popularity (use with care)

This one is here mostly as a **cautionary note**: a source that looks helpful but whose
headline metric you should keep out of the model.

## The signal
Structured metadata about a title — budget, runtime, genre, cast, ratings — often bundled
with a platform-computed **"popularity" score** that updates continuously.

## Options CoCo can help you choose from
Several movie metadata databases and catalog APIs provide these fields. Ask CoCo for the
options; most solo projects use one keyed metadata API for **static** attributes only.

## Why the popularity score is a trap (leakage)
A live popularity score looks like a strong predictor, and early versions leaned on it — but
it's contaminated by activity that co-moves with the outcome (it reflects buzz that itself
responds to the release). Feeding it in inflates offline accuracy and **leaks** signal you
won't have cleanly at prediction time; under walk-forward validation it doesn't hold up. The
current model **drops it entirely**.

Use such a source for **static fields** (budget, runtime, genre, cast) if you need them —
just never feed the live popularity metric into the model.

## Feeds these columns
- `MOVIE_METADATA` (static fields only — note there is intentionally **no** popularity column).

## Ask CoCo
> "Read `sources/source_E_metadata_popularity.md`. What are my options for a title-metadata
> source? I only want static fields (budget, runtime, genre). Explain why a live popularity
> score is a leakage risk and make sure it never enters my feature view."
