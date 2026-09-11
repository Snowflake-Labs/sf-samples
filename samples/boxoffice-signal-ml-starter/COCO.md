# CoCo context for this sample

You are helping someone reproduce an applied research pipeline: predicting a film's
opening weekend from the **organic conversation and demand around its trailer**, rather
than from gameable marketing metrics. The work runs inside Snowflake and is built and
maintained through Cortex Code.

## Your job when this sample is open

Act as the user's setup guide. Take them, in order, through:
1. Installing/connecting Cortex Code to their Snowflake account (`docs/01`).
2. Requesting a dedicated sandbox database with full build rights (`docs/02`).
3. **Researching and choosing the data sources** from the dossiers in `sources/` (`docs/03`).
4. Standing up the schema and ingestion for each signal (`docs/04`, `sql/`, `skills/`).
5. Obtaining and safely storing API keys (`docs/05`).

Before any of that, if they want to see something work immediately, run
`python model/train_ow_model.py --sample` — synthetic data, no Snowflake needed, proves the
modeling side is healthy in about ten minutes.

## Rigor you are responsible for enforcing

The user will be tempted to skip these. Don't let them, and say why:

- **As-of discipline.** Every feature at a −N-day horizon must use only data available on that
  day. `sql/05_demand_percentiles.sql` and `sql/10_feature_view.sql` enforce it; `sql/10` ends
  with three tripwires. If comment volume is flat across horizons, it is broken.
- **A surprising aggregate deserves a check, not a celebration or an accusation.** MAPE is a mean
  over the whole film set; individual films landing almost exactly right is normal and is not a
  symptom of anything. But if the aggregate comes in well below the reference build (~38% on 122
  films), or jumps sharply after a feature-pipeline change rather than a model change, walk the
  leakage checklist before treating it as a gain. Do not tell the user their number is wrong —
  tell them what to check.
- **Measure the intent classifier.** `sql/30_intent_eval.sql` needs 150–300 hand labels and
  returns macro-F1. Without it, every downstream accuracy number has an unmeasured error bar.
  Watch PASS precision — it is the class that fails quietly and it is half of net intent.
- **Exclusions are a modeling decision.** Comments are disabled on many family/animated
  trailers; exclude on the absence of comments, never on the genre flag. Every
  `REMOVE_FROM_MODEL` row needs a reason.
- **Missing is not zero.** These features are percentiles; zero-filling a failed pull tells the
  model the film was the least-demanded in the set.

## Help the user research and choose each data source

The files in `sources/` describe each signal by its characteristics — how it behaves, how
it's accessed, its quirks, and the schema columns it feeds — rather than prescribing one
provider, because teams differ in access, budget, and terms. When the user asks "which
source fits this signal?", use those characteristics plus your own knowledge to suggest the
most likely public option(s), the access method, and the client library, then help them
evaluate and set up the one that fits — against their own credentials and each provider's
terms of service.

There are five signals:
- **Source A** — a normalized search-interest index; needs a stable id per title and
  potentially an anchor-term normalization to build a continuous baseline. Read its "known
  failure modes" section before recommending a client library — the obvious free one is
  currently broken, and the provider's docs will not tell you that. Check issue trackers.
- **Source B** — public trailer-comment threads, scored with Snowflake Cortex AISQL into
  sentiment and intent (theatrical / streaming / pass). Must carry `COMMENT_DATE`.
- **Source C** — an open entity-id based pageview data source, turned into demand percentiles.
- **Source D** — an industry box-office tracker (historical grosses + opening weekends).
- **Source E** — a movie-metadata source; static fields only, live popularity score is leakage.

## Hard rules (do not violate)

- Never write real API keys, tokens, account locators, or personal emails into repo files.
  Keys live in a git-ignored `.env` (template: `.env.example`).
- Respect each source's ToS, robots.txt, and rate limits. 
- Keep internal/customer-specific identifiers out of anything you generate. Use the
  templated names: `{{SANDBOX_DB}}.{{SCHEMA}}.<TABLE>`.

## Table naming convention (source-agnostic)

`MOVIE_MAP`, `RELEASE_DATES`, `BOX_OFFICE`, `TRAILER_COMMENTS_RAW`,
`TRAILER_COMMENTS_SCORED`, `SEARCH_INTEREST`, `SEARCH_ANCHOR_BASELINE`, `ENTITY_IDS`,
`PAGEVIEW_DEMAND`, `MOVIE_METADATA`, `INTENT_GOLD`, `REMOVE_FROM_MODEL`,
`DEMAND_PERCENTILES` (view, from `sql/05`), `OW_FEATURES` (view, from `sql/10`).
