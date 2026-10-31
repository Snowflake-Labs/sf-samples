# Box-Office Signal ML Starter

A hands-on starter kit for reproducing a piece of applied research: **can the organic conversation around a movie trailer predict its opening weekend as well as industry trade data?**

This sample helps you stand up the *pipeline* yourself, inside your own Snowflake account, guided end-to-end by **Cortex Code (CoCo)** — Snowflake's agentic desktop IDE. It is a method kit, not a dataset: you bring your own data access, and the sample brings the schema, the AI scoring, the guardrails, the leakage tripwires, and a set of CoCo prompts that walk you from an empty sandbox to a research-ready feature set.

The techniques generalize past films. Anywhere you have public conversation ahead of a commercial outcome — game launches, product releases, ticketed events — the same pattern applies: classify intent inside the text with Cortex AISQL, build as-of features by horizon, and validate walk-forward in time.

## What it does

- Creates a **source-agnostic schema** for titles, releases, outcomes, trailer comments, search interest, and pageview demand.
- Scores raw comment text into **sentiment + viewing intent** (theatrical / streaming / pass) with **Snowflake Cortex AISQL** in a single consolidated call.
- Builds **as-of demand percentiles** at −21 / −14 / −7 / −3 day horizons, so no feature can see past its own prediction date.
- Assembles an **as-of feature view** (one row per title per horizon) with demand-gated pedigree interactions and no leakage features.
- Trains a **distributional regressor** (CatBoost + ElasticNet blend, residual-mixture prediction intervals, calibrated demand-forward flag) under **strict walk-forward validation**.
- Ships an **evaluation harness for the AI classifier itself** — macro-F1 and a per-class confusion matrix — because the intent classification *is* the method.

## Prerequisites

- **Snowflake account** with access to **Cortex AISQL** functions (`AI_SENTIMENT`, `SNOWFLAKE.CORTEX.TRY_COMPLETE`) and a warehouse you can use. An XS is plenty.
- A **sandbox database** you own, with rights to create schemas, tables, views, and functions inside it. `docs/02_request_a_sandbox.md` has a copy-paste request for your platform team.
- **Cortex Code (CoCo) Desktop**, connected to that account. See `docs/01_install_cortex_code.md`.
- **Python 3.9+** for the model (`pip install -r model/requirements.txt`).
- **Your own access to five public data sources.** None are included or endorsed here — `docs/03_identify_your_sources.md` walks you through choosing them with CoCo, and you are responsible for each provider's terms of service.

## How to run it

### 1. Verify your setup in ten minutes, before wiring up any data

Standing up five data sources takes real time. Don't spend it wondering whether your environment works. The model runs on synthetic data with no Snowflake connection at all:

```bash
pip install -r model/requirements.txt
python model/train_ow_model.py --sample
```

You should see a metric block print with MAPE around 40%. Those numbers describe a random-number generator, not films — don't quote them — but if that runs, the modeling machinery is fine and every later problem is a data problem.

### 2. Then follow the guided path

Open this directory in Cortex Code. `COCO.md` is loaded automatically and primes the agent to act as your setup guide.

| Step | Read | What happens |
|---|---|---|
| 1 | `docs/01_install_cortex_code.md` | Install CoCo Desktop, connect it to Snowflake |
| 2 | `docs/02_request_a_sandbox.md` | Get a sandbox database with full build rights |
| 3 | `docs/03_identify_your_sources.md` | Research and choose each data source with CoCo |
| 4 | `docs/04_stand_up_the_pipeline.md` | Create the schema, ingest and score each signal |
| 5 | `docs/05_api_keys_with_coco.md` | Obtain and store keys safely (never in the repo) |
| 6 | `docs/06_model_overview.md` | The method, what "good" looks like, and the traps |
| 7 | `docs/07_model_architecture.md` | The full model framework, with a runnable implementation |

SQL runs in this order:

```bash
snow sql -f sql/00_schema.sql              # schema (replace {{SANDBOX_DB}})
snow sql -f sql/20_intent_scoring_aisql.sql # Cortex AISQL sentiment + intent
snow sql -f sql/30_intent_eval.sql          # macro-F1 for your classifier
snow sql -f sql/05_demand_percentiles.sql   # as-of demand percentiles by horizon
snow sql -f sql/10_feature_view.sql         # modeling matrix + leakage tripwires
python model/train_ow_model.py --connection my_sandbox --database MY_SANDBOX_DB
```

## What's in here

```
COCO.md                          agent priming — read automatically by CoCo
docs/01..07                      the guided path, in order
sources/source_A..E              signal dossiers: characteristics, gotchas, known failure modes
skills/                          five installable CoCo skills (ingest, normalize, score, refresh)
prompts/coco_prompt_library.md   copy-paste CoCo prompts
sql/00_schema.sql                source-agnostic schema (run first)
sql/05_demand_percentiles.sql    as-of demand percentiles by horizon  <- the core transform
sql/10_feature_view.sql          the modeling matrix + leakage tripwires
sql/20_intent_scoring_aisql.sql  Cortex AISQL sentiment + intent scoring
sql/30_intent_eval.sql           macro-F1 for your intent classifier   <- don't skip this
model/train_ow_model.py          runnable reference model (--sample works with no data)
model/sample_data.py             synthetic feature generator
.env.example                     key names only; copy to .env, never commit
```

## Three things this sample insists on

**1. As-of discipline.** Comment threads and search curves keep growing right up to release. Every feature at a −21-day horizon must be computed from data available on that day. `sql/05` and `sql/10` enforce it, and `sql/10` ends with three tripwires that catch it when it breaks.

**2. Check a surprising result before you publish it.** The reference build lands around 38% MAPE on 122 films under strict forward validation. That is a reference point, not a law — a different film set, a longer history, or better features could legitimately do better. But leakage is by far the most common reason an aggregate error lands well below a comparable published baseline, so if yours does, run the checklist before you draw a conclusion from it. Individual predictions are a separate matter: a good model absolutely will nail some films almost exactly, and that is a property of the film, not a symptom.

**3. Measure the AI, not just the model.** Net intent is only as good as the classifier producing it. The reference build's production scorer turned out to sit at macro-F1 0.65 while a revised prompt reached 0.85 on the same hand labels — a gap invisible from output alone. `sql/30_intent_eval.sql` is how you find that out about your own.

## Data, terms, and privacy

**No data is included, and no provider is endorsed.** The `sources/` dossiers describe each signal by its *characteristics* — how it behaves, how it is typically accessed, its known failure modes — and CoCo helps you evaluate the options against your own access and budget. You are responsible for reading each provider's current terms of service and API or scraping policy, and for using official or licensed access where it is offered.

Two rules the sample enforces throughout: keys live in a git-ignored `.env` (template in `.env.example`) and never in a file you commit, and any user handles collected from public comment threads are **pseudonymized on ingest** — hashed, never stored raw.

## License

Apache 2.0, per this repository's [LICENSE](../../LICENSE). **No data is included or licensed** — data access and rights are entirely your responsibility.
