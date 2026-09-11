# 04 — Stand up the pipeline

By now you have CoCo connected (docs/01), a sandbox (docs/02), and identified sources
(docs/03). Time to build. Do it conversationally with CoCo — this page is the map.

## 0. Prove your setup works BEFORE you have any data

Standing up five data sources takes real time. Don't spend it not knowing whether your
environment is sane. The model runs on synthetic data with no Snowflake connection at all:

```bash
pip install -r model/requirements.txt
python model/train_ow_model.py --sample
```

You should see a metric block print with MAPE around 40%. Those numbers describe a
random-number generator, not films — do not quote them — but if that command works, the
modeling machinery is fine and every later problem is a data problem.

## 1. Create the schema
```bash
snow sql --connection my_sandbox -f sql/00_schema.sql   # after replacing {{SANDBOX_DB}}
```
Or ask CoCo: *"Run `sql/00_schema.sql` against my sandbox, substituting my database name."*

## 2. Seed the film spine
Populate `MOVIE_MAP` and `RELEASE_DATES` for your research set. **Validate every release
date against a second reference first** (Source D dossier) — a wrong date corrupts the
entire pre-release window.

## 3. Ingest each signal
Run the installed skills (see `skills/`), in this order:

| Order | Skill | Fills |
|---|---|---|
| 1 | `box-office-history` | `BOX_OFFICE`, `RELEASE_DATES` (label + validated dates) |
| 2 | `search-interest-normalize` | `ENTITY_IDS`, `SEARCH_INTEREST`, `SEARCH_ANCHOR_BASELINE` |
| 3 | `research-pageviews` | `PAGEVIEW_DEMAND` |
| 4 | `comment-ingest-score` | `TRAILER_COMMENTS_RAW` → `TRAILER_COMMENTS_SCORED` |
| 5 | (metadata, optional/static only) | `MOVIE_METADATA` |

Then record your exclusions in `REMOVE_FROM_MODEL`, with a reason on every row. The one
that catches everyone: **comments are disabled on a large share of family/animated
trailers**, so those films have no Source B signal at all. Exclude on the *absence of
comments*, never on the genre flag — a family film with a real comment thread belongs in
the set, and a non-family film with comments off does not. There is a copy-paste query for
this at the bottom of `sql/00_schema.sql`.

## 4. Score comments with Cortex AISQL
```bash
snow sql --connection my_sandbox -f sql/20_intent_scoring_aisql.sql
```
Ask CoCo to refresh it to the current AISQL syntax and a good available model first.

## 5. MEASURE the classifier before you trust it
```bash
snow sql --connection my_sandbox -f sql/30_intent_eval.sql
```
Hand-label 150–300 comments into `INTENT_GOLD` and get a macro-F1. This is not optional
polish — intent classification *is* the method, and an unmeasured classifier means every
downstream accuracy number has an unmeasured error bar. Watch PASS precision specifically;
it is the class that fails quietly, and it is half of net intent.

## 6. Build demand percentiles
```bash
snow sql --connection my_sandbox -f sql/05_demand_percentiles.sql
```
This creates `DEMAND_PERCENTILES` as an auto-computing view over Sources A + C at horizons
−21/−14/−7/−3 (rolling / peak / velocity / slope). It is the file that enforces as-of
discipline, so read its header and run its three validation queries. The `pipeline-refresh`
skill just re-runs this across the film set.

## 7. Assemble the feature view
```bash
snow sql --connection my_sandbox -f sql/10_feature_view.sql
```
Then sanity-check:
```sql
SELECT DAYS_OUT, COUNT(*) AS films, ROUND(AVG(YT_COMMENTS)) AS avg_comments,
       ROUND(AVG(NET_INTENT_PCT), 2) AS avg_net_intent
FROM {{SANDBOX_DB}}.RESEARCH.OW_FEATURES
GROUP BY 1 ORDER BY 1;
```
**`avg_comments` must rise from −21 to −3.** If it is flat, the as-of filter is not biting
(usually a NULL `COMMENT_DATE`) and your comment features are leaking release-week
conversation into three-weeks-out rows. Run all three leakage tripwires at the bottom of
`sql/10_feature_view.sql`.

## 8. Keep it fresh
Use the `pipeline-refresh` skill to update films in the active window (−21..+21 days) and to
fill opening weekends once films release.

## You're research-ready
You now have one modeling row **per film per horizon** (−21/−14/−7/−3) with as-of volume,
decomposed intent, and demand percentiles against a validated label. For a rigorous
modeling approach, see **`docs/06_model_overview.md`**.
