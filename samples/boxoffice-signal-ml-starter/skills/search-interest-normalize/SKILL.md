---
name: search-interest-normalize
description: "Pull and normalize a pre-release search/attention demand signal for films into Snowflake. Use for: search interest, attention data, demand index, normalization, continuous baseline. Triggers: search interest, attention, demand signal, normalize interest."
---

# Search-Interest Normalize (Source A)

Pull a **pre-release search/attention** signal for each film and normalize it into a
continuous, comparable demand timeline in Snowflake. Read `sources/source_A_search_interest.md`
first and ask CoCo to compare the available sources and pick one before you build.

## Prerequisites
- Access to your chosen interest source (`SEARCH_ENTITY_API_KEY` in `.env` if it needs a key;
  many client libraries are keyless but need a locale/timezone config).
- Sandbox tables from `sql/00_schema.sql`: `ENTITY_IDS`, `SEARCH_INTEREST`, `SEARCH_ANCHOR_BASELINE`.

## Step 1 — Disambiguate the title
Track the film, not unrelated searches that share its name. If your source offers a stable
topic/entity ID or exact-match mode, resolve each title to that ID first and store it in
`ENTITY_IDS`. Ask CoCo for the lookup that fits your source; keep any key in `.env`, never
hardcoded.

## Step 2 — Normalize for comparability
If your source returns a *relative* index (rescaled per request), separate pulls won't share
a scale. The general fix, which CoCo will tailor to your source:
- include a **stable, high-volume reference term** in each per-film pull, and
- separately pull that reference term on its own over a long window to build a continuous
  baseline (`SEARCH_ANCHOR_BASELINE`),
- then normalize each film's series against the reference so everything lands on one scale.

If your source returns absolute counts, you can skip the normalization and store directly.

## Step 3 — Load & derive features
Write results to `{{SANDBOX_DB}}.{{SCHEMA}}.SEARCH_INTEREST` (Title Case titles; never
UPPERCASE), then build rolling/peak/velocity demand percentiles by horizon (see
`pipeline-refresh`).

## Gotchas
- Expect rate-limiting; back off and retry.
- Validate a new film's pull against the source's own UI before trusting it.
- A wrong release date corrupts the ±window — validate dates first (Source D).
