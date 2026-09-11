---
name: research-pageviews
description: "Pull daily consumer-research pageviews for films and build demand percentiles in Snowflake. Use for: pageviews, research pageview demand, page-traffic, demand percentiles. Triggers: pageviews, page traffic, research demand, demand curve."
---

# Research Pageviews (Source C)

Pull daily pageview counts for each film's information/research page and turn them into
robust **demand percentiles**. Read `sources/source_C_research_pageviews.md` first and ask
CoCo to compare the available page-traffic APIs and pick one with daily granularity.

## Prerequisites
- No API key needed; set a descriptive `PAGEVIEW_CONTACT` in `.env` per provider etiquette.
- Sandbox tables: `PAGEVIEW_DEMAND`, `DEMAND_PERCENTILES` (from `sql/00_schema.sql`).

## Step 1 — Resolve the canonical page title
Titles need exact matching (spaces↔underscores, disambiguation like "(film)" or a year).
Resolve redirects to the canonical title before pulling.

## Step 2 — Pull daily views for the pre-release window
```python
import os, requests
CONTACT = os.environ.get("PAGEVIEW_CONTACT", "you@example.com")
HEADERS = {"User-Agent": f"trailer-signal-research/1.0 ({CONTACT})"}   # polite identification
def daily_views(article, start, end):
    url = "{{PAGEVIEW_API}}"          # CoCo fills the source-specific per-page daily endpoint
    r = requests.get(url.format(article=article, start=start, end=end), headers=HEADERS, timeout=15)
    return r.json()                   # -> [{date, views}, ...]
# Counts are ABSOLUTE (real views) — cache them; history doesn't change.
```
Load to `{{SANDBOX_DB}}.{{SCHEMA}}.PAGEVIEW_DEMAND`.

## Step 3 — Build demand percentiles by horizon
Align to days-out (−21/−14/−7/−3), then convert to percentiles across the film set to tame
the huge dynamic range: cumulative, 7-day rolling, peak-day, velocity. Write to
`DEMAND_PERCENTILES` (columns `PAGEVIEW_R7D_PCTILE`, `PAGEVIEW_PEAK_PCTILE`, `PAGEVIEW_CUM_PCTILE`, ...).

## Etiquette
- Send the contact string; keep request rates polite; cache aggressively.
