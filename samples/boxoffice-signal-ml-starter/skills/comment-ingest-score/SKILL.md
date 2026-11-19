---
name: comment-ingest-score
description: "Ingest public trailer comments and score them with Snowflake Cortex AISQL into sentiment + intent. Use for: trailer comments, comment ingestion, sentiment, intent classification, AISQL scoring, pseudonymize handles. Triggers: comments, trailer conversation, intent, sentiment, AISQL."
---

# Comment Ingest + Score (Source B)

Ingest public trailer-comment threads and turn them into the project's core signal:
**volume** + **decomposed intent**. Read `sources/source_B_trailer_comments.md` first and
ask CoCo to compare the platform options and choose one with an official API.

## Prerequisites
- `COMMENT_PLATFORM_API_KEY` in `.env` (preferred: official platform API).
- Sandbox tables: `TRAILER_COMMENTS_RAW`, `TRAILER_COMMENTS_SCORED` (from `sql/00_schema.sql`).
- Access to Cortex AISQL (docs/02).

## Step 1 — Ingest (prefer the official API)
```python
import os, hashlib
KEY = os.environ["COMMENT_PLATFORM_API_KEY"]
def pseudonymize(handle: str) -> str:
    return hashlib.sha256(handle.encode()).hexdigest()[:16]   # store hash, never raw handle
# Pull comments for a trailer video via {{COMMENT_PLATFORM}}'s official API, paginating.
# For each comment keep: video_id, comment_id, author_hash, text, like_count, date.
```
- **COMMENT_DATE is not optional.** It is what makes horizon-correct features possible. A
  comment with no date cannot be placed relative to the release and will either leak into
  early horizons or be dropped.
- If you must use a browser-DOM path instead of the API, respect ToS/robots/rate limits
  and still pseudonymize. Do not hammer the platform.
- Load rows to `{{SANDBOX_DB}}.{{SCHEMA}}.TRAILER_COMMENTS_RAW`.
- **Record films whose comments are disabled** in `REMOVE_FROM_MODEL` with reason
  `comments_disabled`. This hits many family/animated trailers. Exclude on the absence of
  comments, never on the genre flag — see `sources/source_B_trailer_comments.md`.

## Step 2 — Score with Cortex AISQL
Run `sql/20_intent_scoring_aisql.sql` (ask CoCo to refresh it to current AISQL syntax and a
good available model). It classifies each comment into a sentiment score/bucket and an
intent flag set: `THEATRICAL_INTENT`, `STREAMING_INTENT`, `PASS_INTENT`.

Why intent, not raw positivity: generic praise ("can't wait!!") is the *least* predictive
text, and planted hype only pushes one way. Classifying **intent** is what makes the signal
hard to game.

## Step 3 — Measure the classifier (do this before Step 4)
Run `sql/30_intent_eval.sql` against 150–300 hand-labeled comments in `INTENT_GOLD` and read
the macro-F1 and per-class confusion matrix. Watch **PASS precision** in particular: general
negativity ('this looks awful') getting scored as stated avoidance is the most common way net
intent gets quietly corrupted, and net intent is theatrical *minus* pass. In the reference
build the production scorer sat at macro-F1 0.65 with PASS precision 0.31, while a revised
single-call prompt reached 0.85 on the same labels.

## Step 4 — Per-film features (as-of the horizon)
```sql
-- Per-film totals are fine for a quick look, but the MODEL needs them per horizon.
-- sql/10_feature_view.sql does that; this is the eyeball version.
SELECT MOVIE_ID,
       COUNT(*)                                            AS comment_volume,
       AVG(SENTIMENT_SCORE)                                AS avg_sent,
       100.0*(AVG(THEATRICAL_INTENT) - AVG(PASS_INTENT))   AS net_intent_pct
FROM {{SANDBOX_DB}}.{{SCHEMA}}.TRAILER_COMMENTS_SCORED
GROUP BY MOVIE_ID;
```
Volume does most of the predictive work — though roughly half of its raw correlation with
opening weekend is budget and release scale, so control for those before believing the
headline number (`docs/06`). Net intent is weak alone but nearly independent of volume, so it
stacks additively.

