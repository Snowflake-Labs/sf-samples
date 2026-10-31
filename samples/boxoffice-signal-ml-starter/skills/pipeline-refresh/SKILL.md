---
name: pipeline-refresh
description: "Orchestrate an active-window refresh of all trailer-signal sources for upcoming films. Use for: refresh pipeline, update data, active window, which films need data, pre-release refresh. Triggers: refresh, update movies, active window, data gaps, pipeline refresh."
---

# Pipeline Refresh (all sources)

Find films in the active release window (−21 to +21 days from today) and refresh every
signal for them, then update the label for films that have opened. Orchestrates the other
four skills.

## Prerequisites
- Sandbox schema created (`sql/00_schema.sql`) and the four ingest skills installed.
- Sources identified via CoCo (docs/03).

## Step 0 — Find active films and their gaps
```sql
SELECT m.MOVIE_ID, m.MOVIE_TITLE, rd.RELEASE_DATE,
       DATEDIFF('day', CURRENT_DATE(), rd.RELEASE_DATE)         AS DAYS_UNTIL_RELEASE,
       IFF(e.ENTITY_ID IS NULL, 'MISSING', e.ENTITY_ID)         AS ENTITY_STATUS,
       (SELECT MAX(OBS_DATE) FROM {{SANDBOX_DB}}.{{SCHEMA}}.SEARCH_INTEREST s
         WHERE s.MOVIE_ID = m.MOVIE_ID)                          AS SEARCH_MAX_DATE,
       (SELECT COUNT(*) FROM {{SANDBOX_DB}}.{{SCHEMA}}.TRAILER_COMMENTS_SCORED c
         WHERE c.MOVIE_ID = m.MOVIE_ID)                          AS COMMENTS_SCORED,
       (SELECT MAX(OBS_DATE) FROM {{SANDBOX_DB}}.{{SCHEMA}}.PAGEVIEW_DEMAND p
         WHERE p.MOVIE_ID = m.MOVIE_ID)                          AS PAGEVIEW_MAX_DATE
FROM {{SANDBOX_DB}}.{{SCHEMA}}.MOVIE_MAP m
JOIN {{SANDBOX_DB}}.{{SCHEMA}}.RELEASE_DATES rd USING (MOVIE_ID)
LEFT JOIN {{SANDBOX_DB}}.{{SCHEMA}}.ENTITY_IDS e USING (MOVIE_ID)
WHERE m.MOVIE_ID NOT IN (SELECT MOVIE_ID FROM {{SANDBOX_DB}}.{{SCHEMA}}.REMOVE_FROM_MODEL)
  AND rd.RELEASE_DATE BETWEEN DATEADD('day',-21,CURRENT_DATE()) AND DATEADD('day',21,CURRENT_DATE());
```

## Step 1 — Refresh, per film (order matters)
1. **Validate release date** against a second reference (Source D) — do this first.
2. `search-interest-normalize` — refresh Source A (entity ID must exist).
3. `research-pageviews` — refresh Source C.
4. `comment-ingest-score` — refresh + score Source B.
5. Rebuild `DEMAND_PERCENTILES` by re-running `sql/05_demand_percentiles.sql` (it is a view, so
   this is only needed if you changed the definition — otherwise it recomputes on read).
6. For films now past release: `box-office-history` — fill/verify `OPENING_WEEKEND`.
7. Add any newly-discovered exclusions to `REMOVE_FROM_MODEL` with a reason — most commonly
   `comments_disabled` for trailers with the comment section switched off.

## Step 2 — Rebuild features and check for leaks
Refresh the `OW_FEATURES` view (`sql/10_feature_view.sql`) and spot-check a few films. Then run
the tripwire at the bottom of that file:

```sql
SELECT DAYS_OUT, COUNT(*) AS films, ROUND(AVG(YT_COMMENTS)) AS avg_comments
FROM {{SANDBOX_DB}}.{{SCHEMA}}.OW_FEATURES GROUP BY 1 ORDER BY 1;
```
`avg_comments` must rise from −21 to −3. Flat means the as-of filter is not biting — usually a
NULL `COMMENT_DATE` — and your backtest is being fed release-week conversation.

Refreshing is exactly when as-of discipline breaks, because you are adding recent data to films
whose early horizons are already fixed. A film's −21 percentile should not move because you
pulled new data today. `sql/05` has an as-of proof query for this.

## Conventions
- Titles are Title Case everywhere; never insert UPPERCASE.
- Tell the user what you'll do before doing it.
- Keep raw pulls out of git; pseudonymize comment handles.
