# Source D — Box-office results (the label)

## The signal
The target the model predicts: **domestic opening weekend**, plus historical grosses,
theater counts (to filter wide releases), and accurate release dates.

## Options CoCo can help you choose from
Box-office results are published by several industry trackers and are available as licensed
datasets and, in some cases, official APIs. Ask CoCo for the options and their access terms,
and pick the one that fits. A practical note: some popular trackers **restrict automated
access**, so prefer an **official or licensed** feed, a properly licensed dataset, or manual
curation of the handful of figures your research set needs. Confirm a source's current Terms
of Service before any programmatic access.

## Data-quality discipline (don't skip this)
- **Validate release dates against a second reference.** A wrong date silently corrupts the
  entire pre-release feature window and the release-month feature.
- **Verify opening-weekend figures independently.** Never trust a single fresh value — a
  past audit of a training set turned up fabricated grosses and corrupted dates. Cross-check
  before you train on anything.

```sql
-- Flag suspicious rows for manual review
SELECT b.MOVIE_ID, b.MOVIE_TITLE, b.OPENING_WEEKEND, r.RELEASE_DATE
FROM {{SANDBOX_DB}}.{{SCHEMA}}.BOX_OFFICE b
JOIN {{SANDBOX_DB}}.{{SCHEMA}}.RELEASE_DATES r USING (MOVIE_ID)
WHERE b.OPENING_WEEKEND <= 0 OR b.THEATER_COUNT < 1000 OR r.RELEASE_DATE IS NULL;
```

## Feeds these columns
- `BOX_OFFICE` (`OPENING_WEEKEND`, `THEATER_COUNT`, `MOVIE_TITLE`)
- `RELEASE_DATES` (`RELEASE_DATE`, validated)

## Ask CoCo
> "Read `sources/source_D_box_office_history.md`. What are my options for domestic
> opening-weekend data, and which offer official or licensed access? Help me set one up and
> build a release-date + opening-weekend validation check against a second reference."
