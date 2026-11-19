---
name: box-office-history
description: "Obtain box-office history (opening weekend + release dates) via licensed/official access and validate it. Use for: box office, opening weekend, the label, release dates, target variable. Triggers: box office, opening weekend, label, release date validation."
---

# Box-Office History (Source D — the label)

Obtain the **target** (domestic opening weekend), plus theater count and validated release
dates. Read `sources/source_D_box_office_history.md` first.

Confirm the provider's current Terms of Service before any programmatic access.

## Prerequisites
- Sandbox tables: `BOX_OFFICE`, `RELEASE_DATES` (from `sql/00_schema.sql`).

## Load
Write per film: `OPENING_WEEKEND` (dollars), `THEATER_COUNT`, `MOVIE_TITLE` → `BOX_OFFICE`;
`RELEASE_DATE` → `RELEASE_DATES`.

## Validation discipline (this bit earlier versions)
- **Validate release dates against a second reference** before pulling any pre-release
  signal — a wrong date silently corrupts the days-out window and the release-month feature.
- **Verify opening-weekend figures independently** after release. A prior audit of a
  training set found fabricated OW values and corrupted dates. Never trust a fresh value
  from a single source.

```sql
-- Flag suspicious rows for manual review
SELECT b.MOVIE_ID, b.MOVIE_TITLE, b.OPENING_WEEKEND, r.RELEASE_DATE
FROM {{SANDBOX_DB}}.{{SCHEMA}}.BOX_OFFICE b
JOIN {{SANDBOX_DB}}.{{SCHEMA}}.RELEASE_DATES r USING (MOVIE_ID)
WHERE b.OPENING_WEEKEND <= 0
   OR b.THEATER_COUNT < 1000
   OR r.RELEASE_DATE IS NULL;
```

