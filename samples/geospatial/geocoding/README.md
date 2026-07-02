# Snowflake-Native Geocoding

Forward and reverse geocoding built entirely inside Snowflake on the **Overture
Maps** Marketplace share — no external geocoding APIs. All objects live in the
`GEOCODING.PUBLIC` schema.

- **Forward** — free-text US address → lat/lon
- **Reverse** — lat/lon → nearest address (worldwide)
- **International** (optional) — worldwide address parsing via libpostal on SPCS

## Repository layout

```
geocoding/
├── README.md                      ← you are here
├── sql/                           US geocoding — deployed & working
│   ├── 00_setup.sql               database/schema + share sanity check
│   ├── udfs/
│   │   ├── parse_address.sql       usaddress parser (free text → fields)
│   │   └── standardize_street.sql  230+ USPS/Nominatim abbreviation rules
│   ├── procedures/
│   │   ├── forward_geocode_table.sql   batch address → lat/lon
│   │   ├── reverse_geocode_table.sql   batch lat/lon → address
│   │   └── evaluate_geocode.sql        optional accuracy metrics (hit-rate, deviation)
│   └── examples/
│       ├── forward_geocode.sql     smoke tests + procedure usage
│       └── reverse_geocode.sql
└── libpostal_service/             International geocoding — SPCS (template, not yet deployed)
    ├── server.py  Dockerfile  requirements.txt  service-spec.yaml
    ├── deploy.sql
    └── README.md
```

## Data source

Overture Maps "Addresses" (Carto), imported from the Marketplace as:

```
OVERTURE_MAPS__ADDRESSES.CARTO.ADDRESS
```

Columns: `ID, STREET, NUMBER, UNIT, POSTCODE, POSTAL_CITY, COUNTRY, GEOMETRY (GEOGRAPHY)`.

> The installer **auto-acquires** this listing (free, `GZT0Z4CM1E9NQ`) — no
> manual Marketplace step. If a copy already exists under either
> `OVERTURE_MAPS__ADDRESSES` or the `DS_`-prefixed name, it is reused. A stable
> view `GEOCODING.PUBLIC.OVERTURE_ADDRESS` is created over whichever share
> resolves, so you never edit the physical share name. No external access
> integration is required by the solution.

## Install with Cortex Code (recommended)

This repo ships a project-local **Cortex Code skill**, `install-geocoding`, that
deploys the whole solution with one command. Open this folder in Cortex Code and
say **"install geocoding"**, or run the orchestrator directly:

```bash
# core: US forward + worldwide reverse geocoding
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <your-snow-connection> [--warehouse <wh>]

# optional: also deploy the libpostal SPCS service for international addresses
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <your-snow-connection> --intl
```

The installer is idempotent (re-runnable), auto-acquires the Overture listing,
and tags every object with `COMMENT = 'oss-geocoding'`. See
[.cortex/skills/install-geocoding/SKILL.md](.cortex/skills/install-geocoding/SKILL.md).

## Quickstart (manual, US geocoding)

Run in order:

```sql
-- 1. Setup + deploy the building blocks
!source sql/00_setup.sql
!source sql/udfs/parse_address.sql
!source sql/udfs/standardize_street.sql
!source sql/procedures/forward_geocode_table.sql
!source sql/procedures/reverse_geocode_table.sql
!source sql/procedures/evaluate_geocode.sql

-- 2. Try it (see sql/examples/ for full versions)
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'MY_DB.PUBLIC.PERMITS', 'ADDRESS_TEXT', 'PERMIT_ID',
    'MY_DB.PUBLIC.PERMITS_GEOCODED');

CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE(
    'MY_DB.PUBLIC.PINGS', 'LAT', 'LON', 'DEVICE_ID', 200,
    'MY_DB.PUBLIC.PINGS_ADDRESSED', 'US');
```

`!source` is Snowflake CLI syntax; in a worksheet/notebook just paste each file's contents.

## Deployed objects (`GEOCODING.PUBLIC`)

| Object | Type | Signature |
|--------|------|-----------|
| `PARSE_ADDRESS` | Python UDF | `(VARCHAR) → OBJECT` |
| `STANDARDIZE_STREET` | Python UDF | `(VARCHAR) → VARCHAR` |
| `FORWARD_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, ADDRESS_COLUMN, ID_COLUMN, OUTPUT_TABLE)` |
| `REVERSE_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, LAT_COLUMN, LON_COLUMN, ID_COLUMN, RADIUS_METERS, OUTPUT_TABLE, [COUNTRY])` |
| `EVALUATE_GEOCODE` | SQL proc | `(RESULT_TABLE, ACTUAL_GEOG_COLUMN, GEOCODED_GEOG_COLUMN, [THRESHOLD_METERS])` |

## How matching works

**Forward** = parse (`usaddress`) + standardize street, then JOIN on
`COUNTRY='US'` AND exact `NUMBER` AND (`POSTCODE` match OR `POSTAL_CITY` match —
~38% of US rows have NULL city) AND (`STREET LIKE %standardized%` OR
`JAROWINKLER_SIMILARITY >= 92`), ranked by `JAROWINKLER_SIMILARITY` (normalized,
prefix-weighted) with `EDITDISTANCE` as a tiebreak.

**Reverse** = iterative expanding-radius search: start at 10 m and double each
pass (up to the `RADIUS_METERS` cap), only re-processing points not yet matched.
Dense-area points resolve in the first cheap pass; sparse points escalate to a
wider search. The input point is built once as a `GEOGRAPHY` with
`ST_POINT(lon, lat)`; within each pass `ST_DWITHIN` filters, `ST_DISTANCE` ranks,
and `ROW_NUMBER` keeps the closest address. Optional country filter.

**Evaluate** (optional) = `EVALUATE_GEOCODE` compares actual vs geocoded points
with `ST_DISTANCE` and returns match rate, % within a distance threshold, and
average deviation.

## GEOGRAPHY is first class

Location is represented as Snowflake `GEOGRAPHY` end to end — never flattened to
FLOAT lat/lon. Both procedures emit a `GEOGRAPHY` column:

- `FORWARD_GEOCODE_TABLE` -> `result_geog` (the matched Overture point).
- `REVERSE_GEOCODE_TABLE` -> `input_geog` (the query point, built with
  `ST_POINT`) and `result_geog` (the nearest Overture point).

These columns drop straight into any spatial function or map layer, and feed
`EVALUATE_GEOCODE` with no wrapping. Recover coordinates with
`ST_X(geog)` (longitude) / `ST_Y(geog)` (latitude) when you need them.

## Accuracy & limits

US-only (for the SQL path), **approximate** (~500–900 m), ~98% hit rate on
clean data (e.g. Austin permits). Coverage gaps exist. First `PARSE_ADDRESS`
call is slow (~30 s) while the PyPI package installs; subsequent calls are fast.
For high-accuracy / routing use a partner solution (Carto / ESRI / Mapbox).

## International geocoding

For non-US / multi-language addresses, see `libpostal_service/` — a libpostal
container on SPCS exposed as service functions (`PARSE_ADDRESS_INTL`,
`EXPAND_STREET_INTL`). The container image is already built and pushed; run
`libpostal_service/deploy.sql` (steps 2–4) to create the service and functions.
