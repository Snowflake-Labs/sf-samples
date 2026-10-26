# Snowflake-Native Geocoding

Forward and reverse geocoding built entirely inside Snowflake on the **Overture
Maps** Marketplace share — no external geocoding APIs. All objects live in the
`GEOCODING.PUBLIC` schema.

- **Forward** - free-text address -> GEOGRAPHY point. US by default; 14
  European countries with the optional `--eu` install (see below)
- **Reverse** - lat/lon -> nearest address (worldwide)
- **International parsing** (optional) - worldwide address parsing via libpostal on SPCS

## Repository layout

```
geocoding/
├── README.md                      ← you are here
├── sql/
│   ├── 00_setup.sql               database/schema + share sanity check
│   ├── udfs/
│   │   ├── parse_address.sql       usaddress parser (free text → fields)
│   │   ├── standardize_street.sql  230+ USPS/Nominatim abbreviation rules
│   │   └── normalize_street_eu.sql European street / place name keys
│   ├── eu/
│   │   └── eu_index.sql            BUILD_EU_INDEX: European candidate tables
│   ├── procedures/
│   │   ├── forward_geocode_table.sql   batch address → GEOGRAPHY
│   │   ├── reverse_geocode_table.sql   batch lat/lon → address
│   │   └── evaluate_geocode.sql        accuracy metrics (hit-rate, deviation)
│   └── examples/
│       ├── forward_geocode.sql     US smoke tests + procedure usage
│       ├── reverse_geocode.sql
│       ├── eu_test_set.sql         360-row European test set with noise
│       ├── eu_forward_geocode.sql  European demos + accuracy evaluation
│       ├── eu_benchmark.sql        10,000-row DE / FR runtime + accuracy
│       └── eu_reverse_geocode.sql  reverse check for DE / FR / IT
└── libpostal_service/             libpostal on SPCS (deployed by --intl / --eu)
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

# optional: also deploy the libpostal SPCS service for international parsing
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <your-snow-connection> --intl

# optional: European forward geocoding (libpostal service + EU index tables)
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <your-snow-connection> --eu [--eu-warehouse <large-wh>]
```

`--eu` does everything `--intl` does, then calls `BUILD_EU_INDEX`, which builds
the European candidate tables once. The first build reads about 140M addresses
and runs a spatial join, so pass an X-Large warehouse with `--eu-warehouse`.
Later runs find the tables and skip the build. Both flags need a running Docker
daemon or Podman machine to build the linux/amd64 libpostal image.

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
| `FORWARD_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, ADDRESS_COLUMN, ID_COLUMN, OUTPUT_TABLE, [COUNTRY='US'], [COUNTRY_COLUMN])` |
| `REVERSE_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, LAT_COLUMN, LON_COLUMN, ID_COLUMN, RADIUS_METERS, OUTPUT_TABLE, [COUNTRY])` |
| `EVALUATE_GEOCODE` | SQL proc | `(RESULT_TABLE, ACTUAL_GEOG_COLUMN, GEOCODED_GEOG_COLUMN, [THRESHOLD_METERS])`, plus a grouped overload `(..., THRESHOLD_METERS, GROUP_COLUMN)` |
| `NORMALIZE_STREET_EU`, `NORMALIZE_NAME_EU` | Python UDF | `(VARCHAR, VARCHAR) → VARCHAR`, `(VARCHAR) → VARCHAR` |
| `BUILD_EU_INDEX` | SQL proc | `([REBUILD=FALSE])` (`--eu` only) |
| `EU_LOCALITY`, `EU_LOCALITY_ALIAS`, `OVERTURE_ADDRESS_EU`, `OVERTURE_ADDRESS_EU_LOC` | Tables | European candidate index (`--eu` only) |
| `LIBPOSTAL_SVC`, `PARSE_ADDRESS_INTL`, `EXPAND_STREET_INTL` | SPCS service + functions | `--intl` / `--eu` only |

## How matching works

**Forward (US)** = parse (`usaddress`) + standardize street, then JOIN on
`COUNTRY='US'` AND exact `NUMBER` AND (`POSTCODE` match OR `POSTAL_CITY` match —
~38% of US rows have NULL city) AND (`STREET LIKE %standardized%` OR
`JAROWINKLER_SIMILARITY >= 92`), ranked by `JAROWINKLER_SIMILARITY` (normalized,
prefix-weighted) with `EDITDISTANCE` as a tiebreak. Remaining ties go to the
candidate whose postcode matches, then the lowest Overture ID, so repeated runs
return the same row.

**Forward (Europe)** is described in [European forward geocoding](#european-forward-geocoding).

**Reverse** = iterative expanding-radius search: start at 10 m and double each
pass (up to the `RADIUS_METERS` cap), only re-processing points not yet matched.
Dense-area points resolve in the first cheap pass; sparse points escalate to a
wider search. The input point is built once as a `GEOGRAPHY` with
`ST_POINT(lon, lat)`; within each pass `ST_DWITHIN` filters, `ST_DISTANCE` ranks,
and `ROW_NUMBER` keeps the closest address. Optional country filter. Outside the
US, `result_city` falls back to the municipality in Overture's `ADDRESS_LEVELS`,
because `POSTAL_CITY` is empty for most European countries.

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

US forward geocoding is **approximate** (~500–900 m), with a ~98% hit rate on
clean data (e.g. Austin permits). Coverage gaps exist. The first `PARSE_ADDRESS`
call is slow (~30 s) while the PyPI package installs; subsequent calls are fast.
For high-accuracy / routing use a partner solution (Carto / ESRI / Mapbox).
European accuracy is in the next section.

## European forward geocoding

Supported countries: FR, ES, PL, BE, DK, CH, AT, NL, NO, FI, PT, DE, IT, CZ.
The UK, Ireland and Sweden are not supported, because this Overture share has
no addresses for them. Rows for GB, UK or IE return `MATCH_LEVEL = 'NONE'`, and
UK support needs a different address dataset.

### Calling it

```sql
-- every row is German
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'MY_DB.PUBLIC.CUSTOMERS_DE', 'ADDRESS', 'ID',
    'MY_DB.PUBLIC.CUSTOMERS_DE_GEOCODED', 'DE');

-- mixed countries: an ISO-2 column per row; NULLs fall back to the COUNTRY argument
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'MY_DB.PUBLIC.CUSTOMERS', 'ADDRESS', 'ID',
    'MY_DB.PUBLIC.CUSTOMERS_GEOCODED', 'US', 'COUNTRY_CODE');
```

When every input row is US, the output is exactly what it was before: the same
9 columns and only matched rows. As soon as any row is non-US, every input row
is returned with three extra columns: `result_country`, `MATCH_LEVEL` (`ADDRESS`,
`STREET`, `POSTCODE`, `LOCALITY` or `NONE`) and `MATCH_SCORE` (0-100). At
`ADDRESS` level the score is the street similarity, `STREET` is 0.8 times that,
`POSTCODE` is 50, `LOCALITY` is 30 and `NONE` is 0. `result_geog` stays a
`GEOGRAPHY` point.

### Coverage

Measured on `OVERTURE_MAPS__ADDRESSES.CARTO.ADDRESS` and the `--eu` index.
"Missing" columns are the share of Overture rows with that field NULL. "In a
locality" is the share of addresses inside an Overture locality polygon, which
is what an input without a postcode is matched through.

| Country | Addresses | Missing postcode | Missing house number | Missing street | Missing postal city | In a locality | Candidate path |
|---|---|---|---|---|---|---|---|
| IT | 26.8M | 100% | 0.8% | 0% | 100% | 100% | locality |
| FR | 26.0M | 0% | 0% | 0% | 100% | 97.1% | postcode, locality |
| DE | 19.3M | 79.3% | 0% | 0% | 100% | 95.8% | locality, postcode where present |
| ES | 15.6M | 0% | 0.3% | 0% | 100% | 100% | postcode, locality |
| NL | 10.0M | 4.0% | 0% | 0% | 100% | 100% | postcode, locality |
| PL | 8.6M | 0% | 0% | 35.8% | 100% | 100% | postcode, locality |
| BE | 7.0M | 0% | 0% | 0% | 100% | 100% | postcode, locality |
| PT | 5.6M | 0% | 22.1% | 0% | 100% | 100% | postcode, locality |
| DK | 3.9M | 0% | 0% | 0% | 100% | 0.8% | postcode |
| FI | 3.7M | 0% | 4.6% | 0% | 100% | 99.9% | postcode, locality |
| NO | 3.6M | 0% | 0% | 0.8% | 0% | 0.5% | postcode |
| CH | 3.3M | 0% | 2.4% | 0% | 100% | 11.6% | postcode |
| CZ | 3.0M | 0% | 0% | 47.8% | 0% | 5.1% | postcode |
| AT | 2.5M | 0% | 0% | 0% | 100% | 85.9% | postcode, locality |
| GB, UK, IE, SE | 0 | - | - | - | - | - | not supported |

In total, 138.9M European addresses are indexed. Italy relies entirely on
locality polygons, and Germany mostly does. Denmark, Norway, Switzerland and the
Czech Republic have almost no Overture locality polygons, so an input there
needs a postcode to reach `ADDRESS` level; without one it can only return `NONE`.

### How it matches

Each distinct address is parsed once by libpostal (`PARSE_ADDRESS_INTL`), and
its street is expanded into variants (`EXPAND_STREET_INTL`), so "Hauptstr."
becomes "hauptstrasse" and "r. de la Poste" becomes "rue de la poste". The
variants and every Overture street are reduced to one comparison key by
`NORMALIZE_STREET_EU`. That function folds case and diacritics (ß to ss, ä to a
and so on), expands local abbreviations (Str., Av., V.le), drops prefixes such
as the Polish "ul.", and removes separators, so "Hauptstrasse", "Haupt
Strasse" and "Hauptstr." all produce the key `hauptstrasse`.

Candidates are only fetched by equi-join, never by a fuzzy comparison across the
whole table. In postcode countries the join is country, postcode and house
number. In Germany and Italy, where Overture has few or no postcodes, and for any
input without one, the join is country, locality and house number. The locality
comes from the parsed town (city, suburb or district label, or the text after
the last comma) matched to an Overture locality polygon. Bilingual names such
as "Pabillois/Pabillonis" match on either part. The point-in-polygon step
(`ST_INTERSECTS`) runs once, at index build time, so no spatial join happens per
query. Overture divisions have locality polygons but no postcode polygons; the
locality polygons cover about 96% of German and all sampled Italian addresses.

Candidates are ranked by Jaro-Winkler street similarity (85 or higher), then an
exact number and unit (3a before 3), then agreement with the input's locality
and postcode, then edit distance. Overture rows with no street, which are
common in Polish and Czech villages, can match on number plus postcode or
locality. An input whose street libpostal could not find is never matched to a
named street it did not mention. Without a house-number match the procedure
falls back to the best-matching street in the postcode or locality (centroid of
its addresses), then the postcode centroid, then the locality centroid.

### Accuracy

The test set (`sql/examples/eu_test_set.sql`) has 60 real Overture addresses
per country. Each is rendered in local format and then noised: one sixth each
is left clean, given local abbreviations (Str., r., V., C/, ul.), stripped of
the postcode, given a one-letter typo, lower-cased, or abbreviated and
lower-cased. Distances are measured to the source Overture point.

| Country | Rows | Hit rate | Within 100 m | Within 500 m | Avg deviation | ADDRESS | STREET | POSTCODE | LOCALITY | NONE |
|---|---|---|---|---|---|---|---|---|---|---|
| DE | 60 | 95.0% | 94.7% | 96.5% | 493 m | 54 | 1 | 2 | 0 | 3 |
| ES | 60 | 100% | 96.7% | 96.7% | 78 m | 60 | 0 | 0 | 0 | 0 |
| FR | 60 | 100% | 100% | 100% | 0 m | 60 | 0 | 0 | 0 | 0 |
| IT | 60 | 100% | 98.3% | 98.3% | 84 m | 58 | 1 | 0 | 1 | 0 |
| NL | 60 | 100% | 100% | 100% | 3 m | 59 | 0 | 1 | 0 | 0 |
| PL | 60 | 100% | 90.0% | 90.0% | 321 m | 58 | 0 | 2 | 0 | 0 |
| All | 360 | 99.2% | 96.6% | 96.9% | 161 m | 349 | 2 | 5 | 1 | 3 |

Within-distance percentages and averages are over matched rows. By match level,
`ADDRESS` results are 98.3% within 100 m (avg 60 m), `STREET` 50% within 100 m
and 100% within 500 m, and `POSTCODE` and `LOCALITY` results are area centroids,
kilometres off by nature. By noise type, lower-case input scored 100% within
100 m, abbreviations and missing postcodes 96.7%, clean 95.0% and typos 90.0%.

The three German misses have no postcode and no town in the text, because the
source address has no postcode in Overture and lies outside every locality
polygon. The three far Polish `ADDRESS` results are real ambiguity: the same
number, street, postcode and locality polygon exist in a neighbouring village a
few kilometres away, and Overture holds no village name to separate them.

On larger clean batches (`sql/examples/eu_benchmark.sql`, 10,000 rows each), DE
has a 96.7% hit rate with 95.6% within 100 m, and FR has 99.8% with 98.8%
within 100 m. All 329 German misses are street-and-number-only renderings, for
the reason above.

### Cost

10,000-row batch, end to end, libpostal service already running:

| Country | X-Small | Medium |
|---|---|---|
| DE | 40 s | 34 s |
| FR | 36 s | 30 s |

Libpostal parsing and expansion take about 3 s per batch. The candidate join
is the largest step on X-Small (8-10 s) and drops to about 4.5 s on Medium; the
rest is many short statements, so a bigger warehouse helps little. The query
profile shows only hash equi-joins, no cartesian join, and join-filter pruning
that reads at most 101 of 527 partitions of the index tables.

The European index uses about 18.7 GB of storage: `OVERTURE_ADDRESS_EU` (139M
rows, 8.1 GB, clustered by country and postcode), `OVERTURE_ADDRESS_EU_LOC`
(148M rows, 8.4 GB, clustered by country and locality), `EU_LOCALITY` (121K
polygons, 2.2 GB) and `EU_LOCALITY_ALIAS` (under 0.01 GB). Rebuild it with
`CALL GEOCODING.PUBLIC.BUILD_EU_INDEX(TRUE)` after the Overture share refreshes.
The libpostal service runs on one `CPU_X64_S` compute pool node, billed while
the pool is active. Suspend it when you are not geocoding:

```sql
ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC SUSPEND;
ALTER COMPUTE POOL GEOCODING_LIBPOSTAL_POOL SUSPEND;
-- and RESUME both before the next European batch
```

### Known limitations

- **UK, Ireland, Sweden**: no addresses in this share. Rows return `NONE`, and
  UK support needs another dataset.
- **Czech Republic**: 47.8% of Overture rows have no street, so many matches
  rely on number plus postcode, and street-level fallback is weaker.
- **Poland**: 35.8% of rows have no street, and one postcode often covers several
  villages with the same street names, as described above.
- **Portugal** (22.1%), **Finland** (4.6%), **Switzerland** (2.4%) and **Italy**
  (0.8%) rows are missing house numbers, so those addresses can only resolve at
  street level or coarser.
- **Germany and Italy** have few or no postcodes in Overture, so an input with
  neither a postcode nor a recognizable town cannot be matched.
- **Denmark, Norway, Switzerland, Czech Republic** have almost no locality
  polygons in Overture divisions, so inputs there need a postcode.
- `POSTAL_CITY` is empty in Overture for most of Europe. `result_city` for
  European forward results is the matched locality polygon name.
- Only the 14 countries above are routed to the European path. Other non-US
  countries return `NONE`.

## International geocoding

For non-US / multi-language addresses, see `libpostal_service/`, a libpostal
container on SPCS exposed as service functions (`PARSE_ADDRESS_INTL`,
`EXPAND_STREET_INTL`). The installer's `--intl` or `--eu` flag builds the image,
pushes it to your account's repository and creates the service and functions.
Re-running it picks up a rebuilt image.
