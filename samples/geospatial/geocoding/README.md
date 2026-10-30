# Snowflake-Native Geocoding

Forward and reverse geocoding built entirely inside Snowflake on the **Overture
Maps** Marketplace share — no external geocoding APIs. All objects live in the
`GEOCODING.PUBLIC` schema.

- **Forward** - free-text address -> GEOGRAPHY point. US plus 14
  European countries
- **Reverse** - lat/lon -> nearest address (worldwide)
- **Tester app** - `GEOCODING_TESTER`, a Snowflake App Runtime app to try forward,
  reverse and normalization with results on a map
- **International parsing** - worldwide address parsing via libpostal on SPCS

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
├── geocoding_tester/              Snowflake App Runtime tester (Next.js + deck.gl)
└── libpostal_service/             libpostal on SPCS (deployed by default; --no-intl skips)
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
> resolves, so you never edit the physical share name. Geocoding itself needs
> no external access integration; only the tester app uses one, to proxy its
> CARTO basemap.

## Install with Cortex Code (recommended)

This repo ships a project-local **Cortex Code skill**, `install-geocoding`, that
deploys the whole solution with one command. Open this folder in Cortex Code and
say **"install geocoding"**, or run the orchestrator directly:

```bash
# full install: core + libpostal SPCS + EU index + tester app
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <your-snow-connection> [--warehouse <wh>] --eu-warehouse GEOCODING_BUILD_WH

# US forward + worldwide reverse only (no Docker needed)
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <your-snow-connection> --no-intl
```

Opt-outs: `--no-intl` (skips libpostal and the EU index), `--no-eu` (keeps
libpostal, skips the index), `--no-app` (skips the tester app). The old `--intl`
and `--eu` flags are still accepted.

The EU step calls `BUILD_EU_INDEX`, which builds the European candidate tables
once. The first build reads about 140M addresses and runs a spatial join, so pass
an X-Large warehouse with `--eu-warehouse`. If you have none, create one first:
`CREATE WAREHOUSE IF NOT EXISTS GEOCODING_BUILD_WH WAREHOUSE_SIZE = XLARGE
AUTO_SUSPEND = 60 INITIALLY_SUSPENDED = TRUE COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';`
Later runs find the tables and skip the build. The libpostal step needs a running
Docker daemon or Podman machine to build the linux/amd64 image.

The installer is idempotent (re-runnable), auto-acquires the Overture listing,
and tags every object with a JSON tracking `COMMENT` (see [Tracking](#tracking)). See
[.cortex/skills/install-geocoding/SKILL.md](.cortex/skills/install-geocoding/SKILL.md).

Around the install, in `.cortex/skills/install-geocoding/scripts/`:

| Script | What it does |
|--------|--------------|
| `preflight.sh` | Read-only check of the CLI, Docker, privileges, SPCS and listings. Exit 0 ready, 1 blocking, 2 degraded. The installer runs it first (`--skip-preflight`). |
| `verify_geocoding.sh` | Read-only check that US, reverse, libpostal and EU geocoding return real matches and the tester is up. The installer runs it last (`--skip-verify`). |
| `uninstall_geocoding.sh` | Removes what the installer created, in a safe order. Dry run unless `--yes`. |

Each run writes a receipt with per-step status and timings to
`logs/install_<account>_<timestamp>.md`, also when a step fails. The libpostal
image is tagged from `libpostal_service/VERSION`; a re-run skips the build and
push when that tag is already in the registry (`--rebuild-image` forces it).

To give another role access, add `--grant-role <ROLE>`. It grants USAGE on the
functions and procedures, the libpostal endpoint and the tester app.
`references/cost-guardrails.sql` has an opt-in resource monitor and budget.
If something fails, see `references/troubleshooting.md`.

### Pause or remove

The libpostal service and its compute pool are billed while they run. The
service suspends itself after 30 minutes without calls (`AUTO_SUSPEND_SECS =
1800`), and the pool suspends 10 minutes after that. The next
`PARSE_ADDRESS_INTL` call, or a European forward geocode, wakes both: about
16 s if only the service was suspended and about 100 s if the pool was too.
The function tester has Start and Stop buttons for this. To pause by hand:

```sql
ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC SUSPEND;
ALTER COMPUTE POOL GEOCODING_LIBPOSTAL_POOL SUSPEND;
```

Re-running the installer resumes both. To free the
about 19.7 GB of EU index storage but keep US and reverse geocoding:

```sql
DROP TABLE IF EXISTS GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU_LOC;
DROP TABLE IF EXISTS GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU;
DROP TABLE IF EXISTS GEOCODING.PUBLIC.EU_LOCALITY_ALIAS;
DROP TABLE IF EXISTS GEOCODING.PUBLIC.EU_LOCALITY;
```

To remove everything the installer created (service, compute pool, image
repository, functions, procedures and index tables), run the uninstall script.
It shows the plan first and only drops with `--yes`:

```bash
bash .cortex/skills/install-geocoding/scripts/uninstall_geocoding.sh --connection <conn>        # dry run
bash .cortex/skills/install-geocoding/scripts/uninstall_geocoding.sh --connection <conn> --yes
```

`--keep-eu-index` keeps the database, `--drop-shares` also drops the Overture
Marketplace databases, and `--keep-warehouses` keeps the build warehouses. The
equivalent SQL:

```sql
DROP APPLICATION SERVICE IF EXISTS GEOCODING.PUBLIC.GEOCODING_TESTER;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS GEOCODING_TESTER_CARTO_EAI;
DROP SERVICE IF EXISTS GEOCODING.PUBLIC.LIBPOSTAL_SVC;
DROP COMPUTE POOL IF EXISTS GEOCODING_LIBPOSTAL_POOL;
DROP DATABASE IF EXISTS GEOCODING;
-- Optional, only if nothing else uses them:
-- DROP DATABASE IF EXISTS OVERTURE_MAPS__ADDRESSES;
-- DROP DATABASE IF EXISTS OVERTURE_MAPS__DIVISIONS;
-- DROP WAREHOUSE IF EXISTS GEOCODING_BUILD_WH;
-- DROP WAREHOUSE IF EXISTS GEOCODING_XS_WH;
```

The compute pool can only be dropped once the service is gone, so keep that
order.

## Function tester

The installer finishes by printing the tester link:

```
Function tester: https://<id>-<org>-<account>.snowflakecomputing.app
```

To print it again: `cd geocoding_tester && snow app open -c <connection> --print-only`.

`GEOCODING_TESTER` is a small Snowflake App Runtime app (Next.js, deck.gl and
MapLibre) in `GEOCODING.PUBLIC`. It has three tabs, and every result is drawn on a map:

- **Forward geocode**: one address per line, up to 200. A country dropdown
  sets the default. Prefix a line with `CC |` (for example
  `DE | Unter den Linden 77, 10117 Berlin`) to mix countries in one batch.
  Points are colored by `MATCH_LEVEL`.
- **Reverse geocode**: one `lat, lon` per line, up to 200. Clicking the map
  adds a point. Each input is linked to its nearest address, and search radii
  are drawn for batches of 25 points or fewer. "Any country" searches worldwide.
- **Normalize address**: runs `PARSE_ADDRESS`, `STANDARDIZE_STREET`,
  `PARSE_ADDRESS_INTL`, `EXPAND_STREET_INTL` and `NORMALIZE_STREET_EU` on each
  line (up to 200, `CC |` prefix as on Forward), then geocodes them onto the
  map. One address shows the functions side by side; several show a table, and
  clicking a row shows that record's full output.

A **Sample** dropdown on each tab loads a test sample, grouped into single
records and multiple records:

| Tab | Multiple-record samples |
|-----|-------------------------|
| Forward | 10 US, 10 DE, 10 FR, 20 across six countries, 50 across six countries |
| Reverse | 10 Berlin, 10 Paris, 20 US, 5 with "Any country" |
| Normalize | 12 across six countries (abbreviations such as Str., bd, C/, Pza), 10 US unit and suffix variants |

On TIB, the 50-address forward sample took 57.5 s, with all 50 at `ADDRESS`
level. The 12-address normalize sample took 10.7 s, all 12 geocoded. A loaded
sample can be edited before Run; the dropdown then shows "Custom". Under the
results, collapsible panels with copy buttons show the exact **request**
JSON, the raw **response** JSON and the **SQL** that ran.

A **libpostal** bar above the tabs shows whether the SPCS service is
running, starting or suspended, with its pool size and idle timer. **Start**
resumes the pool and the service ahead of a European or normalize call, which
avoids the 16-100 s cold start. **Stop** suspends both after you confirm.
When a run needs libpostal and it is suspended, the tester says so before you
run.

The app runs with owner's rights. It sends user input only as bind
variables, and keeps per-request input and output tables in
`GEOCODING.TESTER`, dropping them after each call. App Runtime pages may only
connect to the app's own origin, so the CARTO basemap is fetched through the
app's `/api/basemap` proxy. The proxy only accepts CARTO hosts and uses the
`GEOCODING_TESTER_CARTO_EAI` integration. To redeploy after editing:
`bash geocoding_tester/deploy.sh --connection <connection>`. Deploying needs
a Snowflake CLI with `snow app` (3.26+).

## Batch processing

Every procedure is table in, table out, so batches are the normal path. The
calls below are what the tester runs, scaled up. Timings are in
[Benchmarks](#benchmarks).

```sql
-- Forward, mixed countries: one row per address with an ISO-2 country column.
-- Rows with a NULL country use the 5th argument.
CREATE OR REPLACE TRANSIENT TABLE MY_DB.PUBLIC.CUSTOMERS AS
SELECT * FROM VALUES
  (1, '6310 Milford Rd, Fayetteville NC 28303', 'US'),
  (2, 'Unter den Linden 77, 10117 Berlin',      'DE'),
  (3, '10 Rue de Rivoli, 75001 Paris',          'FR'),
  (4, 'Schiekade 568, 3032AZ Rotterdam',        'NL'),
  (5, 'Calle Moratin 6, 28014 Madrid',          'ES')
  AS t(ID, ADDRESS, COUNTRY);

CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'MY_DB.PUBLIC.CUSTOMERS', 'ADDRESS', 'ID',
    'MY_DB.PUBLIC.CUSTOMERS_GEOCODED', 'US', 'COUNTRY');

SELECT INPUT_ID, RAW_ADDRESS, RESULT_COUNTRY, MATCH_LEVEL, MATCH_SCORE,
       ST_Y(RESULT_GEOG) AS LAT, ST_X(RESULT_GEOG) AS LON
FROM MY_DB.PUBLIC.CUSTOMERS_GEOCODED ORDER BY INPUT_ID;

-- Reverse: a table of points -> nearest address within 200 m (country optional).
CREATE OR REPLACE TRANSIENT TABLE MY_DB.PUBLIC.PINGS AS
SELECT * FROM VALUES
  ('a', 40.7484, -73.9857), ('b', 48.8606, 2.3376), ('c', 52.5163, 13.3777)
  AS t(DEVICE_ID, LAT, LON);

CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE(
    'MY_DB.PUBLIC.PINGS', 'LAT', 'LON', 'DEVICE_ID', 200,
    'MY_DB.PUBLIC.PINGS_ADDRESSED');

-- Normalization UDFs are scalar, so they batch like any SELECT.
SELECT ADDRESS,
       GEOCODING.PUBLIC.PARSE_ADDRESS(ADDRESS)      AS us_parse,
       GEOCODING.PUBLIC.PARSE_ADDRESS_INTL(ADDRESS) AS intl_parse,
       GEOCODING.PUBLIC.NORMALIZE_STREET_EU(
           GEOCODING.PUBLIC.PARSE_ADDRESS_INTL(ADDRESS):road::STRING, COUNTRY) AS street_key
FROM MY_DB.PUBLIC.CUSTOMERS;

-- Accuracy against known coordinates (hit rate, % within 100 / 500 m, average
-- distance). Needs a known-truth GEOGRAPHY column (here ACTUAL_GEOG) on the
-- output, e.g. joined back from your source table.
CALL GEOCODING.PUBLIC.EVALUATE_GEOCODE(
    'MY_DB.PUBLIC.CUSTOMERS_GEOCODED', 'ACTUAL_GEOG', 'RESULT_GEOG', 200);
```

Batches of 10,000 rows or more should run on a dedicated warehouse; the size
trade-off is in [Benchmarks](#benchmarks). `PARSE_ADDRESS_INTL` and
`EXPAND_STREET_INTL` send rows to the libpostal service in batches of 500
(`MAX_BATCH_ROWS`), so the single `CPU_X64_S` node rarely limits throughput.

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
| `BUILD_EU_INDEX` | SQL proc | `([REBUILD=FALSE])` |
| `EU_LOCALITY`, `EU_LOCALITY_ALIAS`, `OVERTURE_ADDRESS_EU`, `OVERTURE_ADDRESS_EU_LOC` | Tables | European candidate index (skipped with `--no-eu`) |
| `LIBPOSTAL_SVC`, `PARSE_ADDRESS_INTL`, `EXPAND_STREET_INTL` | SPCS service + functions | skipped with `--no-intl` |
| `GEOCODING_TESTER` | Application service | tester app (skipped with `--no-app`) |
| `GEOCODING_TESTER_CARTO_RULE`, `GEOCODING_TESTER_CARTO_EAI` | Network rule + EAI | egress so the app can proxy the CARTO basemap (`--no-app` skips) |

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

## Benchmarks

Measured 2026-10-30 with
[`sql/examples/benchmark_functions.sql`](sql/examples/benchmark_functions.sql).
Each figure is wall-clock seconds for one statement or `CALL`, with the result
cache off. "cold" is the first run of a case and "warm" the repeat; the table
shows the warm run unless the two differ a lot.

| Setting | Value |
|---|---|
| Account | `tib85385` with the Overture Addresses share (126.3M US rows) and the EU index (139M rows) |
| Warehouses | `GEOCODING_XS_WH` = X-Small Standard (1 credit/h); `MEDIUM` = adaptive warehouse at Medium size (credit estimates below assume the Medium rate, 4 credits/h) |
| libpostal | `GEOCODING_LIBPOSTAL_POOL`: 1 node of `CPU_X64_S` (3 vCPU, 13 GiB), 1 service instance (requests 1 CPU / 2 GiB, limit 2 CPU / 4 GiB), `MAX_BATCH_ROWS = 500` |
| Inputs | real Overture rows: US addresses `"NUM STREET, CITY ZIP"`, DE+FR addresses (half each), points jittered up to ~30 m from real addresses |

**Normalization UDFs** (one `SELECT` over N rows):

| Function | 1 row | 100 | 1,000 | 10,000 | X-Small / Medium |
|---|---|---|---|---|---|
| `PARSE_ADDRESS` (US, Python) | 1.4 s | 1.9 s | 1.7 s | 1.7 s | X-Small |
| | 1.7 s | 1.2 s | 1.2 s | 1.3 s | Medium |
| `STANDARDIZE_STREET` | 1.6 s | 1.5 s | 1.6 s | 1.6 s | X-Small |
| `NORMALIZE_STREET_EU` | 1.7 s | 1.5 s | 1.6 s | 1.6 s | X-Small |
| `PARSE_ADDRESS_INTL` (SPCS) | 0.3-0.9 s | 0.5 s | 0.4 s | 0.7-1.1 s | both |
| `EXPAND_STREET_INTL` (SPCS) | 0.3 s | 0.4 s | 0.4-0.5 s | 1.2-1.6 s | both |

These scalar functions are effectively free at batch scale: about 1.2-1.5 s is
statement overhead, and 10,000 rows add well under a second. The Python UDFs
only cost noticeable time on the very first call after the warehouse resumes
(3.9 s here), while the package loads.

**Geocoding procedures** (`CALL`, warm):

| Procedure | Warehouse | 1 | 100 | 1,000 | 10,000 | Matched at 10,000 |
|---|---|---|---|---|---|---|
| `FORWARD_GEOCODE_TABLE`, US | X-Small | 7.6 s | 10.8 s | 14.0 s | 26.6 s | 9,829 (98.3%) |
| | Medium | 7.8 s | 11.8 s | 8.9 s | 12.1 s | 9,829 (98.3%) |
| `FORWARD_GEOCODE_TABLE`, DE+FR | X-Small | 23.2 s | 25.3 s | 27.7 s | 29.1 s | 10,000 (100%) |
| | Medium | 24.7 s | 32.2 s | 35.4 s | 30.1 s | 10,000 (100%) |
| `REVERSE_GEOCODE_TABLE`, `'US'`, 200 m | X-Small | 9.4 s | 278 s | 281 s | 297 s | 10,000 |
| | Medium | 11.7 s | 63 s | 61 s | 63 s | 10,000 |
| `REVERSE_GEOCODE_TABLE`, `'DE'`, 200 m | X-Small | 9.1 s | 50 s | 50 s | 51 s (5,000 pts) | 5,000 |
| | Medium | 9.1 s | 22 s | 23 s | 24 s (5,000 pts) | 5,000 |
| `REVERSE_GEOCODE_TABLE`, no country, 200 m | X-Small | 9.2 s | 1,063 s | not run | not run | |
| | Medium | 11.0 s | 196 s | not run | not run | |

Some patterns to plan around:

- **Cost is set per batch, not per row.** One address costs about as much
  as a thousand: forward US on Medium is 7.8 s for 1 row and 12.1 s for
  10,000 (about 0.013 credits, 830 rows/s). Always geocode a table, never a
  loop of single rows.
- **European forward has a fixed floor of about 25 s.** It runs a fixed
  sequence of statements (libpostal parse, key building, postcode and
  locality candidates, ranking) whatever the size, so Medium is no faster
  than X-Small. Use X-Small for EU batches.
- **Reverse geocoding is bound by the address scan.** From 100 points up,
  time is flat because the work is a spatial join over the country's
  addresses: about 63 s for the US and 23 s for DE on Medium. X-Small is
  about 4.7x slower on the US share at a quarter of the price, so a batch
  costs slightly less on Medium (0.07 vs 0.08 credits) and finishes much
  sooner.
- **Always pass a country to reverse.** Without one, every country's
  addresses are candidates. A 100-point batch takes 196 s on Medium versus
  63 s with `'US'`, and over 17 min on X-Small.
- **The libpostal pool is not the bottleneck.** 10,000 rows go through one
  `CPU_X64_S` node in about 1 s (20 calls of 500 rows). A larger instance
  family or more nodes buys nothing at these volumes. The pool costs credits
  for every hour it is running, used or not, so suspend it between batches
  (see [Pause or remove](#pause-or-remove)).

**Function tester round trips** (browser to map, `MEDIUM`, including
scratch-table setup and cleanup): normalize one address 6.6 s, forward 2 US
addresses 27-46 s, a 20-row forward batch over six countries 52 s, a 20-point
US reverse batch 88 s, and a 20-point reverse batch with "Any country" 235 s. Requests longer than the App Runtime ingress
timeout (about 60 s) run as a background job that the page polls, so large
batches finish instead of timing out.

The previous baseline (2026-07-01, another account, X-Small) measured 5,000 US
forward addresses in 24.0 s and 50,000 US reverse points in 229 s. That is in
line with the flat scaling above.

## Tracking

Every object the installer creates carries a JSON `COMMENT`, and the install
session sets a matching `QUERY_TAG`, so usage and object inventory can be
queried from Snowflake system views. The output tables written by
`FORWARD_GEOCODE_TABLE` and `REVERSE_GEOCODE_TABLE` get the same `COMMENT`.

```json
{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}
```

`attributes.source` is `spcs` on the libpostal compute pool and service and
`sql` everywhere else. Temporary working tables are not tagged. The query tag
covers only sessions that set it (the installer and the example scripts).
Calls from your own worksheets are not tagged unless you set it yourself.

```sql
-- Objects created by this package
SELECT TABLE_NAME AS object_name, TABLE_TYPE AS object_type, COMMENT
FROM   GEOCODING.INFORMATION_SCHEMA.TABLES
WHERE  COMMENT LIKE '%sf_sit-is-geocoding%'
UNION ALL
SELECT FUNCTION_NAME, 'FUNCTION', COMMENT
FROM   GEOCODING.INFORMATION_SCHEMA.FUNCTIONS
WHERE  COMMENT LIKE '%sf_sit-is-geocoding%'
UNION ALL
SELECT PROCEDURE_NAME, 'PROCEDURE', COMMENT
FROM   GEOCODING.INFORMATION_SCHEMA.PROCEDURES
WHERE  COMMENT LIKE '%sf_sit-is-geocoding%';

-- Query volume attributable to the package over the last 30 days
SELECT DATE_TRUNC('day', START_TIME) AS day,
       COUNT(*)                      AS query_count
FROM   SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE  QUERY_TAG LIKE '%sf_sit-is-geocoding%'
  AND  START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP  BY 1
ORDER  BY 1 DESC;
```

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

Measured on `OVERTURE_MAPS__ADDRESSES.CARTO.ADDRESS` and the EU index.
"Missing" columns are the share of Overture rows with that field NULL. "In a
locality" is the share of addresses inside an Overture locality polygon, which
is what an input without a postcode is matched through.

| Country | Addresses | Missing postcode | Missing house number | Missing street | Missing postal city | In a locality | Candidate path |
|---|---|---|---|---|---|---|---|
| IT | 26.8M | 100% | 0.8% | 0% | 100% | 100% | locality |
| FR | 26.0M | 0% | 0% | 0% | 100% | 97.1% | postcode, locality |
| DE | 19.3M | 79.3% | 0% | 0% | 100% | 100% | locality, postcode where present |
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
lower-cased. Distances are measured to the source Overture point. The rows are
picked by a hash of the Overture ID, so a rebuilt index gives the same set; it
changes only with a new Overture release.

| Country | Rows | Hit rate | Within 100 m | Within 500 m | Avg deviation | ADDRESS | STREET | POSTCODE | LOCALITY | NONE |
|---|---|---|---|---|---|---|---|---|---|---|
| DE | 60 | 100% | 98.3% | 98.3% | 21 m | 60 | 0 | 0 | 0 | 0 |
| ES | 60 | 100% | 93.3% | 95.0% | 101 m | 60 | 0 | 0 | 0 | 0 |
| FR | 60 | 100% | 100% | 100% | 0 m | 60 | 0 | 0 | 0 | 0 |
| IT | 60 | 100% | 100% | 100% | 3 m | 60 | 0 | 0 | 0 | 0 |
| NL | 60 | 100% | 100% | 100% | 1 m | 60 | 0 | 0 | 0 | 0 |
| PL | 60 | 100% | 96.7% | 96.7% | 152 m | 60 | 0 | 0 | 0 | 0 |
| All | 360 | 100% | 98.1% | 98.3% | 46 m | 360 | 0 | 0 | 0 | 0 |

Within-distance percentages and averages are over matched rows. Every row
matches at `ADDRESS` level. By noise type, clean input and missing postcodes
scored 100% within 100 m, abbreviated lower-case 98.3%, and local
abbreviations, lower-case and typos 96.7%.

Seven results are more than 100 m off. Four Spanish ones have a typo or a
short abbreviation in the street ("c/ fuente 3" in Fuentepinilla, "CALLE DEL
ALMOGAVERS 123" in Barcelona), and
one German lower-case input lands 1.1 km off. The two Polish ones are repeated
addresses: Overture holds "Ogrodowa 1" twice in postcode 55-311, 6.7 km apart,
and "Południowa 8" four times in 05-505, so without a village name in the text
there is nothing to pick the right one.

On larger clean batches (`sql/examples/eu_benchmark.sql`, 10,000 rows each), DE
has a 100% hit rate with 99.0% within 100 m, and FR has 99.7% with 99.4%
within 100 m.

### Cost

10,000-row batch, end to end, libpostal service already running (X-Small is
the range over four runs):

| Country | X-Small | Medium |
|---|---|---|
| DE | 37-43 s | 34 s |
| FR | 31-39 s | 30 s |

Libpostal parsing and expansion take about 3 s per batch. The candidate join
is the largest step on X-Small (8-10 s) and drops to about 4.5 s on Medium; the
rest is many short statements, so a bigger warehouse helps little. The query
profile shows only hash equi-joins, no cartesian join, and join-filter pruning
that reads at most 101 of 527 partitions of the index tables.

The European index uses about 19.7 GB of storage: `OVERTURE_ADDRESS_EU` (139M
rows, 8.1 GB, clustered by country and postcode), `OVERTURE_ADDRESS_EU_LOC`
(163M rows, 9.3 GB, clustered by country and locality), `EU_LOCALITY` (130K
polygons, 2.4 GB) and `EU_LOCALITY_ALIAS` (under 0.01 GB). About 8K localities,
Berlin, Hamburg, Wien and Praha among them, have no locality polygon in
Overture; they use the smallest same-named localadmin, county or region area
(up to 1,500 km2) that contains them. Rebuild it with
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
`EXPAND_STREET_INTL`). The installer builds the image,
pushes it to your account's repository and creates the service and functions.
Re-running it picks up a rebuilt image.
