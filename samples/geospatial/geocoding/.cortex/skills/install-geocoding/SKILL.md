---
name: install-geocoding
description: "One-command installer for Snowflake-native geocoding (forward + reverse) built on the Overture Maps Marketplace share, with a libpostal SPCS service for international addresses, the European forward index and a map-based tester app (GEOCODING_TESTER). Deploys the GEOCODING.PUBLIC UDFs and stored procedures, auto-detecting whichever Overture Addresses share is installed. Idempotent and self-owning: detects-and-reuses existing objects. Use when: install geocoding, deploy geocoding, set up address geocoding, geocode addresses in Snowflake, reverse geocode lat/lon, build the geocoding service, deploy Overture geocoding. Do NOT use for: high-accuracy/routing-grade geocoding (use a partner solution) or changing the Overture data source. Triggers: install geocoding, deploy geocoding, set up geocoding, address geocoding, forward geocode, reverse geocode, geocode my addresses, Overture geocoding, libpostal, international geocoding."
metadata:
  author: Snowflake
  version: 1.0.0
  category: geospatial
---

# Install Geocoding (Snowflake-native)

One command stands up **forward + reverse geocoding** entirely inside Snowflake,
built on the **Overture Maps** Marketplace share — no external geocoding APIs.
The installer is **idempotent** and **self-owning**: it deploys everything into
`GEOCODING.PUBLIC`, detects whichever Overture Addresses share is present, and
re-running detects-and-reuses existing objects.

| Capability | Coverage | How |
|---|---|---|
| **Forward** (free-text address -> GEOGRAPHY) | US | `usaddress` parse + street standardization + Overture JOIN |
| **Forward, Europe** (default; `--no-eu` skips) | FR ES PL BE DK CH AT NL NO FI PT DE IT CZ | libpostal parse + expansion, postcode or locality-polygon candidates, street ranking |
| **Reverse** (lat/lon -> nearest address) | worldwide | `ST_DWITHIN` + `ST_DISTANCE` nearest match |
| **International parsing** (default; `--no-intl` skips) | worldwide | libpostal on Snowpark Container Services |

The UK, Ireland and Sweden have no addresses in this Overture share. Forward
rows for GB, UK or IE return `MATCH_LEVEL = 'NONE'`; UK support needs another
dataset.

## What gets created (`GEOCODING.PUBLIC`)

| Object | Type | Signature |
|--------|------|-----------|
| `OVERTURE_ADDRESS` | View | stable view over the resolved Overture share |
| `PARSE_ADDRESS` | Python UDF | `(VARCHAR) -> OBJECT` |
| `STANDARDIZE_STREET` | Python UDF | `(VARCHAR) -> VARCHAR` |
| `NORMALIZE_STREET_EU`, `NORMALIZE_NAME_EU` | Python UDF | `(VARCHAR, VARCHAR) -> VARCHAR`, `(VARCHAR) -> VARCHAR` |
| `FORWARD_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, ADDRESS_COLUMN, ID_COLUMN, OUTPUT_TABLE, [COUNTRY='US'], [COUNTRY_COLUMN])` |
| `REVERSE_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, LAT_COLUMN, LON_COLUMN, ID_COLUMN, RADIUS_METERS, OUTPUT_TABLE, [COUNTRY])` |
| `EVALUATE_GEOCODE` | SQL proc | `(RESULT_TABLE, ACTUAL_GEOG, GEOCODED_GEOG, [THRESHOLD])`, grouped overload `(..., THRESHOLD, GROUP_COLUMN)` |
| `BUILD_EU_INDEX` | SQL proc | `([REBUILD=FALSE])` |
| `EU_LOCALITY`, `EU_LOCALITY_ALIAS`, `OVERTURE_ADDRESS_EU`, `OVERTURE_ADDRESS_EU_LOC` | Tables (skipped with `--no-eu`) | European candidate index, about 19.7 GB |
| `IMAGE_REPOSITORY`, `GEOCODING_LIBPOSTAL_POOL`, `LIBPOSTAL_SVC`, `PARSE_ADDRESS_INTL`, `EXPAND_STREET_INTL` | SPCS (skipped with `--no-intl`) | international parsing |
| `GEOCODING_TESTER` | Application service (skipped with `--no-app`) | Snowflake App Runtime tester: forward, reverse and normalize on a map |
| `GEOCODING_TESTER_CARTO_RULE`, `GEOCODING_TESTER_CARTO_EAI` | Network rule + EAI (skipped with `--no-app`) | egress for the app's same-origin CARTO basemap proxy |

Every object carries a JSON tracking `COMMENT` and installs under a matching
JSON `QUERY_TAG` with `"origin":"sf_sit-is-geocoding"` and
`"name":"oss-geocoding"`, so it can be discovered and cleaned up later with
`COMMENT LIKE '%sf_sit-is-geocoding%'` (see "Tracking" in the README).

## Prerequisites

- **Snowflake CLI** (`snow`) with an active connection whose role can **create
  databases from a listing** (`IMPORT SHARE` / `CREATE DATABASE`), plus create
  schemas, functions, and procedures. Verify: `snow connection list`.
- The **Overture Maps "Addresses" (Carto)** Marketplace listing — the installer
  **auto-acquires** it (free, listing `GZT0Z4CM1E9NQ`) and reuses an existing
  copy under either `OVERTURE_MAPS__ADDRESSES` or `DS_OVERTURE_MAPS__ADDRESSES`.
  See `references/overture-share.md`.
- A running Docker daemon or Podman machine (amd64 image build) plus privileges
  to create an image repository, compute pool, and service. Use `--no-intl` if
  you have neither (US forward + reverse only). See `references/libpostal-spcs.md`. Podman machines with
  2 GiB of memory tend to fail this build; prefer Docker.

## External access integrations

**None required for geocoding.** The tester app gets `GEOCODING_TESTER_CARTO_EAI` (CARTO basemap hosts only), because Snowflake App Runtime's CSP blocks the browser from fetching tiles directly. All geocoding runs on Snowflake-resident data plus the shared
PyPI repository (`usaddress`, via `snowflake.snowpark.pypi_shared_repository`).
The libpostal SPCS service bakes its ~2 GB models into the container
image at build time and makes no outbound network calls, so it needs no external
access integration or network rule.

## Workflow

### Step 1: Preflight

**Goal:** confirm the tools and connection. The Overture share is acquired
automatically in Step 2, so no manual Marketplace step is needed up front.

1. **Check** the CLI and connection:
   ```bash
   snow --version
   snow connection list
   snow sql -c <connection> -q "SELECT CURRENT_ACCOUNT(), CURRENT_ROLE();"
   ```
2. **(Optional)** see whether an Overture Addresses share is already present
   (either name is fine; the installer also auto-acquires it):
   ```bash
   snow sql -c <connection> -q "SHOW DATABASES LIKE '%OVERTURE_MAPS__ADDRESSES%';"
   ```

**Output:** environment confirmed.

### Step 2: Install

**MANDATORY STOPPING POINT**: the default install builds and pushes a ~3.5 GB
container image, creates an SPCS compute pool (`CPU_X64_S`, auto-suspends after
10 min idle) and service, builds a one-time European index (~19.7 GB, hours below
X-Large) and deploys the `GEOCODING_TESTER` app. Confirm with the user first, and
offer `--no-intl` (US + reverse only), `--no-eu` or `--no-app`.

The index build wants an X-Large warehouse. If you do not have one, create one
that suspends after a minute:

```bash
snow sql -c <connection> -q "CREATE WAREHOUSE IF NOT EXISTS GEOCODING_BUILD_WH
  WAREHOUSE_SIZE = XLARGE AUTO_SUSPEND = 60 INITIALLY_SUSPENDED = TRUE
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';"
```

```bash
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <connection> [--warehouse <wh>] --eu-warehouse GEOCODING_BUILD_WH
```

The orchestrator runs, in order:

1. The SQL modules in one tagged session: `sql/00_setup.sql` (DB/schema,
   **auto-acquires the Overture Addresses listing** `GZT0Z4CM1E9NQ`, builds the
   `OVERTURE_ADDRESS` view and sanity check), then `sql/udfs/*` and
   `sql/procedures/*`. If the listing can't be auto-acquired, acquire
   "Overture Maps - Addresses" (Carto) manually and re-run.
2. libpostal on SPCS: ensures `GEOCODING.PUBLIC.IMAGE_REPOSITORY`, builds the
   image `--platform linux/amd64` into this account's repository, pushes it, runs
   `libpostal_service/deploy.sql` and waits for `READY`. See
   `references/libpostal-spcs.md`.
3. `BUILD_EU_INDEX(FALSE)`, which builds only the European tables that are
   missing. A re-run reports "EU index already present".
4. `geocoding_tester/deploy.sh`, which deploys the `GEOCODING_TESTER` SAR app and
   prints its URL.

Everything is idempotent; re-running resumes a suspended pool or service and
re-applies the service spec.

**Output:** forward (US + 14 EU countries), reverse and international parsing in
`GEOCODING.PUBLIC`, plus the tester app.

### Step 4: Verify

```bash
snow sql -c <connection> -f sql/examples/forward_geocode.sql
snow sql -c <connection> -f sql/examples/reverse_geocode.sql
# European:
snow sql -c <connection> -f sql/examples/eu_test_set.sql
snow sql -c <connection> -f sql/examples/eu_forward_geocode.sql
snow sql -c <connection> -f sql/examples/eu_reverse_geocode.sql
snow sql -c <connection> --warehouse GEOCODING_XS_WH -f sql/examples/eu_benchmark.sql
```

The benchmark timings in the README are for X-Small. Create one if needed:
`CREATE WAREHOUSE IF NOT EXISTS GEOCODING_XS_WH WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60 INITIALLY_SUSPENDED = TRUE COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';`

Confirm the smoke tests return matches. The first `PARSE_ADDRESS` call is slow
(~30 s) while the PyPI package installs; later calls are fast.

### Step 4: Hand off

**Always end the install by giving the user:**

1. The **function tester link**, printed by the installer as
   `Function tester: https://...`. If it is missing, get it with
   `cd geocoding_tester && snow app open -c <connection> --print-only`.
2. The **README** path, pointing to its "Function tester", "Batch processing"
   and "Benchmarks" sections.

Example: "Geocoding is installed. Try it in the function tester at <URL>.
Batch SQL and benchmark numbers are in README.md."

## Configuration

| Flag | Default | Purpose |
|------|---------|---------|
| `--connection` | (required) | Snow CLI connection name |
| `--warehouse` | session default | Warehouse to `USE` (MEDIUM recommended for batch jobs) |
| `--no-intl` | off | Skip the libpostal SPCS service and, with it, the EU index |
| `--no-eu` | off | Keep libpostal, skip the European forward index |
| `--no-app` | off | Skip deploying the `GEOCODING_TESTER` app |
| `--intl`, `--eu` | n/a | Accepted for backward compatibility; now the default |
| `--eu-warehouse` | `--warehouse` | Warehouse for the one-time EU index build (X-Large recommended) |

## Stopping Points

- ✋ Before Step 2: image build + SPCS service cost, about 19.7 GB EU index storage, and the tester app.
- ✋ If the Overture listing cannot be auto-acquired in your region — acquire it
  manually from the Marketplace, then re-run.

## Usage after install

```sql
-- Forward (US): free-text address column -> lat/lon
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'MY_DB.PUBLIC.PERMITS', 'ADDRESS_TEXT', 'PERMIT_ID',
    'MY_DB.PUBLIC.PERMITS_GEOCODED');

-- Reverse (worldwide): lat/lon -> nearest address within radius (meters)
CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE(
    'MY_DB.PUBLIC.PINGS', 'LAT', 'LON', 'DEVICE_ID', 200,
    'MY_DB.PUBLIC.PINGS_ADDRESSED', 'US');   -- omit country / pass NULL for worldwide

-- Forward (Europe): one country for all rows, or a per-row column
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'MY_DB.PUBLIC.CUSTOMERS', 'ADDRESS', 'ID',
    'MY_DB.PUBLIC.CUSTOMERS_GEOCODED', 'US', 'COUNTRY_CODE');
-- adds result_country, MATCH_LEVEL (ADDRESS|STREET|POSTCODE|LOCALITY|NONE), MATCH_SCORE
```

## Accuracy & limits

US forward geocoding is **approximate** (~500-900 m), ~98% hit rate on clean data;
coverage gaps exist. For routing-grade accuracy use a partner solution
(Carto / ESRI / Mapbox).

European forward geocoding on the 360-row noisy test set (60 each for DE, FR,
IT, NL, ES, PL) has a 100% hit rate, with 98.1% of results within 100 m, all at
`ADDRESS` level. FR, IT and NL are exact; ES is the weakest at 93.3% within
100 m (typos and short abbreviations), and PL has repeated street and number
pairs in neighbouring villages. A 10,000-row batch takes 30-45 s on X-Small or Medium.
The full tables, cost and limitations (UK none, CZ and PL street gaps; PT, FI and
CH house-number gaps) are in the README section "European forward geocoding".

## Troubleshooting

- **`00_setup.sql` sanity check returns 0 / errors:** the Overture listing
  couldn't be auto-acquired (region not auto-fulfilled) or your role lacks
  `CREATE DATABASE` / `IMPORT SHARE`. Acquire the Carto "Addresses" listing
  manually and grant `IMPORTED PRIVILEGES` to your role, then re-run.
- **`PARSE_ADDRESS` first call slow / times out:** the shared PyPI `usaddress`
  package is installing; retry once it warms.
- **libpostal build fails on ARM Mac:** ensure `--platform linux/amd64`; Podman may
  OOM, so prefer Docker or raise the Podman machine memory. The installer uses
  whichever runtime actually answers (`docker info` / `podman info`).
- **European rows return "need the libpostal service":** run the installer without
  `--no-intl` / `--no-eu`, and check the service is `READY` (it may be suspended).
- **Image push slow or "timeout awaiting response headers":** the first push
  uploads about 2.5 GB in ten steps (the model parts are 200 MB each), one step
  at a time with retries. On a 0.25 MB/s uplink that takes hours; on a few MB/s,
  minutes. If it still fails, re-run the installer: uploaded layers are kept.
- **Service never `READY`:** `SELECT SYSTEM$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC');`
  and check logs via `SYSTEM$GET_SERVICE_LOGS(...)`.

## Output

Snowflake-native forward + reverse geocoding deployed in `GEOCODING.PUBLIC`,
tagged with origin `sf_sit-is-geocoding`, ready to call from any worksheet, notebook, or procedure.
To suspend the libpostal service, free the EU index storage or remove
everything, follow "Pause or remove" in the README.
