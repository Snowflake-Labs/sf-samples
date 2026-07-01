---
name: install-geocoding
description: "One-command installer for Snowflake-native geocoding (forward + reverse) built on the Overture Maps Marketplace share, with an optional libpostal SPCS service for international addresses. Deploys the GEOCODING.PUBLIC UDFs and stored procedures, auto-detecting whichever Overture Addresses share is installed. Idempotent and self-owning: detects-and-reuses existing objects. Use when: install geocoding, deploy geocoding, set up address geocoding, geocode addresses in Snowflake, reverse geocode lat/lon, build the geocoding service, deploy Overture geocoding. Do NOT use for: high-accuracy/routing-grade geocoding (use a partner solution) or changing the Overture data source. Triggers: install geocoding, deploy geocoding, set up geocoding, address geocoding, forward geocode, reverse geocode, geocode my addresses, Overture geocoding, libpostal, international geocoding."
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
| **Forward** (free-text address -> lat/lon) | US | `usaddress` parse + street standardization + Overture JOIN |
| **Reverse** (lat/lon -> nearest address) | worldwide | `ST_DWITHIN` + `ST_DISTANCE` nearest match |
| **International forward** (optional, `--intl`) | worldwide | libpostal on Snowpark Container Services |

## What gets created (`GEOCODING.PUBLIC`)

| Object | Type | Signature |
|--------|------|-----------|
| `OVERTURE_ADDRESS` | View | stable view over the resolved Overture share |
| `PARSE_ADDRESS` | Python UDF | `(VARCHAR) -> OBJECT` |
| `STANDARDIZE_STREET` | Python UDF | `(VARCHAR) -> VARCHAR` |
| `FORWARD_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, ADDRESS_COLUMN, ID_COLUMN, OUTPUT_TABLE)` |
| `REVERSE_GEOCODE_TABLE` | SQL proc | `(SOURCE_TABLE, LAT_COLUMN, LON_COLUMN, ID_COLUMN, RADIUS_METERS, OUTPUT_TABLE, [COUNTRY])` |
| `IMAGE_REPOSITORY`, `GEOCODING_LIBPOSTAL_POOL`, `LIBPOSTAL_SVC`, `PARSE_ADDRESS_INTL`, `EXPAND_STREET_INTL` | SPCS (only with `--intl`) | international parsing |

Every object carries `COMMENT = 'oss-geocoding'` and installs under
`QUERY_TAG = 'oss-geocoding'` so it can be discovered and cleaned up later.

## Prerequisites

- **Snowflake CLI** (`snow`) with an active connection whose role can create
  databases, schemas, functions, and procedures. Verify: `snow connection list`.
- The **Overture Maps "Addresses" (Carto)** Marketplace listing installed in the
  account. It lands as `OVERTURE_MAPS__ADDRESSES` or `DS_OVERTURE_MAPS__ADDRESSES`
  — the installer auto-detects either. See `references/overture-share.md`.
- **Only for `--intl`:** Docker or Podman (amd64 image build) plus privileges to
  create an image repository, compute pool, and service. See `references/libpostal-spcs.md`.

## Workflow

### Step 1: Preflight

**Goal:** confirm the tools, connection, and data source.

1. **Check** the CLI and connection:
   ```bash
   snow --version
   snow connection list
   snow sql -c <connection> -q "SELECT CURRENT_ACCOUNT(), CURRENT_ROLE();"
   ```
2. **Detect** the Overture share (either name is fine):
   ```bash
   snow sql -c <connection> -q "SHOW DATABASES LIKE '%OVERTURE_MAPS__ADDRESSES%';"
   ```
   **If neither is present:** guide the user to acquire the "Overture Maps -
   Addresses" (Carto) listing from the Snowflake Marketplace, then re-run. Do not
   proceed without it — `00_setup.sql` will fail its sanity check.

**Output:** environment confirmed, share resolved.

### Step 2: Install (core: US forward + worldwide reverse)

**Goal:** deploy the `GEOCODING.PUBLIC` objects.

```bash
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <connection> [--warehouse <wh>]
```

The orchestrator runs the SQL modules in order in a single tagged session:
`sql/00_setup.sql` (DB/schema + `OVERTURE_ADDRESS` view + sanity check) ->
`sql/udfs/*` -> `sql/procedures/*`. Idempotent (`CREATE OR REPLACE` / `IF NOT
EXISTS`); safe to re-run.

**Output:** forward + reverse geocoding ready in `GEOCODING.PUBLIC`.

### Step 3 (optional): International path with `--intl`

**⚠️ MANDATORY STOPPING POINT** — the `--intl` path builds and pushes a container
image and creates an always-warm SPCS service (cost). Confirm with the user before
running it.

**Goal:** deploy libpostal on SPCS for worldwide address parsing.

```bash
bash .cortex/skills/install-geocoding/scripts/install_geocoding.sh \
  --connection <connection> --intl
```

This ensures `GEOCODING.PUBLIC.IMAGE_REPOSITORY`, builds the libpostal image
`--platform linux/amd64` **into the installing account's own repository**, pushes
it, then runs `libpostal_service/deploy.sql` (compute pool + service +
`PARSE_ADDRESS_INTL` / `EXPAND_STREET_INTL`). The build is heavy and slow on ARM
Macs. See `references/libpostal-spcs.md`.

**Output:** international parsing functions available once the service is `READY`.

### Step 4: Verify

```bash
snow sql -c <connection> -f sql/examples/forward_geocode.sql
snow sql -c <connection> -f sql/examples/reverse_geocode.sql
```

Confirm the smoke tests return matches. The first `PARSE_ADDRESS` call is slow
(~30 s) while the PyPI package installs; later calls are fast.

## Configuration

| Flag | Default | Purpose |
|------|---------|---------|
| `--connection` | (required) | Snow CLI connection name |
| `--warehouse` | session default | Warehouse to `USE` (MEDIUM recommended for batch jobs) |
| `--intl` | off | Also build/push libpostal image + deploy the SPCS intl service |

## Stopping Points

- ✋ After Step 1 if the Overture share is not installed (acquire it first).
- ✋ Before Step 3 (`--intl`) — heavy image build + always-warm service cost.

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
```

## Accuracy & limits

US forward geocoding is **approximate** (~500-900 m), ~98% hit rate on clean data;
coverage gaps exist. For routing-grade accuracy use a partner solution
(Carto / ESRI / Mapbox). Non-US forward matching via libpostal is fuzzier — see
`references/snowflake-sql-gotchas.md`.

## Troubleshooting

- **`00_setup.sql` sanity check returns 0 / errors:** the Overture share isn't
  installed or your role can't see it. Acquire the Carto "Addresses" listing and
  grant `IMPORTED PRIVILEGES` to your role.
- **`PARSE_ADDRESS` first call slow / times out:** the shared PyPI `usaddress`
  package is installing; retry once it warms.
- **`--intl` build fails on ARM Mac:** ensure `--platform linux/amd64`; Podman may
  OOM — prefer Docker or raise the Podman machine memory.
- **Service never `READY`:** `SELECT SYSTEM$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC');`
  and check logs via `SYSTEM$GET_SERVICE_LOGS(...)`.

## Output

Snowflake-native forward + reverse geocoding deployed in `GEOCODING.PUBLIC`,
tagged `oss-geocoding`, ready to call from any worksheet, notebook, or procedure.
