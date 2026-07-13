# Snowflake vector-tile demo (`tileserver`)

Three ways to serve the **same** Snowflake geospatial dataset to a map, side by side,
so you can compare the tradeoffs - dynamic vs precomputed vs tile-free. The whole
stack stands up with **one idempotent command** via a bundled Cortex Code skill that
detect-reuse-else-creates every object it needs (Snowflake Postgres + PostGIS, SPCS
infra, and the tile server), so it installs and tears down cleanly on its own.

## The three arms

| Arm | What it demonstrates | How it is served | Freshness |
|---|---|---|---|
| **1 - Dynamic MVT** | Live `ST_AsMVT` generated per tile from PostGIS; edits show up immediately | `public.features_mvt(z,x,y)` function source, auto-published by [Martin](https://github.com/maplibre/martin) on SPCS at `/features_mvt/{z}/{x}/{y}` | Always live |
| **2 - PMTiles** | A precomputed tile pyramid baked from the same data with [tippecanoe](https://github.com/felt/tippecanoe); fast and static | A `.pmtiles` archive on a Snowflake stage, mounted into the same Martin service at `/features_pmt/{z}/{x}/{y}` | Stale until re-baked |
| **3 - H3 (tile-free)** | Native core-Snowflake H3 aggregation rendered client-side - no tiles, no server | `scripts/export_h3.py` exports `h3.json` for a deck.gl `H3HexagonLayer` | Re-export to refresh |

The **viewer** is Martin's built-in Web UI (`--webui enable-for-all`) on the public
SPCS ingress. Its *Inspect Tile Source* page renders arms 1 and 2 over a basemap;
arm 3 renders from the exported `h3.json` in `viewer/index.html`.

## How it works

```
Snowflake source table (Overture divisions, else CARTO US states - auto-detected)
        |  build_source_snapshot.py  (owned snapshot, immune to a Marketplace lapse)
        v
TILESERVER.CORE.SOURCE_FEATURES ---------------------------------------------.
        |  sync_to_pg.py                                                      |  export_geojson.py + tippecanoe
        v                                                                     v
Snowflake Postgres + PostGIS: public.features (+ GiST index)             features_pmt.pmtiles
        |  sql/features_mvt.sql                                                |  -> @TILESERVER.CORE.TILES
        v                                                                     v
public.features_mvt(z,x,y)  ==========>  Martin service (SPCS)  <=============
                                          |  public ingress + Web UI
                                          v
                       /catalog   /features_mvt/{z}/{x}/{y}   /features_pmt/{z}/{x}/{y}
```

## Prerequisites

Preflight checks all of these and **fails fast, before any billable infra**, if one
is missing:

- **Snowflake CLI** (`snow`) with an active connection whose role can create Postgres
  instances, databases/schemas, image repositories, compute pools, network
  rules/policies, external access integrations, secrets, and services. `ACCOUNTADMIN`
  covers this; a custom role works if granted those privileges (see `SKILL.md`).
- **A container runtime with its daemon running** - Docker (default) or Podman. The
  daemon must actually be up, not just installed.
- **Python 3.** The installer auto-installs the deps it needs (`psycopg2-binary`,
  `pmtiles`, `snowflake-connector-python`); set `SKIP_DEP_INSTALL=1` to only verify
  and fail fast instead.
- **An accessible polygon source in Snowflake.** By default it auto-detects the first
  reachable of Overture Maps Divisions, then CARTO Academy US States (both free), and
  snapshots it into an owned table early so a mid-run share lapse cannot break the
  install.

Set `export SNOWFLAKE_CLI_NO_UPDATE_CHECK=true` to silence CLI update prompts.

## Quick start

```bash
# US-scoped, all three arms, from scratch (creates a billable Postgres instance):
bash .cortex/skills/deploy-tileserver/scripts/install_tileserver.sh --connection <connection>
```

On success it prints the public ingress URL. Open it for the Martin Web UI, or fetch
tiles directly from `/features_mvt/{z}/{x}/{y}` and `/features_pmt/{z}/{x}/{y}`.

A full from-scratch run (fresh Postgres create included) takes roughly **15-20 min**,
dominated by the Postgres provision and the container image push - not the bake. See
the note on the tippecanoe progress meter below.

## Configuration

| Flag / env | Default | Purpose |
|---|---|---|
| `--connection <name>` | (required) | Snow CLI connection |
| `--no-pmtiles` | bake on | Skip the arm-2 PMTiles bake (arms 1 + 3 still install) |
| `--country <cc>` | `US` | Country scope for load + bake. `--country ALL` = whole world (slow) |
| `--source-table <fqn>` | auto-detect | Pin the Snowflake source table |
| `--pg-instance <name>` | auto | Reuse a specific Postgres instance |
| `--no-create-pg` | create allowed | Never CREATE a (billable) Postgres instance; require reuse |
| `SKIP_PG` / `SKIP_INFRA` / `SKIP_DATA` / `SKIP_MVT` / `SKIP_IMAGE` / `SKIP_BAKE` / `SKIP_SERVICE` | `0` | Shorten idempotent re-runs by reusing existing layers |
| `MAXZOOM` | `12` | tippecanoe max zoom for the bake |
| `SKIP_DEP_INSTALL` | `0` | Verify Python deps only; do not auto-install |

Re-running the installer is safe: each layer detects and reuses what already exists.

## What gets created

All objects carry an `oss-deploy-tileserver` COMMENT tag for discovery.

- Postgres instance `TILESERVER_PG` (PostGIS enabled) - **billable**
- Database `TILESERVER`, schema `CORE`
- Image repository `TILESERVER.CORE.IMAGES`, compute pool `TILESERVER_POOL`
- Stages `TILESERVER.CORE.SPECS` and `TILESERVER.CORE.TILES`
- Secret `TILESERVER.CORE.PG_URL`; network rules + policy `TILESERVER_PG_POLICY`
- External access integrations `TILESERVER_PG_EAI` (Postgres egress) and
  `TILESERVER_BASEMAP_EAI` (basemap tiles for the Web UI)
- Service `TILESERVER.CORE.MARTIN` with a public ingress endpoint

## Verifying the install

The installer self-checks these; to confirm independently:

```bash
C=<connection>
# 2. service RUNNING
snow sql -c $C -q "SHOW SERVICES LIKE 'MARTIN' IN SCHEMA TILESERVER.CORE;"
# 3. PMTiles archive present on the stage
snow sql -c $C -q "LIST @TILESERVER.CORE.TILES PATTERN='.*features_pmt[.]pmtiles';"
# 4. both EAIs attached
snow sql -c $C -q "DESCRIBE SERVICE TILESERVER.CORE.MARTIN;"   # external_access_integrations
# 5. public endpoint
snow sql -c $C -q "SHOW ENDPOINTS IN SERVICE TILESERVER.CORE.MARTIN;"
```

Acceptance criteria for a healthy install: arm-1 `features_mvt(0,0,0)` returns
non-empty bytes, the Martin service is `RUNNING`, a valid `features_pmt.pmtiles`
(magic `PMTiles`) sits on the TILES stage, both EAIs are attached, and the ingress
resolves to a `*.snowflakecomputing.app` host that answers (HTTP 302 to Snowflake
OAuth = live).

## Cleanup

Use the teardown script - it drops objects in dependency order (which hand SQL gets
wrong: the in-schema ingress rule stays bound to the network policy until the policy
is emptied, so a naive `DROP DATABASE` fails).

```bash
# Keep the (billable) Postgres instance for fast re-installs:
bash .cortex/skills/deploy-tileserver/scripts/teardown_tileserver.sh --connection <connection>

# Full teardown, including the Postgres instance (stops all billing):
bash .cortex/skills/deploy-tileserver/scripts/teardown_tileserver.sh --connection <connection> --drop-pg
```

## Cost

The Postgres instance and the compute pool bill while they exist. For a demo you can
spin up, verify, and tear down with `--drop-pg` in one sitting. Omit `--drop-pg` only
if you intend to re-install soon and want to skip the ~several-minute Postgres create.

## Repo layout

- **`.cortex/skills/deploy-tileserver/`** - the installable skill package (start here):
  - `SKILL.md` - full walkthrough, required privileges, configuration, cleanup.
  - `scripts/` - orchestrator (`install_tileserver.sh`), `teardown_tileserver.sh`,
    provisioners (`provision_pg.py`), and de-hardcoded ETL/bake helpers.
  - `sql/features_mvt.sql` - the arm-1 dynamic MVT function source.
  - `spcs/martin_service.yaml.tmpl` - templated SPCS service spec.
  - `tippecanoe/Dockerfile` - builds felt/tippecanoe from source for the arm-2 bake.
  - `references/` - conventions, infra DDL, and Postgres / PMTiles-bake / viewer /
    troubleshooting docs.
- **`viewer/`, `scripts/`, `spcs/`, `sql/`, `tippecanoe/`** (repo root) - the original
  ad-hoc demo artifacts the skill was distilled from, kept for reference. Large bake
  outputs under `viewer/` (`*.pmtiles`, `*.geojsonl`, `*.mbtiles`) are gitignored.

## Troubleshooting

- **tippecanoe looks stuck at ~90%.** Its progress meter is non-linear - it plateaus
  while tiling z11, then races through z12 to 100% in seconds. It is CPU-bound tiling,
  not a hang; the US bake finishes in a couple of minutes.
- **Image push shows `<layer>: Unavailable` retries** and a "only the available
  single-platform image was pushed" note - benign; the push completes.
- **Step 1 looks quiet.** The Postgres provisioner's poll output is captured by the
  orchestrator, so the terminal is quiet while the instance provisions. Not a hang.
- **`DROP DATABASE` fails on cleanup.** Use the teardown script, not hand SQL - it
  empties/drops the network policy before the database.

See `.cortex/skills/deploy-tileserver/references/troubleshooting.md` for more
(crash-loops, bad PMTiles magic, flaky reconnects, image-push hangs).
