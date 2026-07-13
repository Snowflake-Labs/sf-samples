---
name: deploy-tileserver
description: "One-command installer for the Snowflake vector-tile demo: dynamic ST_AsMVT tiles served by Martin on SPCS (arm 1), a precomputed PMTiles archive baked from the same data (arm 2), and native H3 aggregation for client-side deck.gl (arm 3). The viewer is Martin's built-in Web UI on the public ingress. Idempotent and self-owning: detect-reuse-else-create for the Snowflake Postgres + PostGIS instance, SPCS infra (image repo, compute pool, stages, secret, EAIs), and the Martin service. Use when: install the tileserver, deploy Martin vector tiles on Snowflake, set up the Snowflake vector-tile demo, serve ST_AsMVT tiles, dynamic tiles on Snowflake Postgres, bake PMTiles from Snowflake, PostGIS MVT function source. Pass --no-pmtiles to skip the heavy bake. Triggers: install tileserver, deploy-tileserver, deploy martin, martin vector tiles, snowflake vector tiles, ST_AsMVT tiles, PMTiles bake, dynamic tiles snowflake postgres, postgis tile server."
metadata:
  author: Snowflake SIT-IS
  version: 1.0.0
  category: infrastructure
---

# Deploy Tileserver (Snowflake vector-tile demo)

One command stands up a three-arm vector-tile demo over a single dataset and is
the primary installation path for it. It is idempotent and self-owning: it
detect-reuse-else-creates the Snowflake Postgres + PostGIS instance, the SPCS
infrastructure, and the Martin service, so the stack installs and runs without
any other skill.

## Scope: the three arms

| Arm | What it shows | How it is served |
|---|---|---|
| 1 - Dynamic MVT | Live `ST_AsMVT` per tile from PostGIS; reflects edits immediately | `public.features_mvt(z,x,y)` function source auto-published by Martin at `/features_mvt/{z}/{x}/{y}` |
| 2 - PMTiles | Precomputed tile pyramid from the same data; fast, static (freshness tradeoff) | A `.pmtiles` archive baked with tippecanoe, mounted into Martin at `/features_pmt/{z}/{x}/{y}` |
| 3 - H3 (tile-free) | Native core-Snowflake H3 aggregation, rendered client-side | `scripts/export_h3.py` exports `h3.json` for a deck.gl `H3HexagonLayer` (no tiles, no server) |

The **viewer** is Martin's built-in Web UI (`--webui enable-for-all`) on the
public ingress - its Inspect Tile Source page renders arms 1 and 2 over a basemap.

## Execution Rules

1. All relative paths are relative to this skill's directory (`.cortex/skills/deploy-tileserver/`).
2. Replace `<connection>` with the active Snowflake CLI connection (`snow connection list`).
3. The installer is idempotent: re-running detects existing objects and reuses them. Use `SKIP_*` env vars only to shorten re-runs.
4. Every session sets the `query_tag` and every object created carries the `oss-deploy-tileserver` COMMENT tag (see `references/conventions.md`).
5. After a run the installer writes a friction log to `logs/`.

## Prerequisites

- Container runtime (Docker or Podman) with its **daemon running** (preflight fails fast, before any billable infra, if the daemon is down); Python 3 with `snowflake-connector-python`, `psycopg2`, and `pmtiles`; Snowflake CLI (`snow`).
  - The installer's preflight auto-installs any missing Python deps (`psycopg2-binary`, `pmtiles`, `snowflake-connector-python`). Set `SKIP_DEP_INSTALL=1` to only verify and fail fast instead. To install by hand: `pip install snowflake-connector-python psycopg2-binary pmtiles`.
- `export SNOWFLAKE_CLI_NO_UPDATE_CHECK=true`.
- An active connection whose role can create Postgres instances, databases, schemas, image repositories, compute pools, network rules, external access integrations, secrets, and services (see `## Required Privileges`).
- A polygon source dataset in core Snowflake. By default the installer **auto-detects** the first accessible of its known free datasets (Overture Maps - Divisions, then CARTO Academy US States) and **snapshots it into an owned `TILESERVER.CORE.SOURCE_FEATURES` table** early, so a Marketplace share that is missing or lapses mid-run cannot break the install. Preflight fails fast (before any billable infra) if none is reachable. Override with `--source-table <fqn>` / `SOURCE_PROFILE`.

## One-command install

```bash
bash .cortex/skills/deploy-tileserver/scripts/install_tileserver.sh --connection <connection>
```

The orchestrator runs these layers in order (detect-and-reuse-else-create throughout):

0. **Preflight** - tools, container runtime, connection, account.
1. **Postgres + PostGIS** - `scripts/provision_pg.py` reuses an instance (or CREATEs one - billable), installs PostGIS, wires the SPCS egress allowlist, and emits the `DATABASE_URL`. See `references/postgres.md`.
2. **SPCS infra** - `references/infra.sql` creates `TILESERVER.CORE` + image repo, compute pool, spec/tiles stages, and the basemap EAI; the orchestrator then creates the `PG_URL` secret and the `TILESERVER_PG_EAI` egress integration for the resolved host.
3. **Data** - `scripts/build_source_snapshot.py` resolves + CTASs the source into the owned `TILESERVER.CORE.SOURCE_FEATURES` snapshot, then `scripts/sync_to_pg.py` loads it into `public.features` with a GiST index.
4. **MVT function** - applies `sql/features_mvt.sql` (the arm-1 function source).
5. **Martin image** - `scripts/build_push_martin.sh` publishes the Martin image to the SPCS repo.
6. **PMTiles bake** (default on) - `scripts/bake_pmtiles.sh` exports GeoJSONL, builds tippecanoe, bakes, converts to PMTiles, and uploads to the TILES stage. Skip with `--no-pmtiles`. See `references/pmtiles-bake.md`.
7. **Service** - renders `spcs/martin_service.yaml.tmpl`, uploads it, and `CREATE`/`ALTER`s `TILESERVER.CORE.MARTIN` with both EAIs and the Web UI. See `references/viewer.md`.
8. **Verify** - checks `features_mvt(0,0,0)` length and prints the public ingress URL.

## After a successful install (what to hand the user)

The installer ends with a `====` banner containing the **App URL** (the public
ingress) and numbered next steps. Always relay that to the user - lead with the URL,
then the steps. If running this skill for someone, your closing message should mirror
the banner:

1. **App URL** - the `https://<host>.snowflakecomputing.app` ingress the installer
   printed. Open it and sign in with Snowflake credentials (the endpoint is auth-gated).
2. In Martin's **Web UI**, open *Inspect Tile Source* and pick `features_mvt` (arm 1,
   live `ST_AsMVT`) or `features_pmt` (arm 2, precomputed PMTiles); pan/zoom over the US.
3. Tiles are served at `/catalog`, `/features_mvt/{z}/{x}/{y}`, `/features_pmt/{z}/{x}/{y}`.
4. **Arm 3 (H3, no server):** `SNOWFLAKE_CONNECTION=<conn> python3 scripts/export_h3.py`,
   then open `viewer/index.html` locally for the deck.gl H3 layer.
5. Remind them to **tear down** when done (billing): `teardown_tileserver.sh --connection <conn> --drop-pg`.

If the banner shows the URL as "not resolved yet", the endpoint is still provisioning
(a few minutes after RUNNING); tell the user to re-fetch it with
`SHOW ENDPOINTS IN SERVICE TILESERVER.CORE.MARTIN;` and then follow the same steps.

## Configuration

| Parameter | Default | Purpose |
|---|---|---|
| `--connection` | (required) | Snow CLI connection name |
| `--no-pmtiles` | (unset; bake on) | Skip the heavy arm-2 PMTiles bake |
| `--pg-instance <name>` | (auto) | Reuse a specific Postgres instance |
| `--no-create-pg` | (unset; create allowed) | Never CREATE a PG instance (billable); require reuse |
| `--source-table <fqn>` | (auto-detect: Overture divisions, else CARTO states) | Pin the Snowflake source table |
| `--country <cc>` | `US` | Country scope for the load + bake. Defaults to `US` (a global load + bake is impractically slow). Pass `--country ALL` for the whole world. |
| `SKIP_PG` / `SKIP_INFRA` / `SKIP_DATA` / `SKIP_MVT` / `SKIP_IMAGE` / `SKIP_BAKE` / `SKIP_SERVICE` | `0` | Shorten idempotent re-runs |

## Required Privileges

| Privilege | Scope | Why |
|---|---|---|
| CREATE POSTGRES INSTANCE | account | provision the PG instance when self-created |
| CREATE DATABASE / SCHEMA | account | `TILESERVER` + `CORE` |
| CREATE IMAGE REPOSITORY | schema | push the Martin image |
| CREATE COMPUTE POOL | account | run the Martin service |
| CREATE NETWORK RULE / POLICY | account/schema | PG ingress allowlist + SPCS egress |
| CREATE INTEGRATION | account | PG + basemap external access integrations |
| CREATE SECRET | schema | store the `DATABASE_URL` |
| CREATE SERVICE | schema | `TILESERVER.CORE.MARTIN` |

ACCOUNTADMIN satisfies all of the above but is not required if the above are granted to a custom role.

## Cleanup

Use the teardown script - it drops objects in the correct dependency order
(service -> optional PG instance -> network policy -> EAIs -> compute pool ->
database), which hand SQL gets wrong (see `references/troubleshooting.md`):

```bash
# Keep the (billable) Postgres instance for fast re-installs:
bash .cortex/skills/deploy-tileserver/scripts/teardown_tileserver.sh --connection <connection>

# Full teardown, including the Postgres instance:
bash .cortex/skills/deploy-tileserver/scripts/teardown_tileserver.sh --connection <connection> --drop-pg
```

Equivalent manual SQL (ordering matters - the in-schema `PG_INGRESS` rule stays
bound to `TILESERVER_PG_POLICY` until the policy is emptied/dropped, so
`DROP DATABASE` fails otherwise):

```sql
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

DROP SERVICE IF EXISTS TILESERVER.CORE.MARTIN;
-- The Postgres instance is billable and may be shared; drop only on a full teardown.
-- Quote the name if it was created as a lowercase identifier:
-- DROP POSTGRES INSTANCE IF EXISTS TILESERVER_PG;
ALTER NETWORK POLICY TILESERVER_PG_POLICY SET ALLOWED_NETWORK_RULE_LIST=();
DROP NETWORK POLICY IF EXISTS TILESERVER_PG_POLICY;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TILESERVER_PG_EAI;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TILESERVER_BASEMAP_EAI;
DROP COMPUTE POOL IF EXISTS TILESERVER_POOL;
DROP DATABASE IF EXISTS TILESERVER;   -- image repo, stages, secret, network rules, MVT metadata
```

All objects carry the `oss-deploy-tileserver` COMMENT tag for discovery (see `references/conventions.md`).

## References

- `references/conventions.md` - query_tag + COMMENT tracking literals.
- `references/infra.sql` - detect-reuse-else-create SPCS infra DDL.
- `references/postgres.md` - PG instance + PostGIS + the SPCS egress `/24` fix.
- `references/pmtiles-bake.md` - the tippecanoe bake pipeline and its gotchas.
- `references/viewer.md` - Martin Web UI + the basemap CSP/EAI fix.
- `references/troubleshooting.md` - crash-loops, bad-PMTiles magic, flaky reconnects, image-push hangs.
