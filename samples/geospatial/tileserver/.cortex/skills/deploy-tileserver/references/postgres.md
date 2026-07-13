# Postgres + PostGIS for the tileserver

Arm 1 (dynamic `ST_AsMVT`) and the PMTiles bake both read a `public.features`
table in a **Snowflake Postgres** instance with the **PostGIS** extension. The
installer's `scripts/provision_pg.py` handles this layer with a
detect-reuse-else-create strategy.

## What provision_pg.py does

1. **Detect.** Lists Postgres instances on the account (`SHOW POSTGRES INSTANCES`
   equivalent via the connection). If one is passed with `--pg-instance` (or found)
   and reachable with PostGIS available, it is **reused**.
2. **Create (only if none).** Otherwise it `CREATE`s a STANDARD-tier instance
   (billable) and waits for it to become active. This is confirmation-gated by the
   orchestrator - a fresh instance incurs cost.
3. **PostGIS.** Runs `CREATE EXTENSION IF NOT EXISTS postgis;` and verifies with
   `postgis_full_version()` + a `SELECT ST_AsMVT(...)` smoke test.
4. **Egress allowlist (the non-obvious blocker).** SPCS external-access egress
   leaves via a **NAT pool**, not a single IP. Observed range: `153.45.59.0/24`
   (the specific IP rotates). A `/32` allow fails intermittently. The provisioner
   adds `153.45.59.0/24` to a `POSTGRES_INGRESS` network rule
   (`TILESERVER.CORE.PG_INGRESS`) in a network policy attached to the instance, and
   keeps the operator's own dev IP for local `psql`.
5. **Connection string.** Emits a full libpq `DATABASE_URL`
   (`postgresql://<user>:<pw>@<host>:5432/postgres?sslmode=require`) to stdout as
   `PG_URL=...` and `PGHOST=...` lines the orchestrator captures. The host is
   resolved from the instance - never hardcoded.

## How downstream steps consume it

- The orchestrator stores the URL in the `TILESERVER.CORE.PG_URL` **secret** and
  wires the SPCS **egress** EAI (`TILESERVER_PG_EAI`) to `<host>:5432`.
- `sync_to_pg.py`, `pgexec.py`, `bake_pmtiles.py` all read `PG_URL`/`DATABASE_URL`
  from the env (fallback: libpq service `tileserver` in `~/.pg_service.conf`).

## Gotchas

- **Martin crash-loops if PG is unreachable at startup.** It resolves PG function
  sources on boot and `exit(1)`s if the connection fails, so the ingress returns
  "upstream connect ... connection refused" until egress + credentials work. Fix
  egress first, then `ALTER SERVICE ... RESUME`.
- **The instance is flaky on rapid reconnects / long-lived connections** even at
  STANDARD_L. Reuse a single connection; per-tile baking from a laptop is
  impractical - prefer the tippecanoe bake pipeline.
