# Snowflake vector-tile demo (tileserver)

A three-arm vector-tile demo over a single Snowflake dataset, packaged as an
installable Cortex Code skill:

1. **Dynamic MVT** - live `ST_AsMVT` per tile from Snowflake Postgres + PostGIS,
   served by [Martin](https://github.com/maplibre/martin) on SPCS.
2. **PMTiles** - a precomputed tile pyramid baked from the same data with
   tippecanoe, mounted into the same Martin service (freshness tradeoff demo).
3. **H3 (tile-free)** - native core-Snowflake H3 aggregation rendered client-side
   with deck.gl (no tiles, no server).

The viewer is Martin's built-in Web UI on the public SPCS ingress.

## Install

The whole stack installs with one idempotent command via the bundled skill:

```bash
bash .cortex/skills/deploy-tileserver/scripts/install_tileserver.sh --connection <connection>
```

It detect-reuse-else-creates the Postgres + PostGIS instance, the SPCS infra
(image repo, compute pool, stages, secret, EAIs), bakes PMTiles, and deploys the
Martin service. Pass `--no-pmtiles` to skip the heavy bake. See
[`.cortex/skills/deploy-tileserver/SKILL.md`](.cortex/skills/deploy-tileserver/SKILL.md)
for full configuration, required privileges, and cleanup.

## Layout

- `.cortex/skills/deploy-tileserver/` - the installable skill package (start here).
  - `scripts/` - orchestrator + provisioners + de-hardcoded ETL/bake helpers.
  - `sql/features_mvt.sql` - the arm-1 MVT function source.
  - `spcs/martin_service.yaml.tmpl` - templated SPCS service spec.
  - `tippecanoe/Dockerfile` - builds felt/tippecanoe for the arm-2 bake.
  - `references/` - conventions, infra DDL, Postgres/bake/viewer docs, troubleshooting.
- `viewer/`, `scripts/`, `spcs/`, `sql/`, `tippecanoe/` - the original ad-hoc demo
  artifacts the skill was distilled from (kept for reference; bake outputs in
  `viewer/` are gitignored).
