# Troubleshooting

## Martin service ingress returns "upstream connect ... connection refused"

Martin resolves PostgreSQL function sources at **startup** and `exit(1)`s (then the
container crash-loops) if the DB is unreachable. Causes, in order of likelihood:

1. **Egress not allowlisted.** SPCS egress uses a NAT pool (`153.45.59.0/24`, IP
   rotates). Confirm the PG instance's `POSTGRES_INGRESS` network rule includes the
   whole `/24`, not a `/32`. Verify the service's actual egress IP with a curl job
   to `api.ipify.org` through an EAI, then widen if outside the range.
2. **Bad / missing `DATABASE_URL` secret.** Confirm `TILESERVER.CORE.PG_URL` holds a
   valid libpq URL with `sslmode=require`.
3. **PG EAI detached.** `SHOW SERVICES` -> ensure `TILESERVER_PG_EAI` is still in the
   service's `EXTERNAL_ACCESS_INTEGRATIONS` (a later `SET ...` that omits it drops
   PG connectivity). Reattach both PG + basemap EAIs together.

After fixing, `ALTER SERVICE TILESERVER.CORE.MARTIN RESUME;` (or SUSPEND then RESUME)
and re-check `SHOW SERVICE CONTAINERS IN SERVICE TILESERVER.CORE.MARTIN;`.

## Service starts but `/features_pmt` aborts / "PMTiles error Invalid magic number"

The mounted archive is actually MBTiles (SQLite), not PMTiles. Its first bytes are
`SQLite format 3` instead of `PMTiles\x03`. Re-run the bake so `mbtiles2pmtiles.py`
produces a real archive, re-upload to `@TILESERVER.CORE.TILES`, and SUSPEND/RESUME.
(Default `on_invalid: abort` makes a bad source fatal.)

## Web UI basemaps are blank

CSP `connect-src 'self'` blocks external basemap CDNs. Attach `TILESERVER_BASEMAP_EAI`
to the service (keep the PG EAI too). See `viewer.md`.

## Postgres instance errors on rapid reconnects / long-lived connections

The instance is flaky under many short connections even at STANDARD_L. Reuse one
connection (all scripts here retry with backoff). Do not per-tile bake from a
laptop - use the tippecanoe pipeline.

## Image push to the SPCS registry hangs on the final manifest commit

Intermittent with `docker push`. Retry; if it recurs, log in fresh with
`snow spcs image-registry login -c <connection>` and push again.

## `snow` CLI format errors

`snow` >= 3.x has no `plain` output format (valid: TABLE / JSON / JSON_EXT / CSV).
The orchestrator's `obj_exists` probe uses `--format=CSV` and greps the rows.
