# Troubleshooting

Run `scripts/preflight.sh` before an install and `scripts/verify_geocoding.sh`
after one; each failing line names its fix. The latest receipt in `logs/` shows
which step failed and how long each took.

## Preflight and install

| Symptom | Cause | Fix |
|---------|-------|-----|
| Preflight: `no 'snow app' commands` (DEGRADED) | Snowflake CLI older than 3.26 (in 3.16, `snow app` is the Native Apps command) | `pipx upgrade snowflake-cli`, or pass `--no-app`. The installer skips the app and marks it DEGRADED |
| Preflight: `no running Docker daemon or Podman machine` | Docker Desktop or Podman not started | start it, or pass `--no-intl` (US forward + reverse only) |
| Preflight: `SHOW COMPUTE POOLS failed` | SPCS not available (trial, or region/edition without it) | pass `--no-intl --no-app` |
| Preflight: `listing ... is not visible` | listing not auto-fulfilled to this region | get "Overture Maps - Addresses" / "Divisions" (Carto) in Marketplace under the default name, then re-run |
| Preflight: `no direct CREATE COMPUTE POOL grant` (DEGRADED) | the grant may be inherited, which preflight cannot see | use ACCOUNTADMIN, or check `SHOW GRANTS TO ROLE` up the hierarchy |
| `00_setup.sql` sanity check returns 0 | listing could not be acquired, or role lacks `IMPORTED PRIVILEGES` | acquire the listing manually and `GRANT IMPORTED PRIVILEGES ON DATABASE OVERTURE_MAPS__ADDRESSES TO ROLE <role>` |
| Image push: `timeout awaiting response headers` | slow uplink; the image is about 3.5 GB | re-run: uploaded layers are kept, and once the `VERSION` tag is in the registry the build is skipped |
| libpostal build fails on an ARM Mac | Podman machine with 2 GiB memory runs out | prefer Docker, or give the Podman machine 6 GiB or more |
| `LIBPOSTAL_SVC not READY after 20 min` | image pull or OOM | `SELECT SYSTEM$GET_SERVICE_LOGS('GEOCODING.PUBLIC.LIBPOSTAL_SVC', 0, 'libpostal', 100);` |
| `deploy.sql does not use image tag ... from VERSION` | `libpostal_service/VERSION` changed without updating `deploy.sql` | set the same tag in both `deploy.sql` spec blocks and `service-spec.yaml` |
| EU index build takes hours | warehouse below X-Large | `--eu-warehouse GEOCODING_BUILD_WH` (X-Large, AUTO_SUSPEND 60); it is a one-time build |

## Using the functions

| Symptom | Cause | Fix |
|---------|-------|-----|
| First call to `PARSE_ADDRESS` takes about 30 s | shared PyPI `usaddress` package installing | none; later calls are fast |
| First libpostal call takes 16 s, or about 100 s | service (and pool) auto-suspended after 30 min idle | expected; press Start in the tester beforehand, or `ALTER COMPUTE POOL GEOCODING_LIBPOSTAL_POOL RESUME; ALTER SERVICE GEOCODING.PUBLIC.LIBPOSTAL_SVC RESUME;` |
| European rows return `need the libpostal service` | installed with `--no-intl` / `--no-eu`, or service failed | re-run the installer without those flags; check `SYSTEM$GET_SERVICE_STATUS` |
| Reverse geocoding with no country is slow | it scans every country's addresses | pass a country code |
| Another role gets `does not exist or not authorized` | no grants | re-run with `--grant-role <ROLE>` |
| Another role: `Insufficient privileges to operate on service` | missing service role for libpostal | `GRANT SERVICE ROLE GEOCODING.PUBLIC.LIBPOSTAL_SVC!ALL_ENDPOINTS_USAGE TO ROLE <ROLE>;` |

## Tester app

| Symptom | Cause | Fix |
|---------|-------|-----|
| Blank map, points still drawn | basemap EAI missing or not attached | `bash geocoding_tester/deploy.sh --connection <conn>` re-creates `GEOCODING_TESTER_CARTO_EAI` |
| `upstream request timeout` | request over the 60 s App Runtime limit | already handled by the `/api/job` polling route; redeploy if you run an older build |
| libpostal chip says `status unavailable` | app owner role cannot see the service | deploy the app with the role that owns `LIBPOSTAL_SVC` |
| Start / Stop do nothing | the app owner lacks `OPERATE` on the service or pool | deploy as the owner, or grant `OPERATE` on both to the owner role |
| Verify: `function tester URL not found` | app not deployed, or CLI too old | `cd geocoding_tester && snow app open -c <conn> --print-only` |
