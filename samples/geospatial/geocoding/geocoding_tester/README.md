# Geocoding Tester

A small Snowflake App Runtime app for trying the `GEOCODING.PUBLIC` functions
and seeing every result on a map. The geocoding installer deploys it by
default (`--no-app` skips it). See the parent [README](../README.md),
section "Function tester", for how to use it.

## What it does

- **Forward geocode**: up to 200 addresses, one per line. A country dropdown
  sets the default, and a `CC |` prefix sets the country for one line. It
  calls `FORWARD_GEOCODE_TABLE`.
- **Reverse geocode**: up to 200 `lat, lon` points, or clicks on the map. It
  calls `REVERSE_GEOCODE_TABLE`.
- **Normalize address**: runs `PARSE_ADDRESS`, `STANDARDIZE_STREET`,
  `PARSE_ADDRESS_INTL`, `EXPAND_STREET_INTL` and `NORMALIZE_STREET_EU` side by
  side, then geocodes the address.

Each run shows the request JSON, the response JSON and the SQL that ran.

## How it works

- Next.js on App Runtime, with deck.gl and MapLibre for the map. Queries run
  with owner's rights through `lib/snowflake.ts`.
- `app/api/{forward,reverse,normalize}` stage the input in a uniquely named
  transient table in `GEOCODING.TESTER`, call the procedure or UDFs with bind
  variables, and drop the tables afterwards.
- `app/api/job` runs those handlers in the background and the page polls for
  the result. App Runtime's ingress closes requests after about a minute,
  which a large batch can exceed.
- `app/api/basemap` proxies the CARTO basemap. App Runtime pages may only
  connect to their own origin, so the browser cannot fetch CARTO directly.
  The proxy accepts only CARTO hosts and uses `GEOCODING_TESTER_CARTO_EAI`.

## Deploy

```bash
bash deploy.sh --connection <connection>
```

`deploy.sh` creates the `GEOCODING.TESTER` schema and the CARTO network rule
and external access integration, then runs `snow app deploy`. It needs a
Snowflake CLI that has `snow app` (3.26 or later). `app.yml` sets
`query_warehouse: MEDIUM`; change it to a warehouse your role can use.

## Run locally

```bash
npm install
SNOWFLAKE_DEFAULT_CONNECTION_NAME=<connection> npx next dev
```
