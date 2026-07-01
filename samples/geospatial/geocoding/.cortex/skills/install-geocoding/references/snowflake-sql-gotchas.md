# Snowflake SQL gotchas (geocoding)

Notes that matter when editing or debugging the geocoding SQL.

## Overture share indirection

- All matching SQL reads `GEOCODING.PUBLIC.OVERTURE_ADDRESS`, never the physical
  share name. If you add a new physical share name, update the `candidates`
  array in `sql/00_setup.sql` — nowhere else.
- The `00_setup.sql` detection uses a Snowflake Scripting anonymous block
  (`EXECUTE IMMEDIATE $$ ... $$`). It cannot be validated with compile-only mode;
  run it against a connection to test.

## Forward matching (US)

- Match keys: `COUNTRY = 'US'` AND exact `NUMBER` AND (`POSTCODE` match OR
  `POSTAL_CITY` match) AND `STREET LIKE %standardized%`, ranked by `EDITDISTANCE`.
- The `POSTCODE OR POSTAL_CITY` disjunction is deliberate: ~38% of US Overture
  rows have a NULL `POSTAL_CITY`, so requiring city would drop many matches.
- `STANDARDIZE_STREET` expands USPS abbreviations to Overture's full spelling
  (`E 38TH ST` -> `East 38th Street`). Missing matches are often an abbreviation
  the dictionary doesn't cover — extend `STREET_TYPES` / `MULTI_WORD_ABBREVS`.

## Reverse matching (worldwide)

- The input point is built once per row as a `GEOGRAPHY` with
  `ST_POINT(lon, lat)` in the `remaining` CTE and reused for `ST_DWITHIN`,
  `ST_DISTANCE`, and the `ROW_NUMBER` ordering. Do not rebuild it inline with
  `TO_GEOGRAPHY('POINT(...)')` string concatenation — that is slower (constructed
  3x) and reintroduces WKT escaping issues inside the dollar-quoted body.
- `ST_DWITHIN(GEOMETRY, point, RADIUS_METERS)` filters candidates; `ST_DISTANCE`
  ranks; `ROW_NUMBER` dedups to the single closest. `RADIUS_METERS` is required
  (no default) — too large a radius scans more rows and slows the query.
- `COUNTRY` is optional: NULL / '' / 'ALL' searches worldwide; otherwise it adds
  `AND a.COUNTRY = '<ISO>'`.

## GEOGRAPHY-only output contract

- Both procedures output `GEOGRAPHY` columns, not FLOAT lat/lon:
  `FORWARD_GEOCODE_TABLE` -> `result_geog`; `REVERSE_GEOCODE_TABLE` ->
  `input_geog` + `result_geog`. `ST_POINT(lon, lat)` takes longitude first.
- To surface coordinates use `ST_X(geog)` (lon) / `ST_Y(geog)` (lat). Pass
  `result_geog` straight into `EVALUATE_GEOCODE` — no `ST_POINT` wrapping needed.

## Dynamic SQL in the procedures

- Both procs build the result query with `EXECUTE IMMEDIATE` string
  concatenation. Inside the dollar-quoted body, literal single quotes are
  doubled (`''...''`). The point is assembled with `ST_POINT(lon_col, lat_col)`
  from the source columns (no nested WKT quoting) — preserve that when editing.
- Row count is read back via `RESULT_SCAN(LAST_QUERY_ID())`.

## Performance

- First `PARSE_ADDRESS` call is slow (~30 s) while the shared PyPI `usaddress`
  package installs; subsequent calls are fast.
- Use a MEDIUM (or larger) warehouse for batch jobs over large source tables.
