# Validation record

## Development checks

On 2026-09-24, the five reference-query patterns were executed read-only against
an existing Overture 2026-08-19.0 snapshot. Normalization was expressed as CTEs
over those source tables because this sample had not been deployed. No account
objects were created or replaced for these checks.

- Hospital points: 150 rows, all latitude/longitude pairs in range.
- Berlin density: 500 busiest resolution-8 cells containing 114,818 places.
- California land counties: 58 unique area IDs, 409,643.737 square kilometres,
  726,570 bytes of simplified GeoJSON in total.
- Nearest cafes: 20 rows; straight-line distances from 147.998 to 533.612 metres.
- Reverse geocoding: San Francisco county; simplified shape 3,130 bytes.

These are observations from one snapshot, not assertions for newer releases.
The division and boundary normalization projections also compiled against the
existing source tables. The 23 offline tests cover SQL generation, identifier and
literal escaping, configuration validation, ownership guards, unchanged reruns,
and successful/failed S3 publication using mocked CLI responses. They do not
substitute for running the installer against Snowflake.

## Acceptance still required

- Complete fresh installation and unchanged rerun in an isolated test schema.
- Native semantic-view DDL over the new normalized views, including attached queries.
- S3 stage/load execution, invalid geometry rejection, interrupted-load recovery
  and cleanup. The presence-based release resolver needs an upstream completeness check.
- Actual Marketplace-backed installation and viewer access through local views.
- Installer role and viewer role without accidental secondary-role privileges.
- Agent category lookup, generated SQL and answer equivalence for each prompt.
- Native point, H3 and GeoJSON rendering in CoWork, including correct coordinates.
- Cleanup without affecting source objects; upgrade to a new schema without losing
  the old agent's history.

Do not label the sample end-to-end verified until these checks have passed in the
intended customer environment. A successful SQL query is not a rendering test.