# Overture Maps Semantic Layer

Ask questions about Overture places and administrative areas with Cortex Analyst
and a Cortex Agent. Return coordinates, H3 cells and GeoJSON for native maps in
Snowflake CoWork, where that feature is available.

Use existing Overture tables, CARTO Marketplace shares, or the included S3 loader.
All three paths feed the same local views and semantic model. No separate app,
containers, external geocoding API or customer map-provider key is required by this sample.

**Native map availability is a separate account prerequisite.** Installing SQL
objects does not enable the map renderer. Check availability with your Snowflake
account team. Agent API responses alone do not prove that CoWork can render a map.

## Try these prompts

Select the installed **Overture Maps** agent in CoWork, then try:

1. "Show up to 200 hospitals in San Francisco, California, on a map. Include their names and use the primary hospital category."
2. "Show California county land boundaries on a map, colored by area in square kilometres. Include each area's unique ID and name; exclude maritime areas."
3. "Find the 20 nearest places in primary category cafe within 1 km of longitude -122.3937, latitude 37.7955. Map them and show straight-line distance in metres."
4. "Which US county land area contains longitude -122.4194, latitude 37.7749? Show its boundary on a map."

With global places data or a load covering Berlin:

> Map the density of places with locality Berlin in Germany using resolution-8 H3
> hexagons, colored by place count. Show the 500 busiest cells.

[examples/prompts.json](examples/prompts.json) contains the questions, required
coverage, map types, output columns, caveats and reference SQL. These queries are
also included as verified-query examples in the semantic view. Counts vary by
release. Do not substitute a saved reference count for a query against your data.

## Prerequisites

- Python 3.11+ and [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)
  3.16+ with a configured named connection. The Python runner has no package dependencies.
- An existing database and warehouse. The runner creates a dedicated sample schema;
  it does not create a database, warehouse, role or user.
- The installation role needs CREATE SCHEMA on the target database, source data
  access, warehouse USAGE and the ability to create views, tables, a semantic view,
  Cortex Search service and agent in the new schema. For S3 it also creates a
  file format and external stage. Managed-access schemas are not used by this sample.
- A security administrator must provision appropriate Cortex database roles for
  the installer and viewers. See [agent access control](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
  and [search creation privileges](https://docs.snowflake.com/en/sql-reference/sql/create-cortex-search).
  The installer needs embedding privileges for category search. It does not grant
  account-wide Cortex database roles itself.
- An existing viewer role. The installer grants it access to the sample views,
  semantic view, category search and agent, plus database/schema/warehouse USAGE.
  The installer must be authorized to make those grants; an administrator may
  need to grant warehouse/database USAGE beforehand.

Use a primary installation role that can use the configured warehouse without
depending on secondary roles. Preserve its access to source tables for view
resolution. For imported shares, have an administrator grant the appropriate
shared database role or imported privileges to that role. Validate actual viewer
access; do not assume that successful queries as the installer establish it.

## Configure

Run commands from this sample directory. Create a local configuration from
[config.example.toml](config.example.toml):

```bash
cp config.example.toml config.toml
```

Set `connection`, `role`, `warehouse`, `database`, `schema` and `viewer_role`.
Use a new schema such as `OVERTURE_SEMANTIC_V1`. Do not use a shared application
schema. Names must be ordinary unquoted Snowflake identifiers; the runner quotes
and uppercases each component. Quoted mixed-case identifiers are not supported.
Credentials stay in your Snowflake CLI configuration, never in this file.

### Existing tables or Marketplace shares

Leave `source.mode = "existing"`. Set the four source names explicitly. The
example uses CARTO's conventional database names; renamed shares work too.

For data you previously loaded from S3, change only the mappings, for example:

```toml
[source]
mode = "existing"
release = "unknown"
coverage = "Places cover San Francisco; administrative divisions are global."
place = "MY_DATA.OVERTURE.PLACE"
division = "MY_DATA.OVERTURE.DIVISION"
division_area = "MY_DATA.OVERTURE.DIVISION_AREA"
division_boundary = "MY_DATA.OVERTURE.DIVISION_BOUNDARY"
```

Acquire the [CARTO Places listing](https://app.snowflake.com/marketplace/listing/GZT0Z4CM1E9KR)
and [CARTO Divisions listing](https://app.snowflake.com/marketplace/listing/GZT0Z4CM1E9M9)
yourself if needed. The sample never accepts legal terms or acquires a listing
automatically. Availability and import permissions depend on the account.

All four tables are required in v1. Missing tables, empty themes, WKB in place of
GEOGRAPHY, or unsupported nested layouts fail preflight. The contract is documented
in [docs/source-contract.md](docs/source-contract.md). This is an Overture adapter,
not a generic schema-inference tool.

```bash
python3 scripts/install.py preflight --config config.toml
python3 scripts/install.py render --config config.toml
python3 scripts/install.py install --config config.toml --execute
```

`render` prints reviewable SQL without connecting or changing files. For an
existing source, you can run that SQL in Snowsight **only in a new empty schema**
after checking source compatibility. The CLI path adds ownership/collision checks
and checks search readiness. Rendered SQL does not include those safeguards.

### Fresh S3 load

Set `source.mode = "s3"`; source object names are then derived as `RAW_*` tables
inside the sample schema. Set `s3.release = "latest"` or an available explicit
release. `latest` selects the newest release with files for all four required
types; this is a presence check, not proof that an upstream publication is complete.
Compare with [Overture's release catalog](https://stac.overturemaps.org/catalog.json)
before a production load. Old releases can disappear from the public bucket.

`s3.place_bbox` is `[west, south, east, north]`, with San Francisco as the default.
Use `[]` to load global places. Divisions are always loaded globally. Longitude
comes first and bounding boxes crossing the antimeridian are not supported.
The loader records the actual release and coverage in `DATASET_INFO`; it ignores
the manually entered source release/coverage for loaded data.

```bash
python3 scripts/install.py load-s3 --config config.toml --execute
python3 scripts/install.py preflight --config config.toml
python3 scripts/install.py install --config config.toml --execute
```

The bucket is public, but your account may require a storage integration for
external stages. If so, set `s3.storage_integration` to an administrator-provisioned
integration allowed to access `s3://overturemaps-us-west-2/`.

Loading uses `BINARY_AS_TEXT = FALSE` and `TRY_TO_GEOGRAPHY` for WKB. Each candidate
table must be non-empty and have valid geometry and unique, non-null IDs before
any candidate is renamed to `RAW_*`. A failed or interrupted load retains its
candidate tables for inspection. It does not publish semantic objects. Resume by
cleaning up this disposable installation and reloading, or use a new schema.
Do not run concurrent installers against the same target schema.

## Open the agent

After installation, locate `<database>.<schema>.OVERTURE_MAPS_AGENT` in Snowsight
and use your account's supported CoWork agent-selection or registration controls
to expose it to users. Select **Overture Maps** explicitly for the first tests.
UI availability and registration permissions may differ by account.

The sample configures Cortex Analyst and category search. It does not declare a
custom `data_to_map` tool: the client/platform supplies native map rendering.
Do not add internal account parameters to this sample to work around availability.

## Verify

```bash
python3 -m unittest discover -s tests -v
python3 scripts/install.py verify --config config.toml
python3 scripts/install.py verify-prompts --config config.toml
```

`verify` checks normalized views, one semantic query, the agent's existence and
search readiness. `verify-prompts` runs the five reference queries and checks
their columns. Neither tests the agent's answers or the map renderer. An empty
result is reported as empty, not passed as proof of data coverage.

Repeat verification with a connection/role representing the actual viewer, then
run the prompt gallery in CoWork. Check that points are in the expected city,
hexagons use resolution 8, polygons have the right names and units, and the
response has a real map rather than a table or bar chart. Compare the agent's
SQL/results to the reference query at the same scope and release.

See [docs/validation.md](docs/validation.md) for checks completed during sample
development and the remaining installation/rendering acceptance tests.

## Cost and operation

- Existing-source mode creates views without copying source tables. Creating the
  category vocabulary scans the places source once and stores category counts.
- S3 loading reads the selected Overture types. A bounding-box filter limits rows
  written but may not reduce source scan cost. Global places contain tens of
  millions of rows. Cross-region/cloud reads may incur transfer charges.
- Warehouse queries, agent inference, Cortex Analyst, and category-search indexing
  and serving incur charges. Search can incur ongoing cost even between demos.
  Stop/delete unused services according to your account's operating policy.
- The runner uses your existing warehouse and a configurable per-statement timeout.
  These are not an account spending cap. Map row limits are instructions to the
  agent, not an enforceable query or payload-size limit.
- The category vocabulary is an installation-time snapshot. Existing-source views
  can see newer provider data while that vocabulary and recorded release metadata
  stay unchanged. Reinstall into a new schema to refresh the whole sample together.

## Upgrade and cleanup

The installer uses `IF NOT EXISTS` and a schema/object marker derived from the
configuration and shipped files. Rerunning unchanged installation does not replace
the agent or erase its evaluation history. A changed configuration or sample
version requires a **new schema**. Test that installation, then update users'
agent selection. Keep the old installation until its history is no longer needed.

To delete one disposable installation, use its original configuration and sample
version:

```bash
python3 scripts/install.py cleanup --config config.toml --execute
```

Cleanup checks ownership markers, drops only listed sample objects and tagged
load candidates, and drops the schema with `RESTRICT`. It refuses recognized
object kinds with unknown names/comments. It never drops source databases,
Marketplace shares, external source tables, roles or warehouses. It deliberately
does not revoke database/warehouse USAGE because other applications may use it.
**Cleanup deletes this agent's evaluation history and any S3 data loaded by the
sample.** Export history first if needed. Do not add unrelated objects to this
sample-owned schema. Markers prevent accidental collisions; they are not a
security boundary against a role that can change object metadata.

## Limits

Only places and administrative divisions are included. Roads, buildings and
addresses are outside this sample. Point-to-literal distance and containment work;
exact place-to-area joins are not modeled. H3 coverage must not be presented as
exact containment. H3 resolution is fixed at 8. Polygon simplification is fixed
at 100 metres for display; all analytical predicates use original geometry.

`ANY_VALUE` map metrics require their unique place/area ID in the grouping.
Grouping by a non-unique name can attach an arbitrary shape to otherwise correct
counts. Use the dissolved metric for supported coarser polygon groups. Geographic
filtering and row limits still do not guarantee a small polygon payload.

## Files

```text
config.example.toml       Account settings and source mappings
scripts/install.py       Preflight, render, load, install, verify and cleanup
sql/                     Source normalization, optional S3 load, category search
semantic/overture.sql    Semantic view and generated verified-query attachment
agent/instructions.md    Agent behavior and map-output conventions
examples/prompts.json    Prompt gallery and reference SQL
tests/                   Offline installer and rendering tests
docs/                    Source contract and validation record
```

## Attribution and support

Data is from [Overture Maps](https://overturemaps.org/). Review the
[Overture attribution and licensing guidance](https://docs.overturemaps.org/attribution/)
for the release and themes you use, and the applicable Marketplace listing terms.
This repository contains installation code and queries, not a redistributed copy
of Overture data. Preserve required attribution when sharing maps or derived data.

This is sample code under the repository's existing license and support terms.
It is not a managed service or a guarantee of native map availability in every
account. Review costs and permissions before deployment.