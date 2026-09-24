# Overture Maps Semantic Layer

Turn Overture Maps data into a conversational geographic dataset in your own
Snowflake account. Install a semantic view and a Cortex Agent, then ask questions
such as "Where are the hospitals in San Francisco?" or "Which county contains
this coordinate?" The agent can return tables, distances, counts and map-ready
results. Native maps are displayed in Snowflake CoWork where that feature is enabled.

This sample is for solution engineers preparing demos and customers exploring
Overture places and administrative geography. It works with Overture data already
loaded from S3, CARTO's Overture Marketplace shares, or a fresh load using the
included S3 loader. The source changes; the semantic model and agent stay the same.

**Before starting:** native map rendering must be available in your account.
Installing this sample does not enable that platform feature. Confirm availability
with your Snowflake account team. You can still test queries and agent answers
without maps, but that does not validate the visualization experience.

**Validation status:** reference-query patterns and local installer tests have
passed. Full installation, Marketplace viewer access and native CoWork rendering
still require acceptance testing. See the [validation record](docs/validation.md).

- [What it installs](#what-it-installs)
- [Questions it can help answer](#questions-it-can-help-answer)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Using the agent](#using-the-agent)
- [Verify your installation](#verify-your-installation)
- [Troubleshooting](#troubleshooting)
- [Cost and operation](#cost-and-operation)
- [Upgrade and cleanup](#upgrade-and-cleanup)

## What it installs

A semantic view describes the dataset's entities, relationships, dimensions and
metrics so Cortex Analyst can translate a question into SQL. The agent combines
that model with a searchable place-category vocabulary to avoid guessing tags
such as `hospital`, `cafe` or `coffee_shop`.

```text
Existing Overture tables or Marketplace shares
                    |
              Local source views <--- Optional S3 loader
                    |
          OVERTURE_MAPS_SV + CATEGORY_SEARCH
                    |
            OVERTURE_MAPS_AGENT
                    |
       CoWork answers and native maps
```

All created objects live in the database and dedicated schema you configure.

| Object | Purpose |
| --- | --- |
| `PLACE`, `DIVISION`, `DIVISION_AREA`, `DIVISION_BOUNDARY` | Local views exposing a consistent set of columns over the chosen sources. |
| `OVERTURE_MAPS_SV` | Semantic view for geographic queries, with five reference-query examples. |
| `CATEGORY_VOCAB`, `CATEGORY_SEARCH` | Installation-time place-category vocabulary and its Cortex Search service. |
| `OVERTURE_MAPS_AGENT` | Agent displayed as **Overture Maps**, using Analyst and category search. |
| `DATASET_INFO` | Recorded source mode, release and geographic coverage. |
| `RAW_*`, `OVERTURE_S3`, `OVERTURE_PARQUET` | Tables, stage and file format created only by the optional S3 loader. |

The sample uses places, administrative label points, area polygons and boundary
lines. It does not load buildings, roads or address datasets. No separate web app,
container deployment, external geocoding API or customer map-provider key is
configured by this sample.

## Questions it can help answer

Use these five prompts for a first demonstration. Their reference SQL is included
in [examples/prompts.json](examples/prompts.json). The SQL patterns were tested;
the agent responses and native maps still need verification in your account.

### Find places on a map

> Show up to 200 hospitals in San Francisco, California, on a map. Include their
> names and use the primary hospital category.

Expected output: hospital names and coordinate points. This uses the address
locality "San Francisco", not an exact city-boundary containment test. A primary
category count describes Overture records, not a complete census of hospitals.

### Compare county land areas

> Show California county land boundaries on a map, colored by area in square
> kilometres. Include each area's unique ID and name; exclude maritime areas.

Expected output: a polygon map colored by land area. Display shapes are simplified
by 100 metres; area measurements use the original geometry. Separate land and
maritime records must not be added together as land area.

### Find nearby places

> Find the 20 nearest places in primary category cafe within 1 km of longitude
> -122.3937, latitude 37.7955. Map them and show straight-line distance in metres.

Expected output: up to 20 cafes ordered by distance from that coordinate. Distances
are geographic straight-line distances, not driving or walking routes.

### Identify the area containing a coordinate

> Which US county land area contains longitude -122.4194, latitude 37.7749? Show
> its boundary on a map.

Expected output: the matching county name and polygon, using exact point-in-polygon
containment. This is reverse geocoding to an administrative area, not a street address.

### Find concentrations of places

> Map the density of places with locality Berlin in Germany using resolution-8
> H3 hexagons, colored by place count. Show the 500 busiest cells.

Expected output: the 500 busiest H3 cells, colored by record count. H3 groups nearby
points into geographic cells. This example needs global places data or coverage
of Berlin; it will not work with the default San Francisco-only S3 places load.

You can adapt these questions to other locations and categories covered by your
source. For example, ask for the most common place categories in a city or the
largest county land areas in a state. These variations are not additional tested
examples. Specify the country and region when a name is ambiguous.

### Questions outside this sample's scope

- "How many cafes are inside each county polygon?" requires an exact spatial
  relationship between the places and areas datasets, which this model does not define.
- "Which hospitals are within 500 metres of any pharmacy?" requires a spatial
  join between two sets of places, rather than a search around one coordinate.
- "What is the fastest driving route?" requires routing data and a routing service.
- "Which locations are open right now?" requires current business information.
  Overture releases are snapshots, and this sample does not expose opening hours.

Do not substitute H3 coverage or an address locality for an exact polygon test.
Counts and available categories depend on the installed release and coverage.

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

## Installation

### 1. Get the sample

Clone the repository and enter the sample directory:

```bash
git clone https://github.com/Snowflake-Labs/sf-samples.git
cd sf-samples
```

If you are evaluating the sample before its PR is merged, check out its branch:

```bash
git switch --track origin/overture-maps-semantic-layer
```

Then enter the directory. Run all subsequent commands from here:

```bash
cd samples/geospatial/overture_maps_semantic_layer
python3 --version
snow --version
```

If you already have the repository, use your existing checkout instead of cloning
again. Configure a named connection using the
[Snowflake CLI connection guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections).

### 2. Set account and installation options

Create a local configuration from [config.example.toml](config.example.toml):

```bash
cp config.example.toml config.toml
```

Edit the top-level values in `config.toml` for your account:

```toml
connection = "my_connection"
role = "MY_INSTALLER_ROLE"
warehouse = "MY_WAREHOUSE"
database = "MY_DATABASE"
schema = "OVERTURE_SEMANTIC_V1"
viewer_role = "MY_VIEWER_ROLE"
```

`role` runs the installation; `viewer_role` receives access to use the installed
objects. The database, warehouse and both roles must already exist.
Use a new schema such as `OVERTURE_SEMANTIC_V1`. Do not use a shared application
schema. Names must be ordinary unquoted Snowflake identifiers; the runner quotes
and uppercases each component. Quoted mixed-case identifiers are not supported.
Credentials stay in your Snowflake CLI configuration, never in this file.

### 3. Connect your data and install

Choose the path that matches your data. Once the data is ready, both paths run the
same `install` command and create the same semantic layer.

#### Existing tables or Marketplace shares

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

#### Fresh S3 load

Use this path if you do not already have compatible Overture tables. Edit the
existing `[source]` and `[s3]` sections in `config.toml`, rather than adding duplicate
sections:

```toml
[source]
mode = "s3"
release = "unknown"
coverage = "Recorded automatically by the S3 loader."

[s3]
release = "latest"
storage_integration = ""
place_bbox = [-122.52, 37.70, -122.35, 37.83]
```

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

## Using the agent

After installation, locate `<database>.<schema>.OVERTURE_MAPS_AGENT` in Snowsight
and use your account's supported CoWork agent-selection or registration controls
to expose it to users. Select **Overture Maps** explicitly for the first tests.
UI availability and registration permissions may differ by account.

1. Sign in as a user with the configured viewer role and required Cortex access.
2. Open CoWork and select **Overture Maps**. If it is not listed, confirm agent
   registration and permissions with your account administrator.
3. Start with the hospital or California county prompt above. Both fit the default
   S3 coverage: San Francisco places plus global administrative divisions.
4. Inspect the result table and generated SQL alongside the answer. Check the
   selected category, geographic filters, units and any row limit.
5. Try a follow-up such as "Limit this to the 10 nearest results" or "Return a
   table instead." Recheck the scope if you change the location or category.

Map requests need geographic columns in the results: latitude/longitude for
points, an H3 cell ID for hexagons, or GeoJSON for polygons. A country or city name
alone is not a map geometry. If native maps are unavailable, the agent should
return the data and explain the limitation.

The sample configures Cortex Analyst and category search. It does not declare a
custom `data_to_map` tool: the client/platform supplies native map rendering.
Do not add internal account parameters to this sample to work around availability.

## Verify your installation

```bash
python3 -m unittest discover -s tests -v
python3 scripts/install.py verify --config config.toml
python3 scripts/install.py verify-prompts --config config.toml
```

`verify` checks normalized views, one semantic query, the agent's existence and
search readiness. `verify-prompts` runs the five reference queries and checks
their columns. Neither tests the agent's answers or the map renderer. An empty
result is reported as empty, not passed as proof of data coverage.

To check viewer access, make a separate local copy of your configuration and
change `role` to the actual viewer role. Run only `verify` and `verify-prompts`
with that copy; keep the original configuration for installation and cleanup.
The runner disables secondary roles, so these checks use the configured role's
permissions rather than incidental access from other roles.

Run the prompt gallery in CoWork as that viewer. Check that points are in the expected city,
hexagons use resolution 8, polygons have the right names and units, and the
response has a real map rather than a table or bar chart. Compare the agent's
SQL/results to the reference query at the same scope and release.

See [docs/validation.md](docs/validation.md) for checks completed during sample
development and the remaining installation/rendering acceptance tests.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| A source table is missing or unauthorized | Check the four fully qualified source names and the installer role's source access. Marketplace database names can differ from the example. |
| Preflight rejects a column or geometry | Compare your source with the [source contract](docs/source-contract.md). The adapter requires native GEOGRAPHY and the supported Overture nested layout. |
| Berlin returns no results | The default S3 load contains only San Francisco places. Use a Berlin bounding box or global places in a new installation. |
| SQL works but no map appears | Confirm native map availability, the selected agent and the result's geographic columns. A successful API response is not a renderer test. |
| The agent is not visible in CoWork | Check viewer access and the account's agent registration/selection controls. Creating an agent object alone may not make it selectable. |
| The schema is not owned by this configuration/version | Use the original configuration and sample files, or choose a new schema. Do not overwrite another installation's marker. |
| S3 loading stops before completion | Inspect retained candidate tables and the error. Clean up the disposable installation with its original configuration before retrying, or choose a new schema. |
| Category search is not ready | Inspect the service in Snowsight and resolve its indexing, warehouse or privilege error before retrying verification. |

Preflight checks source metadata and samples. It does not establish all deployment
privileges or validate every source row; a later creation or grant can still fail.

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