# Native Maps in Snowsight: Getting Started with Overture Maps

Use this guide to try geographic visualization in Snowsight: query-result maps
in Workspaces, conversational maps in CoWork, and map tiles in Dashboards.
The native maps private preview requires account enablement. Ask your Snowflake
account team to confirm access for the surface you want to test; installing this
sample does not enable the preview.

## Start here

**Have an enabled account?** Start with the small synthetic query in the
[Workspaces walkthrough](docs/map-testing-guide.md#workspaces). It needs no data
loading, semantic view or agent. Then try points, lines, polygons and H3 with the
[read-only SQL examples](docs/examples/map_queries.sql).

**Want a conversational demo?** Use the [one-prompt installation](#install-with-one-prompt)
below, then try the [CoWork walkthrough](docs/map-testing-guide.md#cowork) and
[demo prompts](#questions-to-try). The installed sample is called **Overture Maps
Semantic Layer**; its agent is displayed as **Overture Maps**.

**Building a dashboard?** Follow the
[Dashboard walkthrough and CoCo prompt](docs/map-testing-guide.md#dashboards).
Native maps need their own spec; a Vega-only chart example is not a map template.

**Evaluating before requesting access?** Read the
[preview scope](docs/map-testing-guide.md#preview-access),
[FAQ](docs/map-testing-guide.md#faq) and
[MCP support distinctions](docs/map-testing-guide.md#mcp).
This is a sample testing guide, not a guarantee of feature availability or a
replacement for your preview agreement. UI rendering remains unverified in this
sample's [validation record](docs/validation.md).

The installer reuses compatible Overture tables or Marketplace shares when
available. If you need data, it recommends an acquisition path and can load
Overture from public S3 after approval. Both sources use the same semantic model.

## Install with one prompt

Open **Cortex Code Desktop**, connect to the account where you want the sample,
and paste this prompt. You do not need to clone the repo, edit TOML, or run setup
commands yourself. Cortex Code needs local file access and Python 3.11+; this
agent workflow does not require a separately configured Snowflake CLI connection.

```text
Install Overture Maps Semantic Layer in my connected Snowflake account.
Read and follow the installation skill at:
https://github.com/Snowflake-Labs/sf-samples/blob/overture-maps-semantic-layer/samples/geospatial/overture_maps_semantic_layer/.cortex/skills/install-overture-maps/SKILL.md

Retrieve the sample and its referenced files from the same commit, or use my
existing checkout without overwriting local changes. Discover and reuse compatible
Overture data if available; otherwise recommend a data source. Propose sensible
account settings and ask me to approve the target, audience and costs. Then
perform installation and verification using my connected session. Do not make
me edit configuration files or run setup commands. Finish with the installed
agent, supported example questions, and a clear report of what was verified.
```

The link targets the feature branch while this change is under review. It becomes
usable remotely when that branch includes the skill. For a local checkout, ask:
**"Read `.cortex/skills/install-overture-maps/SKILL.md` in the Overture sample and
install Overture Maps Semantic Layer in my connected account."** After merge,
the published prompt should use the corresponding `main` URL.

### What happens next

1. Cortex Code confirms the connected account and inspects candidate data and
   available settings. Renamed Marketplace databases and loaded Overture tables
   are supported through explicit source mappings.
2. It proposes a destination, role, warehouse, source and audience. You approve
   the changes and costs. It asks about ambiguity or missing access instead of guessing.
3. It prepares configuration, installs the objects and runs checks itself. Any
   S3 loading or Marketplace acquisition requires explicit approval; accepting
   legal terms is not implied by the starting prompt.
4. It returns the agent location, suitable demo questions and verification results.
   SQL, agent answers, viewer access and native maps are reported separately.

One prompt starts the workflow; it does not bypass permission dialogs, legal
consent or cost approvals. If your account lacks required access, Cortex Code
reports the specific administrator action rather than silently escalating roles.
Native map enablement is a separate platform prerequisite that this sample cannot
install. A SQL or API response alone does not prove a map renders in CoWork.

**Validation status:** 45 offline tests pass. An approved isolated live smoke test
checked the deployed SQL objects, search readiness, one real agent answer and the
helper's verification receipt cycle. Full clean-session single-prompt installation,
exact-template deployment/rerun and native maps remain unverified. See the
[validation record](docs/validation.md) for the precise scope.

## What it installs

```text
Existing Overture tables / Marketplace shares / approved S3 load
                              |
                     Normalized local views
                              |
             Semantic view + place-category search
                              |
                     Overture Maps agent
                              |
                   CoWork answers and maps
```

| Object | Purpose |
| --- | --- |
| `PLACE`, `DIVISION`, `DIVISION_AREA`, `DIVISION_BOUNDARY` | Consistent local views over the chosen Overture sources. |
| `OVERTURE_MAPS_SV` | Entity definitions, geographic metrics and reference-query examples for Cortex Analyst. |
| `CATEGORY_VOCAB`, `CATEGORY_SEARCH` | Category lookup so the agent can resolve place types to actual tags. |
| `OVERTURE_MAPS_AGENT` | Agent displayed as **Overture Maps**. |
| `DATASET_INFO` | Recorded release and source coverage. |
| `RAW_*`, `OVERTURE_S3`, `OVERTURE_PARQUET` | Optional S3-loaded data, stage and file format. |

Objects live in an approved dedicated schema. Existing source tables and shares
are not overwritten. All four Overture themes above are required; buildings,
roads and address datasets are outside this sample. See the
[source contract](docs/source-contract.md).

## Questions to try

Select the installed **Overture Maps** agent in CoWork. Start with one of the
prompts the installer verified against your source. These five examples have
[reference SQL and expected columns](examples/prompts.json).
See the [demo checklist](docs/map-testing-guide.md#demo-checklist) for coverage,
field bindings and follow-up questions for each example.

### Place locations

> Show up to 200 hospitals in San Francisco, California, on a map. Include their
> names and use the primary hospital category.

Returns names and coordinate points using address locality. This is not an exact
city-boundary test or a complete census of real-world hospitals.

### County land areas

> Show California county land boundaries on a map, colored by area in square
> kilometres. Include each area's unique ID and name; exclude maritime areas.

Returns county polygons and land areas. Shapes are simplified for display; area
measurements use the original geometry.

### Nearby cafes

> Find the 20 nearest places in primary category cafe within 1 km of longitude
> -122.3937, latitude 37.7955. Map them and show straight-line distance in metres.

Returns cafes ordered by distance from that point. This is not driving distance.

### Reverse geocoding

> Which US county land area contains longitude -122.4194, latitude 37.7749? Show
> its boundary on a map.

Tests exact point-in-polygon containment. It identifies an administrative area,
not a street address.

### Geographic density

> Map the density of places with locality Berlin in Germany using resolution-8
> H3 hexagons, colored by place count. Show the 500 busiest cells.

Requires Berlin coverage. The default S3 demo loads San Francisco places and
global administrative divisions, so the first four examples apply, but Berlin
does not. Counts vary by release; results outside known coverage are not evidence
of real-world absence.

You can adapt locations and categories within your data coverage. Specify country,
region, units and desired output. Follow up with "Return a table instead" or
"Limit this to the 10 nearest results", then inspect the generated query's scope.

## Prerequisites and costs

Use an authenticated Cortex Code session with permission to create the sample
objects, read the chosen sources and use an existing warehouse. The proposed
installer role needs schema creation privileges and appropriate Cortex access.
A separate viewer role needs approved grants. The default current-role audience
may include other users holding that role; it is not necessarily private.

Missing databases, warehouses, roles, storage integrations or account features
require an explicitly approved bootstrap or administrator action. The skill does
not silently create those resources. See the full
[privilege checklist](docs/manual-installation.md#prerequisites).

- Existing-source installation does not copy the source dataset. Building the
  category vocabulary scans place categories and stores the resulting vocabulary.
- S3 loading incurs scan, warehouse and storage costs, and possibly transfer costs.
  Geographic clipping may reduce stored rows without reducing source scan cost.
- Cortex Search indexing/serving, Cortex Analyst and agent inference incur costs.
  Search can continue costing money between demos.
- Timeouts and suggested map-row limits are not an account spending cap or a hard
  geometry-payload limit. Inspect costs before authorizing larger coverage.

## Verification and recovery

The installation report distinguishes object checks, reference SQL, actual agent
answers, viewer access, CoWork registration and native map rendering. If a client
or permission is unavailable, the corresponding check stays **not tested** or
**blocked**. Only a real CoWork map is a successful rendering test.

If interrupted, ask Cortex Code to resume the installation. It keeps non-secret
configuration and receipts in ignored `.install/` files and rechecks live account
state before proceeding. A partial S3 load needs inspection and approved recovery;
it is not automatically discarded or reloaded. Do not run concurrent installers
against the same schema.

Unchanged installations reuse their objects. Changed configuration or deployment
code requires a new schema under the current installer design. Existing agents
are not automatically replaced, preserving their evaluation history. Cleanup is
an explicit destructive operation and is never automatic error recovery.

## Limits and troubleshooting

| Situation | What it means |
| --- | --- |
| SQL works, but no map appears | Check native map availability, agent selection, registration and geographic result columns. |
| The agent is not selectable | Check CoWork registration and viewer permissions in your account. |
| No compatible sources found | Confirm access and the source contract; approve acquisition or loading before proceeding. |
| Several compatible sources found | Choose the intended release/coverage; the agent must not combine unrelated datasets silently. |
| An example returns no rows | Check coverage, category and release. Empty data is not a passed coverage test. |
| Schema ownership/configuration mismatch | Reuse the original files/settings or approve a new destination; do not overwrite markers. |

Exact place-to-area spatial joins and proximity between two datasets are not
modeled. "Cafes per county polygon" and "hospitals near any pharmacy" need another
spatial preparation step. H3 resolution is fixed at 8. Polygon simplification is
100 metres for display only. Map geometry metrics require unique IDs in their
grouping; names alone can attach an arbitrary shape to otherwise correct counts.
Travel routing and real-time opening hours are not included.

## For developers

- [Map testing guide](docs/map-testing-guide.md): preview access, all three surfaces, FAQ and diagnostics.
- [Read-only map queries](docs/examples/map_queries.sql): synthetic rendering probes and scalar-key aggregation.
- [Installation skill](.cortex/skills/install-overture-maps/SKILL.md): discovery, approvals, installation and handoff.
- [Agent protocol](docs/agent-installation.md): offline SQL renderer and receipt format.
- [Manual installation](docs/manual-installation.md): advanced CLI setup, upgrade and cleanup.
- [Source contract](docs/source-contract.md): required columns and geometry semantics.
- [Validation record](docs/validation.md): completed checks and remaining acceptance work.
- [Prompt gallery](examples/prompts.json): questions and reference queries.

Run offline tests from this sample directory with `python3 -m unittest discover -s tests -v`.
The agent and manual CLI paths share installation functions and SQL templates.
DCM is not required. No external map application or container deployment is included.

## Attribution and support

Data comes from [Overture Maps](https://overturemaps.org/). Review its
[attribution and licensing guidance](https://docs.overturemaps.org/attribution/)
and applicable Marketplace listing terms. Preserve required attribution in shared
maps and derived data. This repository distributes code and queries, not Overture data.

This is sample code under the repository's existing license and support terms.
It does not guarantee native map availability or eliminate account setup requirements.