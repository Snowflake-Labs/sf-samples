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

## Agent-driven installation checks

The expanded suite has 42 passing offline tests. Added coverage includes source
ambiguity, renamed source databases, config without a CLI connection, account/role/
warehouse mismatches, approval checks, SQL string splitting, receipt integrity and
a simulated full existing-source installation through the shared workflow. No SQL
is executed by the offline helper. The identity and scoped source-discovery queries
were also executed successfully as read-only checks.

The 13 cases in [installation_scenarios.json](../tests/installation_scenarios.json)
are acceptance scenarios, not recorded successful agent conversations. Run them
in clean sessions, capture transcripts and check their expected behaviors before
claiming single-prompt installation is verified. In particular, test a session
without Snowflake CLI and a resumed conversation with stale receipt history.

The remote bootstrap prompt becomes usable only once its referenced branch
includes the skill and helper files.

## Isolated live smoke test

On 2026-09-24, an explicitly approved isolated installation reused the existing
snapshot without S3 loading or changes to the shared deployment. The primary
installer role and approved warehouse were checked through the session SQL tool;
secondary roles were disabled. Four normalized views, dataset metadata, a semantic
view with five reference queries, category vocabulary/search and an agent were
created. Objects were retained, including the search service and its ongoing costs.

- Source metadata and 100 geometry samples per theme passed the contract checks.
- Category Search reported ACTIVE indexing and serving with 1,984 categories.
- Semantic point output returned 150 hospital rows with valid coordinates.
- Semantic H3 output returned 500 Berlin cells containing 114,818 places.
- Semantic county output returned 58 polygons, 409,643.737 square kilometres and
  726,570 bytes of simplified shapes.
- Installed-view radius and containment queries returned 20 cafes at distances
  of 147.998 to 533.612 metres and one containing county, respectively.
- A real Agent API invocation resolved `hospital` through category search, then
  generated and executed the correctly scoped count query. Its answer of 150
  matched reference SQL and identified the snapshot release. This verifies one
  count question, not the complete prompt gallery or any native map.
- The helper's `verify` command completed all seven emitted statements through
  the authenticated SQL tool, with actual results recorded in an ignored receipt.

This was a smoke test, not full single-prompt acceptance. The creation phase used
direct SQL-tool calls rather than a complete emitted-step receipt cycle. The live
semantic view has the modeled entities, metrics and reference queries, but omitted
the template's per-field/table comments and used equivalent unquoted names in
reference SQL. Its deployment marker therefore is not proof of exact template
parity. Do not use this test to claim the shipped template was installed verbatim.
The Agent API test used the explicitly selected connection to the same account;
it does not establish a separate viewer role's access or secondary-role isolation
inside that API session.

No authenticated CoWork browser session was available for rendering checks.
Marketplace acquisition, fresh S3 loading and cleanup were not authorized or run.

## Map testing guide checks (2026-09-25)

The full offline suite now has 45 passing tests, including read-only example
structure, local documentation links and required guide sections. `git diff
--check` passes. The deployment fingerprint is unchanged from the live smoke test.

All six statements in [map_queries.sql](examples/map_queries.sql) executed
successfully through the authenticated SQL tool using only synthetic inline data:

| Example | SQL result | Agent test | Native UI test |
| --- | --- | --- | --- |
| `points` | Two numeric coordinate rows near San Francisco | Not applicable | Not tested |
| `geography_points` | Two GEOGRAPHY point rows | Not applicable | Not tested |
| `lines` | One complete GeoJSON LineString | Not applicable | Not tested |
| `polygons` | One complete GeoJSON Polygon | Not applicable | Not tested |
| `h3` | Two string cells with counts 1 and 2 | Not applicable | Not tested |
| `aggregate_then_join` | Two areas retained, counts 2 and 0 | Not applicable | Not tested |

The new SQL and guide live under `docs/` because the installer fingerprints files
directly under `examples/`. No installer templates, prompt specifications or live
objects were changed for this documentation work. The five original prompt
definitions remain the source of truth; suggested follow-ups are not recorded
successful agent tests.

No browser tabs were available for an authenticated rendering check. No matching
native-map dashboard artifact was found in the searched local samples. A verified
`.dash` example is therefore deferred rather than invented. The guide provides a
UI-first test procedure and authoring prompt, not a claim of UI success.

MCP image content and MCP Apps descriptions were checked against their linked
protocol documentation. Snowflake server/client support for those paths, and
CoWork maps from a specific MCP result format, remain unconfirmed here.

Before publication, obtain a dated product-owner review of preview scope and UI
wording, validate the public links on the published revision, and complete the
relevant surface tests. Internal rollout details and customer identifiers are not
part of the guide.

## Acceptance still required

- Complete exact-template installation and unchanged rerun using the receipt
  protocol in a clean session, including the original semantic metadata comments.
- S3 stage/load execution, invalid geometry rejection, interrupted-load recovery
  and cleanup. The presence-based release resolver needs an upstream completeness check.
- Actual Marketplace-backed installation and viewer access through local views.
- Separate viewer-role access without accidental secondary-role privileges.
- Agent category lookup, generated SQL and answer equivalence for all gallery prompts.
- Native point, H3 and GeoJSON rendering in CoWork, including correct coordinates.
- Synthetic point, line, polygon and H3 rendering in Workspaces and Dashboards;
  confirm current native dashboard schema and save/reopen/refresh behavior before
  shipping a `.dash` example. Arbitrary projected GEOMETRY rendering is unverified.
- Cleanup without affecting source objects; upgrade to a new schema without losing
  the old agent's history.

Do not label the sample end-to-end verified until these checks have passed in the
intended customer environment. A successful SQL query is not a rendering test.