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

## Review fixes and optional themes (2026-10-25)

Read-only checks against the CARTO Marketplace shares imported in account
tib85385. PLACE has TAXONOMY and BASIC_CATEGORY and no CATEGORIES column, so
the previous place view and taxonomy probe would have failed preflight on a
Marketplace source. `taxonomy:primary` holds the leaf tag (for example
`skin_care_and_makeup`). The new PLACE, ADDRESS, SEGMENT and BUILDING view
bodies compiled against those shares.

The three new reference queries were run with the view expressions inlined over
the share tables, all inside downtown San Francisco boxes. The address box held
71,133 addresses, 1,778 of them on streets matching `market st%`. The road box
held 7,595 segments, 113 of them motorway, trunk or primary. The building box
held 2,562 footprints, 2,340 with a height and a maximum of 326 m. These counts
come from one snapshot.

The suite now has 70 passing offline tests. New coverage includes optional-theme
rendering into one semantic view, prompt and grant gating by installed theme,
bbox clipping of optional S3 loads, `allow_invalid` geometry conversion, STAC
release parsing and its offline failure, the pinned release in agent replays,
fail-closed source choice and LineString map validation.

Not verified: an S3 load of any optional theme, agent answers that keep the bbox
filters, and native line and footprint rendering in CoWork.

## Single installation, upgraded in place (2026-10-25)

In tib85385 the older installation schemas and the TAXONOMY-to-CATEGORIES
compatibility view were dropped, and the sample was installed fresh into
`OVERTURE_MAPS.OVERTURE_SEMANTIC` from the Marketplace shares with all three
optional themes, as in a new account. The first live run found that the optional
theme fragments kept an unsubstituted schema placeholder; that is fixed and
covered by a test.

Install was then rerun over that schema, built by an earlier revision, without
dropping it. The schema and all nine of its tables and views moved to the new
marker in place, one semantic view and one agent remained in the account, and
all nine reference queries passed: 156 hospitals, 368 SF and 500 Berlin cells,
58 counties, 20 cafes, 1 containing county, 500 Market Street addresses, 113
major road segments and 10 tallest buildings. In an earlier install of the same
model, Cortex Analyst, asked for the tallest buildings in a downtown box,
generated SQL with all four bbox filters.
The suite has 75 passing offline tests, including upgrade over an older marker,
refusal of foreign schemas and objects, and pruning of removed theme views.

## CoWork registration (2026-10-25)

The agent had been created but never added to the account's Snowflake CoWork
object, so it appeared in Agent Studio and not in CoWork. Install now adds it to
that object after every agent replace, grants the viewer USAGE on the object,
and fails verification unless the agent is listed. On tib85385, two consecutive
installs both completed. Afterwards `SHOW AGENTS IN SNOWFLAKE INTELLIGENCE
SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT` listed
`OVERTURE_MAPS.OVERTURE_SEMANTIC.OVERTURE_MAPS_AGENT` exactly once, and the four
agents already registered there were unchanged. All nine reference queries
passed again. The suite has 80 passing offline tests. Whether the agent appears
and answers in the CoWork UI was not checked from this session.

## CoWork object never created; live install, rerun, resume and cleanup (2026-10-28)

Review found that the previous revision created `SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT`
when an account had none. Once that object exists CoWork lists only the agents
added to it, so every other agent in such an account would disappear for every
user. Install now adds the agent only to an existing CoWork object and otherwise
registers nothing; `verify` and the agent receipt report `passed` or
`not required`. The suite has 81 passing offline tests. Against the previous
`install.py`, four of the new or rewritten tests fail and one errors, so they
detect the regression.

On tib85385 (ACCOUNTADMIN, MEDIUM, Marketplace shares with all three optional
themes), in two new schemas and leaving `OVERTURE_SEMANTIC` untouched. The
account's CoWork object was created on 2026-08-31, before this sample, and held
five agents throughout as a baseline.

- Install into `OVERTURE_SEMANTIC_E2E` with `install.py`: completed. The CoWork
  list went from five to six agents; the new agent appeared once and the other
  five were unchanged. All nine reference queries passed (156, 368, 500, 58, 20,
  1, 500, 113 and 10 rows).
- Rerun over the same schema: completed. One schema, one semantic view and one
  agent remained; the replaced agent was listed in CoWork once.
- Interrupted install through the agent helper into `OVERTURE_SEMANTIC_E2R`. A
  driver executed each emitted statement with the sample's own CLI client and
  saved the receipt after each one, then stopped after six mutations (schema,
  `DATASET_INFO` and four views). A new process resumed from the receipt at
  statement 39 and completed all 61 statements with `cowork_registration:
  "passed"`. All nine reference queries passed on the result.
- Cleanup of both schemas: completed. Both schemas and their search services were
  dropped, the CoWork list returned to the original five agents, and the CoWork
  object and `OVERTURE_SEMANTIC` were unchanged.

Not verified live: the no-object branch, which needs an account without a CoWork
object (tib85385 has one and it was not dropped), so it is covered offline only.
Also not verified: a separate viewer role (viewer equalled installer, so no
grants ran), an S3 load, agent answers and native map rendering in CoWork.

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

## Map friction checks (2026-09-25)

The updated suite has 60 passing offline tests. Added checks cover the default
SF H3 prompt, rendered agent configuration, every-row map result validation and
successful and failed receipt replay of the H3 validation query. Negative cases
include missing columns, duplicate/null IDs, text or nonfinite coordinates and measures, out-of-range
coordinates, incomplete or wrong-type GeoJSON, numeric H3 IDs and failed H3
validity/resolution results. Polygon checks validate structure, not topology.
`git diff --check` passes.

A read-only Snowflake query over three synthetic strings checked the H3 SQL:
one valid resolution-8 cell, one valid resolution-9 cell and one invalid string.
It returned CHECKED = 3 and INVALID = 2. No source table scan or deployment was
performed. The new SF reference query has not been run against installed data.

The ten cases in [map_scenarios.json](../tests/map_scenarios.json) are acceptance
specifications, not executed agent conversations. Offline tests validate their
gallery references and coverage only. Updated instructions, generated semantic
SQL, multi-turn recovery, subagent handoff and native rendering still require
tests in an approved isolated installation and enabled CoWork session.

Unlike the earlier documentation-only update, these instruction, prompt and
installer changes alter the deployment fingerprint. Rerunning install upgrades the
same schema in place. The earlier smoke test does not verify this revision.

## Acceptance still required

- Complete exact-template installation and unchanged rerun from a single prompt
  in a clean session, including the original semantic metadata comments. The receipt protocol, including resume after interruption,
  passed on 2026-10-28 through a scripted driver, not a recorded agent session.
- S3 stage/load execution, invalid geometry rejection, interrupted-load recovery
  and cleanup. The presence-based release resolver needs an upstream completeness check.
- CoWork registration in an account without a CoWork object (offline only).
- Separate viewer-role access without accidental secondary-role privileges.
- Agent category lookup, generated SQL and answer equivalence for all gallery prompts.
- Native point, H3 and GeoJSON rendering in CoWork, including correct coordinates.
- Synthetic point, line, polygon and H3 rendering in Workspaces and Dashboards;
  confirm current native dashboard schema and save/reopen/refresh behavior before
  shipping a `.dash` example. Arbitrary projected GEOMETRY rendering is unverified.

Cleanup of an existing-source installation without affecting source objects or
other CoWork agents passed on 2026-10-28; cleanup after an S3 load is still open.

Do not label the sample end-to-end verified until these checks have passed in the
intended customer environment. A successful SQL query is not a rendering test.