# Testing native maps in Snowsight

Start with a tiny query-result map, then test your own data or the Overture demo.
This separates rendering problems from data access, SQL generation and agent setup.

## Preview access

Guide reviewed: 2026-09-25. Native maps are described here as a private-preview
capability. Confirm current availability and preview terms with your Snowflake
account team before planning a customer demonstration. Requesting access, receiving
approval and having the feature enabled are separate steps. Confirm the intended
account and each surface: Workspaces, CoWork and Dashboards.

You can share this guide to explain the testing workflow before requesting access.
It does not grant access, promise a release date or enable account features.
The account team can supply the appropriate current enrollment route. There are
no customer-run feature-flag commands in this sample.

### Capability and evidence

The table describes what to exercise in an enabled preview account, not a claim
that every cell has been tested by this sample. UI labels may vary by build.

| Map content | Workspaces test | CoWork test | Dashboard test | Sample evidence |
| --- | --- | --- | --- | --- |
| Points | Bind numeric coordinates or a geographic point | Ask to map named places | Bind the same query columns | Overture SQL checked; rendering pending |
| Lines | Use a geographic line result | Ask for a bounded line result from an accessible source | Reuse the line query | Synthetic SQL example; rendering pending |
| Polygons | Bind geographic shapes | Ask for county land boundaries | Bind shapes and an area/color metric | Overture SQL checked; rendering pending |
| H3 | Use string cell IDs and a measure | Ask for resolution-8 density | Bind cell ID and count | Overture SQL checked; rendering pending |

Do not infer support in one surface from success in another. See the dated
[validation record](validation.md) for executed checks and outstanding tests.

## Workspaces

No agent or semantic view is required for this path. Use a warehouse you are
authorized to query. The examples are read-only but query compute can incur cost.

1. Open a SQL file in a Snowsight Workspace.
2. Run **one statement** from [map_queries.sql](examples/map_queries.sql), starting
   with `points`. The examples use inline synthetic data and create no objects.
3. Inspect the result table first: two rows, distinct IDs, numeric coordinates
   near San Francisco. The first point is longitude -122.3937, latitude 37.7955.
4. In result visualization/chart controls, choose **Map** if offered. Select point
   coordinates and bind `LATITUDE` and `LONGITUDE` explicitly. Use `VALUE` for color
   if the control is available. Do not substitute a scatter chart for a map.
5. Confirm both points appear in San Francisco rather than at 0,0. Inspect their
   labels/tooltips where supported. The table and map must refer to the same rows.
6. Try `geography_points`, `lines`, `polygons`, then `h3`. Bind the fields listed
   in each statement's comment. If the build does not offer that mapping option,
   record it as unavailable instead of guessing a spec or column format.

If **Map** is absent, confirm surface enablement with your account team. Do not
install the Overture agent as a workaround for a missing Workspaces picker.

### Use your own data

Start with a filtered area and a small result. Keep stable IDs, labels and only
the fields needed for the map. Numeric coordinates must be non-null and within
latitude [-90, 90] and longitude [-180, 180]. WKT point constructors use
**longitude then latitude**, while functions with LATLNG in their name may take
latitude first: follow each function's documented argument order.

Use geographic coordinates for these examples. A `GEOMETRY` value may use a
projected coordinate system whose numbers are not longitude/latitude. Establish
its source SRID and transform to an appropriate geographic CRS before mapping;
merely relabeling an SRID is not a coordinate transformation. This sample does
not verify arbitrary projected GEOMETRY rendering. See the
[GEOGRAPHY/GEOMETRY reference](https://docs.snowflake.com/en/sql-reference/data-types-geospatial).

When a GeoJSON column is required, use `ST_ASGEOJSON` and retain valid complete
geometry, not truncated text. Keep raw geometry for analysis and create a separate
simplified display shape. These examples use `GEOGRAPHY`, where simplification
tolerance is in metres; do not apply that unit assumption to planar GEOMETRY.

## CoWork

For a ready-made data/model example, use the
[one-prompt Overture installer](../README.md#install-with-one-prompt). It installs
data adapters, a semantic view, category lookup and an agent, not the map feature.
Alternatively, use your existing agent with authorized geographic data access.

1. Confirm native maps are enabled for CoWork in your account and the intended
   agent is visible. Agent registration and permissions are separate from creation.
2. Select **Overture Maps** (or your own configured agent).
3. Choose a [demo prompt](../README.md#questions-to-try) within your source coverage.
   Ask explicitly for a map and specify the location, output and units.
4. Inspect the generated SQL/result where available. Check filters, row limits,
   IDs, coordinates and category selection before judging the map.
5. Confirm a native map artifact actually appears, plots the expected location
   and represents the returned rows. An answer saying "here is a map" is not proof.
6. If you save/share a map, reopen it and verify the content and authorized audience.
   Do not assume a saved snapshot will automatically refresh.

No user-declared `data_to_map` tool needs to be added to the sample's agent spec.
The map capability belongs to the enabled platform/client workflow. An Agent API
answer or Desktop SQL result is not a CoWork rendering test.

### Result references and recovery

A native map needs a successful SQL result with geographic columns. A chart
result, an invented ID or an inaccessible result from another agent/thread is
not interchangeable with that SQL result. If lookup fails, the sample instructions
allow one re-execution of the same authorized, bounded query in the mapping
agent's context, preserving filters and limits. If that cannot recover the result,
return any available table and the blocker. Do not broaden access or silently
switch to a Python plotting workflow.

This guidance mitigates failures; it does not fix platform result handoff or
guarantee that a parent can access a subagent's results. Record SQL success,
map-tool execution, artifact emission and browser rendering separately.

### Conversation acceptance tests

[map_scenarios.json](../tests/map_scenarios.json) contains unexecuted acceptance
cases for points, polygons, default-coverage H3, follow-up filtering, table/chart
to map, unresolved results, logical geometry names and unsupported joins. Start
each case in a fresh test conversation with verified nonempty source coverage.
Use its gallery `prompt_id` and then its follow-ups. Compare generated SQL and
returned IDs/measures to reference results at the same scope and release.

Inspect actual tool traces for result IDs and retry counts, and inspect the
native map in CoWork. Run missing-result cases only in an isolated test harness;
if controlled failure injection is unavailable, mark them not tested. Subagent
handoff needs a separately approved integration setup. Record passed, failed,
blocked or not tested with sanitized evidence in a separate run report. Offline
fixture tests check coverage and references, not agent behavior. Do not publish
private prompts, customer identifiers or account-specific traces in this repo.

## Demo checklist

The exact prompts and reference SQL live in [prompts.json](../examples/prompts.json)
and are presented in the [README](../README.md#questions-to-try). Use those prompts
rather than maintaining separate variants for acceptance testing. `$ns` in the
reference queries means your approved installation's database/schema, not a
standalone Snowflake SQL variable; substitute its quoted identifiers before use.

| Example ID | Coverage needed | Expected map fields | Useful follow-up |
| --- | --- | --- | --- |
| `hospital_points` | San Francisco places | `LATITUDE`, `LONGITUDE`; `ID`, `NAME` labels | Return the same results as a table so I can compare locations. |
| `california_counties` | California county land areas | `GEOJSON`, color `AREA_SQKM`; `ID`, `NAME` | Show only the 10 largest counties by land area. |
| `nearby_cafes` | Places near the supplied SF point | Coordinates, `DISTANCE_M`; `ID`, `NAME` | Limit this to the 10 nearest cafes and state the selected primary category. |
| `reverse_geocode` | County land area containing the SF point | `GEOJSON`; `ID`, `NAME` | Return the county name and ID as a table. |
| `sf_hexagons` | San Francisco places, including the default SF-only load | String `H3_CELL`, color `PLACE_COUNT` | Show the 20 busiest cells as a table, keeping resolution 8. |
| `berlin_hexagons` | Berlin places, not the default SF-only load | String `H3_CELL`, color `PLACE_COUNT` | Show the 20 busiest cells as a table, keeping resolution 8. |

All results depend on installed coverage and release. Primary category matching
is not a census of real businesses. Address locality is not exact city-boundary
containment. Proximity is straight-line distance, not driving distance. Simplified
shapes are for display; area and containment use original geometry. Follow-ups
are suggestions, not separately verified agent responses.

## Safe geographic aggregation

Do not put GEOGRAPHY or GEOMETRY in `GROUP BY`, and avoid `GROUP BY ALL` when the
selected fields include geometry. Converting the whole geometry to text just to
group it is not a good default: aggregate business measures at their intended
grain instead.

The `aggregate_then_join` example in [map_queries.sql](examples/map_queries.sql)
shows the preferred pattern: count observations by a scalar area key, then left
join those counts to a table with one geometry per key. Starting from areas keeps
areas with no observations; `COALESCE` makes their count zero. Check that real
area keys are unique and that observation assignments are valid, or the join may
duplicate measures. A shared name is not necessarily a unique area key.

In a semantic view, keep raw geometry as a fact for spatial predicates rather
than a grouping dimension. The Overture model exposes display geometry through
`ANY_VALUE` metrics. Only use those metrics when the grouping key determines the
geometry: group by `areas.area_id` or `places.place_id`, not just a name.
`ANY_VALUE` does not dissolve several shapes or repair a many-to-many join.

When a dissolved geographic shape is actually wanted, the sample's
`areas.dissolved_geojson` metric uses `ST_UNION_AGG` over original GEOGRAPHY and
then simplifies the result for display. Keep the input spatially bounded. Do not
assume the same aggregate accepts arbitrary GEOMETRY or that simplification
preserves every small island or narrow feature.

Exact places-per-polygon analysis needs an upstream spatial association. Compute
the intended containment/intersection, decide how to handle boundary points and
overlapping areas, and expose scalar equality keys to the semantic model. This
sample has no place-to-area spatial relationship. Address locality and H3 cell
coverage are not substitutes for exact containment.

## Dashboards

**Example status:** this sample does not yet contain a verified native-map `.dash`
file. No working map-tile export or authenticated dashboard test session was
available during preparation. The exact tile envelope and import/save/reopen
behavior remain unverified. Do not use a fabricated JSON spec to fill this gap.

For an enabled preview account, use the UI as the starting point:

1. First verify the small `points` query in Workspaces.
2. Open a test dashboard you are authorized to edit. Add a query-backed tile using
   the same SQL. If the tile editor offers **Map**, choose it and bind `LATITUDE`
   and `LONGITUDE`. If no Map option appears, stop and confirm dashboard enablement.
3. Check both positions, save the dashboard, close it and reopen it. Confirm the
   map and bindings survive. Test refresh separately from viewing a saved snapshot.
4. Where your environment exposes the dashboard file, inspect/export the UI-created
   tile. Remove account-specific references and data before sharing an example.
5. Use that working file as the reference for CoCo-assisted authoring. Keep the
   native map spec distinct from a Vega-Lite chart spec; validate against the
   current dashboard format, not only a Vega-Lite schema.

This is a test procedure, not a record that the UI steps passed on your build.
Publication, viewer grants and deployment to CoWork require their own approval.

### Prompt for CoCo when a dashboard skill rejects maps

Use this in the environment where you edit the dashboard. Attach or point to an
actual UI-created native-map tile if you have one.

```text
I am testing native map tiles in a preview-enabled Snowsight account.
Read docs/map-testing-guide.md and docs/examples/map_queries.sql from this sample.
Use my UI-created working map tile as the format reference if one is available.

Inspect the current dashboard schema and reference before editing. Do not infer
that maps are unsupported merely because a skill only describes Vega-Lite charts.
Do not invent native map fields, disable validation, or replace the map with a
scatter chart. If no compatible reference/schema is available, explain the blocker
and help me create a small map tile through the UI first.

Start with the synthetic points query. Bind the exact returned LATITUDE and
LONGITUDE columns. For aggregated data, group by stable scalar keys and join
geometry afterward; never GROUP BY GEOGRAPHY/GEOMETRY or blindly GROUP BY ALL.
Preserve existing tiles. Ask before publishing or changing sharing permissions.
Verify actual rendering and save/reopen behavior separately from SQL success.
```

This prompt supplies context and acceptance criteria. It cannot enable a missing
feature or make an incompatible dashboard format work.

## MCP

Support assessment: 2026-09-25. These are three different integration questions.
None is established by this sample's SQL-backed agent smoke test.

| Path | What travels through MCP | What still needs confirmation |
| --- | --- | --- |
| MCP rows into CoWork | Geographic tabular tool results consumed by CoWork | Whether this server's result shape is supported by the enabled CoWork build and produces a native map |
| Image response from a server | Encoded image bytes and a MIME type | Whether the particular Snowflake/customer server generates an image and the intended client displays it |
| MCP Apps | A UI resource for an interactive interface in a supporting host | Whether both the specific server and host support the extension, with the required security policies |

The [MCP image-content specification](https://modelcontextprotocol.io/specification/2025-06-18/server/tools#image-content)
defines `type: image`, base64 `data` and `mimeType`. It does not define a native
geospatial layer, automatically turn rows into an image, or promise interactive
pan/zoom. A map image and a native map artifact are not interchangeable.

[MCP Apps](https://modelcontextprotocol.io/extensions/apps/overview) now has published
extension documentation for interactive UI resources. Host support must be checked;
the extension's existence does not establish CoWork or Snowflake MCP server support.
Do not describe it simply as an unspecified future protocol or promise a Snowflake
delivery date based on its documentation.

**Can our Snowflake MCP server render geomaps in another client?** This guide does
not confirm that capability. Identify the exact server, tool response type and
receiving client. Ask your account team for a supported example of that pairing.
CoWork consuming MCP data is the opposite direction from a Snowflake MCP server
returning a visualization to another host. Success in one does not prove the other.

For a map-from-rows test, use a tiny non-sensitive result with explicit column
names, numeric coordinates and string identifiers (including H3 IDs). Inspect
the received rows and actual artifact. Keep unsupported or untested combinations
marked **unconfirmed**, not universally unsupported or generally available.

## FAQ

### What can I share before requesting preview access?

Share this guide's preview scope, synthetic queries and Overture demo prompts.
The account team should confirm terms and current access procedures. General
[geospatial SQL documentation](https://docs.snowflake.com/en/sql-reference/functions-geospatial)
explains analysis functions; it is not native-map preview enrollment or UI documentation.

### Any helpful prompts that showcase the feature?

Start with [hospital points](../README.md#place-locations),
[county polygons](../README.md#county-land-areas) and
[H3 density](../README.md#geographic-density). Add
[nearby cafes](../README.md#nearby-cafes) and
[reverse geocoding](../README.md#reverse-geocoding) to show spatial predicates.
Use the [demo checklist](#demo-checklist) to confirm coverage and field bindings.

### Do I need to bring geographic data?

For real analysis, yes: provide coordinates, geometry or a geographic reference
dataset you are authorized to use. The map renderer does not supply a complete
county, postal-code or street-address dataset. This sample's synthetic examples
test rendering; the optional Overture installation supplies reference places and
administrative geography. Existing customer data can be mapped without Overture.

### Can I map postal codes or place names directly?

Do not assume automatic geocoding. Join to an appropriate licensed reference with
coordinates or boundaries. Keep postal codes as strings to preserve leading zeros,
and include country to avoid collisions. A representative point is not a postal
boundary; US Census ZCTAs are not identical to postal delivery ZIP codes. Choose
the geography appropriate to the question. This sample does not include a postal
code lookup or validate a third-party geocoding service.

### Why does CoCo say the dashboard spec cannot contain maps?

A skill's examples may cover only conventional charts or a different release.
Use the [dashboard procedure](#dashboards) to check the actual enabled UI and a
working native-map tile. Give CoCo that reference rather than asking it to ignore
schema errors. This sample does not yet provide a verified `.dash` artifact.

### Why does generated SQL group by GEOGRAPHY?

The assistant may have treated geometry as a category/dimension or applied
`GROUP BY ALL`. Use the [scalar-key aggregation pattern](#safe-geographic-aggregation)
and include that constraint in the authoring prompt. Check the result grain after
rewriting; merely making the query compile does not establish correct counts.

### Why does a map work in Workspaces but not CoWork?

Workspaces renders a selected query result; CoWork also depends on agent data
access, generated SQL, map generation and artifact presentation. Confirm preview
access for both surfaces. A working result map does not validate agent routing or
registration. Conversely, an agent answering correctly does not validate rendering.

## Troubleshooting

| Symptom | Checks and safe next step |
| --- | --- |
| Map option missing | Confirm the intended account/surface is enabled. An enrollment submission is not an enablement confirmation. |
| All points at 0,0 | Inspect the original table values, nulls, numeric types, column bindings/casing and coordinate order. Compare the tiny synthetic example. If inputs are correct but positions are wrong, report a rendering issue; casting alone is not a proven fix. |
| Some polygons missing | Compare returned IDs with expected IDs. Check filters, inner joins, null/invalid geometry, result limits, complete GeoJSON and geometry payload size. Reduce the area or simplify display shapes, then compare again. Do not assume payload size is the cause without evidence. |
| Areas with zero observations missing | Aggregate facts first and left join from the unique area table. Preserve unmatched areas and decide whether zero or unknown is semantically correct. |
| H3 map blank or misplaced | Use string H3 IDs at one declared resolution. Do not send 64-bit IDs as floating-point numbers or infer a resolution different from the query. |
| Geometry shown as text, not a map | Check map selection/binding and output format. GeoJSON needs a complete valid geometry; WKT text is not a GeoJSON column. |
| Answer says map, but no artifact appears | Check for an actual map output in the client/trace if available. Record SQL success and missing rendering separately. |
| Map reports no result found or wrong result type | Check that the referenced ID belongs to successful SQL, not a chart. Try the bounded recovery described above once; report unresolved cross-agent handoff through the approved support channel. |
| Invalid identifier or JSON access on GEOGRAPHY | Generated semantic SQL must use the model's logical names, such as `geometry` and `area_class`. Physical reference SQL uses `GEOM` and `CLASS`. Use exposed coordinates and string H3 fields; GEOGRAPHY is not a JSON object. |
| Saved map differs from initial view | Compare IDs/row counts after reopening and refreshing. Saved snapshots and query-backed refresh may have different behavior. |

Start small to isolate the issue; then increase scope gradually. Row count alone
does not measure geometry payload size. The sample's suggested row limit is not
a platform guarantee that every shape fits. Small display features can disappear
under simplification, so compare with original geometry when that matters.

### Report an issue

Send diagnostics through your account team's approved support channel, not a
public GitHub issue containing customer data. Include:

- Surface, account identification through the approved channel, timestamp and build/version if visible.
- Sanitized prompt/SQL, query ID and relevant errors or trace details.
- Returned row count, column names/types, geometry representation and coordinate ranges.
- Expected geography versus displayed geography, with a sanitized screenshot.
- Whether the synthetic probe reproduces it, and whether another enabled surface works.
- For dashboards, whether initial view, save/reopen or refresh fails; for MCP, the exact server/client and result content type.

Do not attach credentials, private source rows or unredacted customer identifiers.