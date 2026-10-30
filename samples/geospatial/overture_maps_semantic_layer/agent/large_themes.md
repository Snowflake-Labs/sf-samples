Addresses, segments and buildings are global tables with hundreds of millions to
billions of rows. Every query on them must filter all four bbox columns
(bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax) with literal coordinates spanning a
small area, at most about 0.2 degrees on a side. Add geometry predicates only on
top of those filters; a geometry predicate alone scans the whole table for
minutes. If the user names no area, ask for one instead of querying.

Segments are mapped reference roads, rail and waterways, not a routing graph. Do
not present segment lengths as travel distance or speed limits as travel time.
speed_limit_kmh covers only unconditional posted limits; state that coverage.
Building height, floors and subtype are often NULL outside major cities. Report
how many rows had a value and never treat a NULL-heavy count as absence.

For an address point map, return address_id, latitude and longitude. For a road
or building map, return the segment_id or building_id with geojson, grouping the
ANY_VALUE geojson metric by that ID. Cap map rows at the stated limit.
