create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE comment = 'overture-maps-semantic-layer:1.1.0:088bf494136a3f0c' as
select id::varchar as id, geometry as geom,
       names:primary::varchar as name,
       coalesce(taxonomy:primary::varchar, basic_category::varchar) as category,
       basic_category::varchar as basic_category,
       addresses[0]:country::varchar as country,
       addresses[0]:region::varchar as region,
       addresses[0]:locality::varchar as locality,
       st_y(geometry)::float as latitude, st_x(geometry)::float as longitude,
       h3_point_to_cell_string(geometry, 8) as h3_cell
from "MY_DATABASE"."OVERTURE_SEMANTIC"."RAW_PLACE";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION comment = 'overture-maps-semantic-layer:1.1.0:088bf494136a3f0c' as
select id::varchar as id, geometry as geom, names:primary::varchar as name,
       country::varchar as country, region::varchar as region,
       subtype::varchar as subtype, population::number as population
from "MY_DATABASE"."OVERTURE_SEMANTIC"."RAW_DIVISION";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_AREA comment = 'overture-maps-semantic-layer:1.1.0:088bf494136a3f0c' as
select id::varchar as id, division_id::varchar as division_id, geometry as geom,
       names:primary::varchar as name, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype, class::varchar as class,
       st_area(geometry) / 1e6 as area_sqkm,
       st_asgeojson(st_simplify(geometry, 100))::varchar as geojson
from "MY_DATABASE"."OVERTURE_SEMANTIC"."RAW_DIVISION_AREA";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_BOUNDARY comment = 'overture-maps-semantic-layer:1.1.0:088bf494136a3f0c' as
select id::varchar as id, geometry as geom, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype,
       class::varchar as class, is_land::boolean as is_land,
       st_length(geometry) / 1000 as length_km
from "MY_DATABASE"."OVERTURE_SEMANTIC"."RAW_DIVISION_BOUNDARY";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".BUILDING comment = 'overture-maps-semantic-layer:1.1.0:088bf494136a3f0c' as
select id::varchar as id, geometry as geom,
       names:primary::varchar as name,
       subtype::varchar as subtype, class::varchar as class,
       height::float as height_m, num_floors::number as num_floors,
       st_area(geometry)::float as footprint_sqm,
       st_y(st_centroid(geometry))::float as latitude,
       st_x(st_centroid(geometry))::float as longitude,
       st_asgeojson(geometry)::varchar as geojson,
       bbox:xmin::float as bbox_xmin, bbox:xmax::float as bbox_xmax,
       bbox:ymin::float as bbox_ymin, bbox:ymax::float as bbox_ymax
from "MY_DATABASE"."OVERTURE_SEMANTIC"."RAW_BUILDING";

create or replace semantic view "MY_DATABASE"."OVERTURE_SEMANTIC".OVERTURE_MAPS_SV
tables (
    places as "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE primary key (ID)
        comment = 'Overture POIs. One point per ID. Coverage is recorded in DATASET_INFO.',
    areas as "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_AREA primary key (ID)
        comment = 'Administrative polygons. Land and maritime have separate IDs.',
    divisions as "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION primary key (ID)
        comment = 'Administrative labels as points, not containment polygons.',
    boundaries as "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_BOUNDARY primary key (ID)
        comment = 'Border line segments. Filter IS_LAND for land-only length.',
    buildings as "MY_DATABASE"."OVERTURE_SEMANTIC".BUILDING primary key (ID)
        comment = 'Overture building footprints. Largest theme: filter all four BBOX columns first.'
)
relationships (
    area_to_division as areas(DIVISION_ID) references divisions(ID)
)
facts (
    places.geometry as GEOM
        comment = 'Point GEOGRAPHY. Use literal coordinates in ST_DWITHIN/ST_DISTANCE.',
    places.latitude as LATITUDE,
    places.longitude as LONGITUDE,
    areas.geometry as GEOM
        comment = 'Exact GEOGRAPHY for predicates. Return simplified GeoJSON for maps.',
    areas.area_sqkm as AREA_SQKM,
    divisions.population_value as POPULATION,
    boundaries.geometry as GEOM,
    boundaries.length_km as LENGTH_KM,
    buildings.geometry as GEOM
        comment = 'Polygon GEOGRAPHY. Apply only after the BBOX filters.',
    buildings.height_m as HEIGHT_M comment = 'Often NULL outside major cities.',
    buildings.num_floors as NUM_FLOORS comment = 'Often NULL.',
    buildings.footprint_sqm as FOOTPRINT_SQM,
    buildings.latitude as LATITUDE comment = 'Footprint centroid latitude.',
    buildings.longitude as LONGITUDE comment = 'Footprint centroid longitude.',
    buildings.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    buildings.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    buildings.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    buildings.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.'
)
dimensions (
    places.place_id as ID,
    places.name as NAME,
    places.category as CATEGORY
        comment = 'Exact Overture taxonomy primary tag. Resolve with category_vocabulary first.',
    places.basic_category as BASIC_CATEGORY
        comment = 'Coarser Overture basic category; use when the taxonomy tag is too specific.',
    places.country as COUNTRY,
    places.region as REGION comment = 'Bare code, e.g. CA. Not US-CA.',
    places.locality as LOCALITY comment = 'Address locality, not exact polygon containment.',
    places.h3_cell as H3_CELL comment = 'Hexadecimal H3 index at fixed resolution 8.',
    areas.area_id as ID comment = 'Include this ID when returning one polygon per row.',
    areas.name as NAME,
    areas.country as COUNTRY,
    areas.region as REGION comment = 'ISO region, e.g. US-CA. Not CA.',
    areas.subtype as SUBTYPE comment = 'Use subtype for hierarchy; not sparse admin_level.',
    areas.area_class as CLASS comment = 'Filter land for land area and county map examples.',
    divisions.division_id as ID,
    divisions.name as NAME,
    divisions.country as COUNTRY,
    divisions.region as REGION,
    divisions.subtype as SUBTYPE,
    boundaries.boundary_id as ID,
    boundaries.country as COUNTRY,
    boundaries.region as REGION,
    boundaries.subtype as SUBTYPE,
    boundaries.boundary_class as CLASS,
    boundaries.is_land as IS_LAND,
    buildings.building_id as ID comment = 'Include this ID when returning one footprint per row.',
    buildings.name as NAME,
    buildings.subtype as SUBTYPE
        comment = 'residential, commercial, industrial... Mostly NULL; never treat NULL as absence.',
    buildings.building_class as CLASS
)
metrics (
    places.place_count as count(places.ID),
    places.map_latitude as any_value(places.LATITUDE)
        comment = 'Use only with place_id; otherwise returns an arbitrary point.',
    places.map_longitude as any_value(places.LONGITUDE)
        comment = 'Use only with place_id; otherwise returns an arbitrary point.',
    areas.area_count as count(areas.ID),
    areas.total_area_sqkm as sum(areas.AREA_SQKM),
    areas.geojson as any_value(areas.GEOJSON)
        comment = 'Map shape simplified by 100 metres. Group by area_id, never name alone.',
    areas.dissolved_geojson as
        st_asgeojson(st_simplify(st_union_agg(areas.GEOM), 100))::varchar
        comment = 'Dissolved map shape for small, filtered groups coarser than area_id.',
    divisions.division_count as count(divisions.ID),
    divisions.total_population as sum(divisions.POPULATION)
        comment = 'Incomplete population coverage; not a complete census total.',
    boundaries.total_length_km as sum(boundaries.LENGTH_KM),
    buildings.building_count as count(buildings.ID),
    buildings.total_footprint_sqm as sum(buildings.FOOTPRINT_SQM),
    buildings.max_height_m as max(buildings.HEIGHT_M),
    buildings.geojson as any_value(buildings.GEOJSON)
        comment = 'Footprint map shape. Group by building_id, never name alone.'
)
comment = 'overture-maps-semantic-layer:1.1.0:088bf494136a3f0c'
ai_sql_generation 'Use original geometry for predicates and simplified GeoJSON only for display.
Use exposed logical names in semantic SQL: geometry, area_class and place_id,
not physical GEOM, CLASS or ID from reference queries against normalized views.
Geometry is GEOGRAPHY, not JSON; do not use GET_PATH or coordinates[] on it.
Reuse exposed latitude, longitude and resolution-8 string h3_cell fields.
Do not GROUP BY geometry or use GROUP BY ALL when selecting geometry.
Group map geometry by area_id and point coordinates by place_id, never name alone.
Filter land area with area_class = land. Place region is CA, area region is US-CA.
The examples are reference queries; counts depend on installed release and coverage.
Use coordinate literals, not semantic variables. There is no place-to-area spatial relationship. Tables BUILDING are global and very large. Every query on them must filter bbox_xmin, bbox_xmax, bbox_ymin and bbox_ymax with literal coordinates spanning a small area before any geometry predicate; a geometry predicate alone scans the whole table. If no area is given, ask for one. Their height, floors and subtype fields are often NULL, so never present a NULL-heavy count as absence.'
ai_verified_queries (
hospital_points as (question 'Show up to 200 hospitals in San Francisco, California, on a map. Include their names and use the primary hospital category.' sql 'select id, name, latitude, longitude from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where country = ''US'' and region = ''CA'' and locality = ''San Francisco'' and category = ''hospital'' order by id limit 200'),
sf_hexagons as (question 'Map the density of places with locality San Francisco in California, US, using resolution-8 H3 hexagons, colored by place count. Show the 500 busiest cells.' sql 'select h3_cell, count(*) as place_count from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where country = ''US'' and region = ''CA'' and locality = ''San Francisco'' group by h3_cell order by place_count desc, h3_cell limit 500'),
berlin_hexagons as (question 'Map the density of places with locality Berlin in Germany using resolution-8 H3 hexagons, colored by place count. Show the 500 busiest cells.' sql 'select h3_cell, count(*) as place_count from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where country = ''DE'' and locality = ''Berlin'' group by h3_cell order by place_count desc, h3_cell limit 500'),
california_counties as (question 'Show California county land boundaries on a map, colored by area in square kilometres. Include each area''s unique ID and name; exclude maritime areas.' sql 'select id, name, geojson, area_sqkm from "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_AREA where country = ''US'' and region = ''US-CA'' and subtype = ''county'' and class = ''land'' order by id limit 200'),
nearby_cafes as (question 'Find the 20 nearest places in primary category cafe within 1 km of longitude -122.3937, latitude 37.7955. Map them and show straight-line distance in metres.' sql 'select id, name, latitude, longitude, st_distance(geom, to_geography(''POINT(-122.3937 37.7955)'')) as distance_m from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where category = ''cafe'' and st_dwithin(geom, to_geography(''POINT(-122.3937 37.7955)''), 1000) order by distance_m, id limit 20'),
reverse_geocode as (question 'Which US county land area contains longitude -122.4194, latitude 37.7749? Show its boundary on a map.' sql 'select id, name, geojson from "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_AREA where country = ''US'' and subtype = ''county'' and class = ''land'' and st_contains(geom, to_geography(''POINT(-122.4194 37.7749)'')) order by id limit 10'),
tallest_buildings as (question 'What are the 10 tallest buildings in downtown San Francisco, between longitude -122.41 and -122.39 and latitude 37.78 and 37.80? Show their footprints on a map.' sql 'select id, name, geojson, height_m from "MY_DATABASE"."OVERTURE_SEMANTIC".BUILDING where bbox_xmin >= -122.41 and bbox_xmax <= -122.39 and bbox_ymin >= 37.78 and bbox_ymax <= 37.80 and height_m is not null order by height_m desc, id limit 10')
);
