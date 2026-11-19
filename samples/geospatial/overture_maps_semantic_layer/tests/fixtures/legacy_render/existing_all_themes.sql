create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1' as
select id::varchar as id, geometry as geom,
       names:primary::varchar as name,
       coalesce(taxonomy:primary::varchar, basic_category::varchar) as category,
       basic_category::varchar as basic_category,
       addresses[0]:country::varchar as country,
       addresses[0]:region::varchar as region,
       addresses[0]:locality::varchar as locality,
       st_y(geometry)::float as latitude, st_x(geometry)::float as longitude,
       h3_point_to_cell_string(geometry, 8) as h3_cell
from "OVERTURE_MAPS__PLACES"."CARTO"."PLACE";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1' as
select id::varchar as id, geometry as geom, names:primary::varchar as name,
       country::varchar as country, region::varchar as region,
       subtype::varchar as subtype, population::number as population
from "OVERTURE_MAPS__DIVISIONS"."CARTO"."DIVISION";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_AREA comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1' as
select id::varchar as id, division_id::varchar as division_id, geometry as geom,
       names:primary::varchar as name, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype, class::varchar as class,
       st_area(geometry) / 1e6 as area_sqkm,
       st_asgeojson(st_simplify(geometry, 100))::varchar as geojson
from "OVERTURE_MAPS__DIVISIONS"."CARTO"."DIVISION_AREA";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_BOUNDARY comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1' as
select id::varchar as id, geometry as geom, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype,
       class::varchar as class, is_land::boolean as is_land,
       st_length(geometry) / 1000 as length_km
from "OVERTURE_MAPS__DIVISIONS"."CARTO"."DIVISION_BOUNDARY";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".ADDRESS comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1' as
select id::varchar as id, geometry as geom,
       street::varchar as street, number::varchar as house_number,
       postcode::varchar as postcode,
       coalesce(postal_city::varchar,
                address_levels[array_size(address_levels) - 1]:value::varchar) as city,
       country::varchar as country,
       st_y(geometry)::float as latitude, st_x(geometry)::float as longitude,
       bbox:xmin::float as bbox_xmin, bbox:xmax::float as bbox_xmax,
       bbox:ymin::float as bbox_ymin, bbox:ymax::float as bbox_ymax
from "OVERTURE_MAPS__ADDRESSES"."CARTO"."ADDRESS";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".SEGMENT comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1' as
select id::varchar as id, geometry as geom,
       names:primary::varchar as name,
       subtype::varchar as subtype, class::varchar as class,
       subclass::varchar as subclass,
       iff(speed_limits[0]:between is null and speed_limits[0]:when is null,
           speed_limits[0]:max_speed:value::float
             * iff(speed_limits[0]:max_speed:unit::varchar = 'mph', 1.609344, 1),
           null) as speed_limit_kmh,
       st_length(geometry) / 1000 as length_km,
       st_asgeojson(geometry)::varchar as geojson,
       bbox:xmin::float as bbox_xmin, bbox:xmax::float as bbox_xmax,
       bbox:ymin::float as bbox_ymin, bbox:ymax::float as bbox_ymax
from "OVERTURE_MAPS__TRANSPORTATION"."CARTO"."SEGMENT";

create or replace view "MY_DATABASE"."OVERTURE_SEMANTIC".BUILDING comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1' as
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
from "OVERTURE_MAPS__BUILDINGS"."CARTO"."BUILDING";

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
    addresses as "MY_DATABASE"."OVERTURE_SEMANTIC".ADDRESS primary key (ID)
        comment = 'Overture address points. Very large: filter all four BBOX columns first.',
    segments as "MY_DATABASE"."OVERTURE_SEMANTIC".SEGMENT primary key (ID)
        comment = 'Overture road, rail and water segments. Not a routing graph. Very large: filter BBOX columns first.',
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
    addresses.geometry as GEOM
        comment = 'Point GEOGRAPHY. Apply only after the BBOX filters.',
    addresses.latitude as LATITUDE,
    addresses.longitude as LONGITUDE,
    addresses.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    addresses.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    addresses.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    addresses.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.',
    segments.geometry as GEOM
        comment = 'LineString GEOGRAPHY. Apply only after the BBOX filters.',
    segments.length_km as LENGTH_KM,
    segments.speed_limit_kmh as SPEED_LIMIT_KMH
        comment = 'First unconditional posted maximum in km/h; NULL when absent or conditional.',
    segments.bbox_xmin as BBOX_XMIN comment = 'Required filter. Minimum longitude.',
    segments.bbox_xmax as BBOX_XMAX comment = 'Required filter. Maximum longitude.',
    segments.bbox_ymin as BBOX_YMIN comment = 'Required filter. Minimum latitude.',
    segments.bbox_ymax as BBOX_YMAX comment = 'Required filter. Maximum latitude.',
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
    addresses.address_id as ID,
    addresses.street as STREET comment = 'Street name as published. Match with ILIKE.',
    addresses.house_number as HOUSE_NUMBER,
    addresses.postcode as POSTCODE,
    addresses.city as CITY comment = 'Postal city, else the most local address level.',
    addresses.country as COUNTRY comment = 'ISO 3166-1 alpha-2 code, e.g. US.',
    segments.segment_id as ID comment = 'Include this ID when returning one line per row.',
    segments.name as NAME comment = 'Primary street or route name. Match with ILIKE.',
    segments.subtype as SUBTYPE comment = 'road, rail or water.',
    segments.road_class as CLASS
        comment = 'motorway, trunk, primary, secondary, tertiary, residential, service, footway...',
    segments.subclass as SUBCLASS comment = 'Refines class, e.g. sidewalk or link.',
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
    addresses.address_count as count(addresses.ID),
    addresses.map_latitude as any_value(addresses.LATITUDE)
        comment = 'Use only with address_id; otherwise returns an arbitrary point.',
    addresses.map_longitude as any_value(addresses.LONGITUDE)
        comment = 'Use only with address_id; otherwise returns an arbitrary point.',
    segments.segment_count as count(segments.ID),
    segments.total_length_km as sum(segments.LENGTH_KM),
    segments.avg_speed_limit_kmh as avg(segments.SPEED_LIMIT_KMH)
        comment = 'Only segments with an unconditional posted limit; state that coverage.',
    segments.geojson as any_value(segments.GEOJSON)
        comment = 'LineString map shape. Group by segment_id, never name alone.',
    buildings.building_count as count(buildings.ID),
    buildings.total_footprint_sqm as sum(buildings.FOOTPRINT_SQM),
    buildings.max_height_m as max(buildings.HEIGHT_M),
    buildings.geojson as any_value(buildings.GEOJSON)
        comment = 'Footprint map shape. Group by building_id, never name alone.'
)
comment = 'overture-maps-semantic-layer:1.1.0:caa2230ef3ccbbc1'
ai_sql_generation 'Use original geometry for predicates and simplified GeoJSON only for display.
Use exposed logical names in semantic SQL: geometry, area_class and place_id,
not physical GEOM, CLASS or ID from reference queries against normalized views.
Geometry is GEOGRAPHY, not JSON; do not use GET_PATH or coordinates[] on it.
Reuse exposed latitude, longitude and resolution-8 string h3_cell fields.
Do not GROUP BY geometry or use GROUP BY ALL when selecting geometry.
Group map geometry by area_id and point coordinates by place_id, never name alone.
Filter land area with area_class = land. Place region is CA, area region is US-CA.
The examples are reference queries; counts depend on installed release and coverage.
Use coordinate literals, not semantic variables. There is no place-to-area spatial relationship. Tables ADDRESS, SEGMENT, BUILDING are global and very large. Every query on them must filter bbox_xmin, bbox_xmax, bbox_ymin and bbox_ymax with literal coordinates spanning a small area before any geometry predicate; a geometry predicate alone scans the whole table. If no area is given, ask for one. Their height, floors and subtype fields are often NULL, so never present a NULL-heavy count as absence.'
ai_verified_queries (
hospital_points as (question 'Show up to 200 hospitals in San Francisco, California, on a map. Include their names and use the primary hospital category.' sql 'select id, name, latitude, longitude from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where country = ''US'' and region = ''CA'' and locality = ''San Francisco'' and category = ''hospital'' order by id limit 200'),
sf_hexagons as (question 'Map the density of places with locality San Francisco in California, US, using resolution-8 H3 hexagons, colored by place count. Show the 500 busiest cells.' sql 'select h3_cell, count(*) as place_count from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where country = ''US'' and region = ''CA'' and locality = ''San Francisco'' group by h3_cell order by place_count desc, h3_cell limit 500'),
berlin_hexagons as (question 'Map the density of places with locality Berlin in Germany using resolution-8 H3 hexagons, colored by place count. Show the 500 busiest cells.' sql 'select h3_cell, count(*) as place_count from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where country = ''DE'' and locality = ''Berlin'' group by h3_cell order by place_count desc, h3_cell limit 500'),
california_counties as (question 'Show California county land boundaries on a map, colored by area in square kilometres. Include each area''s unique ID and name; exclude maritime areas.' sql 'select id, name, geojson, area_sqkm from "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_AREA where country = ''US'' and region = ''US-CA'' and subtype = ''county'' and class = ''land'' order by id limit 200'),
nearby_cafes as (question 'Find the 20 nearest places in primary category cafe within 1 km of longitude -122.3937, latitude 37.7955. Map them and show straight-line distance in metres.' sql 'select id, name, latitude, longitude, st_distance(geom, to_geography(''POINT(-122.3937 37.7955)'')) as distance_m from "MY_DATABASE"."OVERTURE_SEMANTIC".PLACE where category = ''cafe'' and st_dwithin(geom, to_geography(''POINT(-122.3937 37.7955)''), 1000) order by distance_m, id limit 20'),
reverse_geocode as (question 'Which US county land area contains longitude -122.4194, latitude 37.7749? Show its boundary on a map.' sql 'select id, name, geojson from "MY_DATABASE"."OVERTURE_SEMANTIC".DIVISION_AREA where country = ''US'' and subtype = ''county'' and class = ''land'' and st_contains(geom, to_geography(''POINT(-122.4194 37.7749)'')) order by id limit 10'),
market_street_addresses as (question 'Show up to 500 addresses on Market Street in downtown San Francisco, between longitude -122.42 and -122.39 and latitude 37.77 and 37.80, on a map.' sql 'select id, street, house_number, latitude, longitude from "MY_DATABASE"."OVERTURE_SEMANTIC".ADDRESS where bbox_xmin >= -122.42 and bbox_xmax <= -122.39 and bbox_ymin >= 37.77 and bbox_ymax <= 37.80 and street ilike ''market st%'' order by id limit 500'),
downtown_major_roads as (question 'Map the motorway, trunk and primary road segments in downtown San Francisco, between longitude -122.42 and -122.39 and latitude 37.77 and 37.80, with their length in kilometres.' sql 'select id, name, class, geojson, length_km from "MY_DATABASE"."OVERTURE_SEMANTIC".SEGMENT where bbox_xmin >= -122.42 and bbox_xmax <= -122.39 and bbox_ymin >= 37.77 and bbox_ymax <= 37.80 and subtype = ''road'' and class in (''motorway'', ''trunk'', ''primary'') order by id limit 500'),
tallest_buildings as (question 'What are the 10 tallest buildings in downtown San Francisco, between longitude -122.41 and -122.39 and latitude 37.78 and 37.80? Show their footprints on a map.' sql 'select id, name, geojson, height_m from "MY_DATABASE"."OVERTURE_SEMANTIC".BUILDING where bbox_xmin >= -122.41 and bbox_xmax <= -122.39 and bbox_ymin >= 37.78 and bbox_ymax <= 37.80 and height_m is not null order by height_m desc, id limit 10')
);
