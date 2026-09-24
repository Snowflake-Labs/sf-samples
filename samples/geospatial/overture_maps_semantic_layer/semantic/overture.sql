create semantic view if not exists $ns.OVERTURE_MAPS_SV
tables (
    places as $ns.PLACE primary key (ID)
        comment = 'Overture POIs. One point per ID. Coverage is recorded in DATASET_INFO.',
    areas as $ns.DIVISION_AREA primary key (ID)
        comment = 'Administrative polygons. Land and maritime have separate IDs.',
    divisions as $ns.DIVISION primary key (ID)
        comment = 'Administrative labels as points, not containment polygons.',
    boundaries as $ns.DIVISION_BOUNDARY primary key (ID)
        comment = 'Border line segments. Filter IS_LAND for land-only length.'
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
    boundaries.length_km as LENGTH_KM
)
dimensions (
    places.place_id as ID,
    places.name as NAME,
    places.category as CATEGORY
        comment = 'Exact primary category tag. Resolve with category_vocabulary first.',
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
    boundaries.is_land as IS_LAND
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
    boundaries.total_length_km as sum(boundaries.LENGTH_KM)
)
comment = $owner_comment
ai_sql_generation 'Use original geometry for predicates and simplified GeoJSON only for display.
Group map geometry by area_id and point coordinates by place_id, never name alone.
Filter land area with area_class = land. Place region is CA, area region is US-CA.
The five examples are reference queries; counts depend on installed release and coverage.
Use coordinate literals, not semantic variables. There is no place-to-area spatial relationship.'
$verified_queries;