-- Run one statement at a time. All data below is synthetic; no objects are created.
-- See ../map-testing-guide.md for field binding and per-surface verification.

-- example: points
-- Bind LATITUDE/LONGITUDE; color VALUE. Two points near San Francisco.
select column1::varchar as id, column2::varchar as name,
       column3::float as latitude, column4::float as longitude,
       column5::number as value
from values
    ('a', 'Demo east', 37.7955, -122.3937, 10),
    ('b', 'Demo west', 37.7955, -122.4037, 20)
order by id;

-- example: geography_points
-- Bind GEOM as geographic points. Same positions as the coordinate example.
select column1::varchar as id, column2::varchar as name,
       to_geography(column3::varchar) as geom
from values
    ('a', 'Demo east', 'POINT(-122.3937 37.7955)'),
    ('b', 'Demo west', 'POINT(-122.4037 37.7955)')
order by id;

-- example: lines
-- Bind GEOJSON as geometry. Synthetic line, not a road or navigable route.
select 'line-a' as id, 'Synthetic segment' as name,
       st_asgeojson(to_geography(
           'LINESTRING(-122.4037 37.7955,-122.3937 37.7955)'
       ))::varchar as geojson;

-- example: polygons
-- Bind GEOJSON; color VALUE. Synthetic square, not an administrative boundary.
select 'area-a' as id, 'Synthetic area' as name, 10 as value,
       st_asgeojson(to_geography(
           'POLYGON((-122.41 37.79,-122.40 37.79,-122.40 37.80,-122.41 37.80,-122.41 37.79))'
       ))::varchar as geojson;

-- example: h3
-- Bind string H3_CELL; color POINT_COUNT. Sum of counts must be 3.
with points as (
    select to_geography(column1::varchar) as geom
    from values ('POINT(-122.3937 37.7955)'),
                ('POINT(-122.3937 37.7955)'),
                ('POINT(-122.4137 37.7855)')
)
select h3_point_to_cell_string(geom, 8) as h3_cell, count(*) as point_count
from points
group by h3_point_to_cell_string(geom, 8)
order by h3_cell;

-- example: aggregate_then_join
-- Bind GEOJSON; color OBSERVATION_COUNT. Area a = 2, area b = 0.
-- Scalar keys are aggregated before a left join to unique area geometries.
-- Real data requires checking key uniqueness and exact spatial assignments first.
with areas as (
    select column1::varchar as area_id, to_geography(column2::varchar) as geom
    from values
      ('a', 'POLYGON((-122.41 37.79,-122.40 37.79,-122.40 37.80,-122.41 37.80,-122.41 37.79))'),
      ('b', 'POLYGON((-122.40 37.79,-122.39 37.79,-122.39 37.80,-122.40 37.80,-122.40 37.79))')
), observations as (
    select column1::varchar as observation_id, column2::varchar as area_id
    from values ('obs-1', 'a'), ('obs-2', 'a')
), counts as (
    select area_id, count(*) as observation_count
    from observations
    group by area_id
)
select areas.area_id, st_asgeojson(areas.geom)::varchar as geojson,
       coalesce(counts.observation_count, 0) as observation_count
from areas
left join counts on areas.area_id = counts.area_id
order by areas.area_id;

-- Overture examples: ../../examples/prompts.json holds the reference queries.
-- Replace $ns there with your quoted installation database/schema, for example
-- "MY_DATABASE"."OVERTURE_SEMANTIC_V1". Do not run the JSON as SQL.
-- Keep the queries' coverage filters, stable IDs and limits.