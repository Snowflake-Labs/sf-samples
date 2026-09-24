create view if not exists $ns.PLACE comment = $owner_comment as
select id::varchar as id, geometry as geom,
       names:primary::varchar as name, categories:primary::varchar as category,
       addresses[0]:country::varchar as country,
       addresses[0]:region::varchar as region,
       addresses[0]:locality::varchar as locality,
       st_y(geometry)::float as latitude, st_x(geometry)::float as longitude,
       h3_point_to_cell_string(geometry, 8) as h3_cell
from $source_place;

create view if not exists $ns.DIVISION comment = $owner_comment as
select id::varchar as id, geometry as geom, names:primary::varchar as name,
       country::varchar as country, region::varchar as region,
       subtype::varchar as subtype, population::number as population
from $source_division;

create view if not exists $ns.DIVISION_AREA comment = $owner_comment as
select id::varchar as id, division_id::varchar as division_id, geometry as geom,
       names:primary::varchar as name, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype, class::varchar as class,
       st_area(geometry) / 1e6 as area_sqkm,
       st_asgeojson(st_simplify(geometry, 100))::varchar as geojson
from $source_division_area;

create view if not exists $ns.DIVISION_BOUNDARY comment = $owner_comment as
select id::varchar as id, geometry as geom, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype,
       class::varchar as class, is_land::boolean as is_land,
       st_length(geometry) / 1000 as length_km
from $source_division_boundary;