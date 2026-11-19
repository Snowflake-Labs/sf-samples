{%- set ns = q(database ~ '.' ~ schema) -%}

DEFINE VIEW {{ ns }}.PLACE comment = {{ lit(owner_comment) }} as
select id::varchar as id, geometry as geom,
       names:primary::varchar as name,
       coalesce(taxonomy:primary::varchar, basic_category::varchar) as category,
       basic_category::varchar as basic_category,
       addresses[0]:country::varchar as country,
       addresses[0]:region::varchar as region,
       addresses[0]:locality::varchar as locality,
       st_y(geometry)::float as latitude, st_x(geometry)::float as longitude,
       h3_point_to_cell_string(geometry, 8) as h3_cell
from {{ q(source_place) }};

DEFINE VIEW {{ ns }}.DIVISION comment = {{ lit(owner_comment) }} as
select id::varchar as id, geometry as geom, names:primary::varchar as name,
       country::varchar as country, region::varchar as region,
       subtype::varchar as subtype, population::number as population
from {{ q(source_division) }};

DEFINE VIEW {{ ns }}.DIVISION_AREA comment = {{ lit(owner_comment) }} as
select id::varchar as id, division_id::varchar as division_id, geometry as geom,
       names:primary::varchar as name, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype, class::varchar as class,
       st_area(geometry) / 1e6 as area_sqkm,
       st_asgeojson(st_simplify(geometry, 100))::varchar as geojson
from {{ q(source_division_area) }};

DEFINE VIEW {{ ns }}.DIVISION_BOUNDARY comment = {{ lit(owner_comment) }} as
select id::varchar as id, geometry as geom, country::varchar as country,
       region::varchar as region, subtype::varchar as subtype,
       class::varchar as class, is_land::boolean as is_land,
       st_length(geometry) / 1000 as length_km
from {{ q(source_division_boundary) }};
