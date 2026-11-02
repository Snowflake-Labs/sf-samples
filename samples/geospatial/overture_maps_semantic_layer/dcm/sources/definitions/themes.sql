{%- set ns = q(database ~ '.' ~ schema) -%}

{% if 'address' in optional_themes %}
DEFINE VIEW {{ ns }}.ADDRESS comment = {{ lit(owner_comment) }} as
select id::varchar as id, geometry as geom,
       street::varchar as street, number::varchar as house_number,
       postcode::varchar as postcode,
       coalesce(postal_city::varchar,
                address_levels[array_size(address_levels) - 1]:value::varchar) as city,
       country::varchar as country,
       st_y(geometry)::float as latitude, st_x(geometry)::float as longitude,
       bbox:xmin::float as bbox_xmin, bbox:xmax::float as bbox_xmax,
       bbox:ymin::float as bbox_ymin, bbox:ymax::float as bbox_ymax
from {{ q(source_address) }};
{% endif %}

{% if 'segment' in optional_themes %}
DEFINE VIEW {{ ns }}.SEGMENT comment = {{ lit(owner_comment) }} as
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
from {{ q(source_segment) }};
{% endif %}

{% if 'building' in optional_themes %}
DEFINE VIEW {{ ns }}.BUILDING comment = {{ lit(owner_comment) }} as
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
from {{ q(source_building) }};
{% endif %}
