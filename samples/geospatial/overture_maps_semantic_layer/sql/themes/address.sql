create or replace view $ns.ADDRESS comment = $owner_comment as
select id::varchar as id, geometry as geom,
       street::varchar as street, number::varchar as house_number,
       postcode::varchar as postcode,
       coalesce(postal_city::varchar,
                address_levels[array_size(address_levels) - 1]:value::varchar) as city,
       country::varchar as country,
       st_y(geometry)::float as latitude, st_x(geometry)::float as longitude,
       bbox:xmin::float as bbox_xmin, bbox:xmax::float as bbox_xmax,
       bbox:ymin::float as bbox_ymin, bbox:ymax::float as bbox_ymax
from $source_address;
