create or replace view $ns.SEGMENT comment = $owner_comment as
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
from $source_segment;
