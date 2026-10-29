create or replace view $ns.BUILDING comment = $owner_comment as
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
from $source_building;
