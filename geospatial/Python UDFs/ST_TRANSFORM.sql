CREATE OR REPLACE FUNCTION PY_TRANSFORM(g1 GEOGRAPHY, srid_from NUMBER, srid_to NUMBER)
returns geometry
language python
runtime_version = 3.8
packages = ('pyproj','shapely')
handler = 'udf'
AS $$
import pyproj
import shapely
from shapely.ops import transform
from shapely.geometry import shape
from shapely import wkb, wkt
def udf(g1, srid_from, srid_to):
    source_srid = pyproj.CRS(srid_from)
    target_srid = pyproj.CRS(srid_to)
    project = pyproj.Transformer.from_crs(source_srid, target_srid, always_xy=True).transform
    shape_wgs = shape(g1)
    shape_tr = transform(project, shape_wgs)
    return shapely.geometry.mapping(shape_tr)        
$$;

SELECT ST_SETSRID(PY_TRANSFORM(geom, 4326, 3443), 3443) as transformed_shape FROM GEOPLAYGROUND.GEO.ENERGY_GRIDS;