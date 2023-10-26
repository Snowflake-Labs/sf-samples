-- Returns a GEOGRAPHY object that represents a MultiPolygon containing the points within a specified distance of the input GEOGRAPHY object. 

CREATE OR REPLACE FUNCTION py_convexhull(g1 geography)
returns geography
language python
runtime_version = 3.8
packages = ('shapely', 'pyproj')
handler = 'udf'
AS $$
import pyproj
from shapely.geometry import shape, mapping
from shapely.ops import transform
def udf(g1):
    geom1 = shape(g1)
    project = pyproj.Transformer.from_crs(pyproj.CRS('EPSG:4326'), pyproj.CRS('EPSG:3857'), always_xy=True).transform
    unproject = pyproj.Transformer.from_crs(pyproj.CRS('EPSG:3857'), pyproj.CRS('EPSG:4326'), always_xy=True).transform
    geometry_proj = transform(project, geom1)
    convex_hull = geometry_proj.convex_hull
    return mapping(transform(unproject, convex_hull))
$$;

select py_convexhull(to_geography('LINESTRING(-100.050692 39.128480,-100.260431 39.062645,-99.867499 39.003928,-100.383408 38.911905,-99.478231 38.913013)'));

