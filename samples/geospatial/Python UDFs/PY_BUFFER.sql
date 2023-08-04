-- Returns a GEOGRAPHY object that represents a MultiPolygon containing the points within a specified distance of the input GEOGRAPHY object. 

CREATE OR REPLACE FUNCTION py_buffer(g1 geography, radius float)
returns geography
language python
runtime_version = 3.8
packages = ('shapely', 'pyproj')
handler = 'udf'
AS $$
import pyproj
from shapely.geometry import shape, mapping
from shapely.ops import transform
def udf(g1, radius):
    geom1 = shape(g1)
    project = pyproj.Transformer.from_crs(pyproj.CRS('EPSG:4326'), pyproj.CRS('EPSG:3857'), always_xy=True).transform
    unproject = pyproj.Transformer.from_crs(pyproj.CRS('EPSG:3857'), pyproj.CRS('EPSG:4326'), always_xy=True).transform
    geometry_proj = transform(project, geom1)
    buffer_proj = geometry_proj.buffer(radius)
    return mapping(transform(unproject, buffer_proj))
$$;

select py_buffer(to_geography('LINESTRING(-94.5915591164521 39.04260595970442,-94.59160119786246 39.04199639399846)'), 5);