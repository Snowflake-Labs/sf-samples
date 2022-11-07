CREATE OR REPLACE FUNCTION py_makevalid(geo geography)
returns geography
language python
runtime_version = 3.8
packages = ('shapely', 'pyproj')
handler = 'udf'
AS $$
import pyproj
import shapely
from shapely.ops import transform
from shapely.geometry import shape, mapping
from shapely.validation import make_valid
from shapely import wkb, wkt
def udf(geo):
    g1 = shape(geo)
    if g1.is_valid == True:
        project = pyproj.Transformer.from_crs(4326, 3857, always_xy=True).transform
        g1 = transform(project, g1)
        g1 = g1.buffer(1, 1, 2)
        g1 = g1.simplify(1)
        project = pyproj.Transformer.from_crs(3857, 4326, always_xy=True).transform
        fixed_shape = transform(project, g1)
    else:
        fixed_shape = make_valid(g1)
    return  mapping(fixed_shape)
$$;

select py_makevalid(to_geography('POLYGON((-98.85490804910658 39.51838733193898,-83.3978497982025 38.74447338887313,-104.6494236588478 31.751698667563574,-84.25497114658353 31.57011240319494,-98.85490804910658 39.51838733193898))', True)) as repaired_shape;