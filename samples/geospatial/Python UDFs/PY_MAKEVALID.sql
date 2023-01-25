CREATE OR REPLACE FUNCTION py_makevalid(geo geography)
returns geography
language python
runtime_version = 3.8
packages = ('shapely', 'pyproj')
handler = 'udf'
AS $$
import shapely
from shapely.geometry import shape, mapping
from shapely.validation import make_valid
def udf(geo):
    g1 = shape(geo)
    if g1.is_valid == True:
        g1 = g1.buffer(0.000001, resolution = 1, join_style = 1)
        g1 = g1.simplify(0.000001)
        fixed_shape = g1
    else:
        fixed_shape = make_valid(g1)
    return  mapping(fixed_shape)
$$;

select py_makevalid(to_geography('POLYGON((-98.85490804910658 39.51838733193898,-83.3978497982025 38.74447338887313,-104.6494236588478 31.751698667563574,-84.25497114658353 31.57011240319494,-98.85490804910658 39.51838733193898))', True)) as repaired_shape;