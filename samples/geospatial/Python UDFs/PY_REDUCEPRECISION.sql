-- function reduces the precision of the specified input geography and keeps the specified number of decimal digits

CREATE OR REPLACE FUNCTION py_reduceprecision(geo geography, n integer)
returns geography
language python
runtime_version = 3.8
packages = ('shapely')
handler = 'udf'
AS $$
from shapely.geometry import shape, mapping
from shapely import wkt
def udf(geo, n):
    if n < 0:
        raise ValueError('Number of digits must be positive')
    else:
        g1 = shape(geo)
        updated_shape = wkt.loads(wkt.dumps(g1, rounding_precision=n))
        return mapping(updated_shape)
$$;


select py_reduceprecision(to_geography('POLYGON((-108.720703125 34.99400375800002,-100.01953124999999 46.589069083,
-90.791015625 34.921971036,-108.720703125 34.99400375800002),(-100.10742187500001 41.47566019999999,
-102.919921875 37.614231415000006,-96.85546875 37.54457732099998,-100.10742187500001 41.47566019999999))'), 3) as geog_reduced;